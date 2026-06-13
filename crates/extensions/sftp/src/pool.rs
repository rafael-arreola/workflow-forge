//! Un pool de conexiones pequeño que reutiliza sesiones SSH autenticadas
//! entre llamadas de tarea, indexadas por identidad de conexión.
//!
//! La parte cara de una operación SFTP es el connect TCP, el handshake SSH y
//! la autenticación. A volumen alto (p. ej. un `foreach` bajando cientos de
//! archivos del mismo host), abrir una sesión nueva por llamada domina el
//! costo. [`Pool`] conserva sesiones autenticadas y las vuelve a entregar;
//! por llamada solo se abre un canal SFTP barato encima.
//!
//! El pool es genérico sobre un [`Connector`] para que su contabilidad
//! (reuso, descarte por liveness, tope por llave) sea testeable por unidad
//! sin un servidor vivo; producción usa el conector respaldado por `ssh2`
//! cableado en `lib.rs`.

use std::collections::HashMap;
use std::sync::Mutex;

use workflow_forge_core::error::WorkflowError;

use crate::Connection;

/// Abre y verifica la vida de las conexiones del pool. Síncrono: toda
/// llamada ocurre dentro de `spawn_blocking`.
pub trait Connector: Send + Sync + 'static {
    /// La conexión pooleada (una sesión SSH autenticada en producción).
    type Conn: Send;
    /// Abre una conexión nueva.
    fn connect(&self, conn: &Connection) -> Result<Self::Conn, WorkflowError>;
    /// ¿La conexión pooleada sigue usable? Las muertas se descartan al
    /// hacer checkout.
    fn alive(&self, conn: &Self::Conn) -> bool;
}

/// Pool de conexiones reusables indexado por llave, con tope de ociosas por
/// llave.
pub struct Pool<C: Connector> {
    connector: C,
    idle: Mutex<HashMap<String, Vec<C::Conn>>>,
    max_idle_per_key: usize,
}

impl<C: Connector> Pool<C> {
    /// Un pool sobre `connector` que guarda como máximo `max_idle_per_key`
    /// conexiones ociosas por identidad de conexión distinta.
    pub fn new(connector: C, max_idle_per_key: usize) -> Self {
        Self {
            connector,
            idle: Mutex::new(HashMap::new()),
            max_idle_per_key: max_idle_per_key.max(1),
        }
    }

    /// Toma una conexión viva para `conn` (identificada por `key`): reusa una
    /// ociosa (descartando las que fallen la verificación de vida) o abre una
    /// nueva.
    pub fn checkout(&self, key: &str, conn: &Connection) -> Result<C::Conn, WorkflowError> {
        loop {
            let candidate = {
                let mut idle = self.idle.lock().expect("sftp pool lock poisoned");
                idle.get_mut(key).and_then(Vec::pop)
            };
            match candidate {
                Some(c) if self.connector.alive(&c) => return Ok(c),
                Some(_) => continue, // muerta: se descarta y se prueba la siguiente
                None => return self.connector.connect(conn),
            }
        }
    }

    /// Devuelve una conexión al pool (se descarta si la llave ya está en su
    /// tope de ociosas). Devuelve solo conexiones en estado limpio.
    pub fn checkin(&self, key: &str, conn: C::Conn) {
        let mut idle = self.idle.lock().expect("sftp pool lock poisoned");
        let slot = idle.entry(key.to_string()).or_default();
        if slot.len() < self.max_idle_per_key {
            slot.push(conn);
        }
    }

    #[cfg(test)]
    fn idle_count(&self, key: &str) -> usize {
        self.idle
            .lock()
            .unwrap()
            .get(key)
            .map(Vec::len)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn dummy_connection() -> Connection {
        serde_json::from_value(serde_json::json!({
            "host": "h", "username": "u", "auth": { "type": "password", "password": "p" }
        }))
        .unwrap()
    }

    struct FakeConnector {
        connects: AtomicUsize,
        alive: AtomicBool,
    }

    impl Connector for FakeConnector {
        type Conn = usize;
        fn connect(&self, _conn: &Connection) -> Result<usize, WorkflowError> {
            Ok(self.connects.fetch_add(1, Ordering::SeqCst))
        }
        fn alive(&self, _conn: &usize) -> bool {
            self.alive.load(Ordering::SeqCst)
        }
    }

    fn pool(cap: usize) -> Arc<Pool<FakeConnector>> {
        // connect() devuelve un id incremental: el test lee "¿reconectó?" del
        // valor devuelto en vez de un contador aparte.
        let connector = FakeConnector {
            connects: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
        };
        Arc::new(Pool::new(connector, cap))
    }

    #[test]
    fn reusa_una_conexion_viva() {
        let pool = pool(4);
        let conn = dummy_connection();
        let key = "k";

        let first = pool.checkout(key, &conn).unwrap();
        pool.checkin(key, first);
        assert_eq!(pool.idle_count(key), 1);

        // Reusada: connect() no se llama de nuevo (vuelve el id 0, no un id 1).
        let second = pool.checkout(key, &conn).unwrap();
        assert_eq!(second, 0);
        assert_eq!(pool.idle_count(key), 0);
    }

    #[test]
    fn descarta_conexiones_muertas_al_checkout() {
        let connector = FakeConnector {
            connects: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
        };
        let pool = Pool::new(connector, 4);
        let conn = dummy_connection();

        let c = pool.checkout("k", &conn).unwrap(); // id 0
        pool.checkin("k", c);
        // Marca muertas las ociosas: el siguiente checkout debe abrir una nueva.
        pool.connector.alive.store(false, Ordering::SeqCst);
        let fresh = pool.checkout("k", &conn).unwrap();
        assert_eq!(fresh, 1, "debió reconectar, no reusar la muerta");
    }

    #[test]
    fn respeta_el_tope_de_ociosas() {
        let pool = pool(2);
        pool.checkin("k", 10);
        pool.checkin("k", 11);
        pool.checkin("k", 12); // sobre el tope: se descarta
        assert_eq!(pool.idle_count("k"), 2);
    }
}
