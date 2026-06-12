//! A small connection pool that reuses authenticated SSH sessions across
//! task calls, keyed by connection identity.
//!
//! The expensive part of an SFTP operation is the TCP connect, SSH handshake
//! and auth. For high-volume use (e.g. a `foreach` fetching hundreds of files
//! from the same host) opening a fresh session per call dominates the cost.
//! [`Pool`] keeps authenticated sessions and hands them back out; a cheap SFTP
//! channel is opened on top per call.
//!
//! The pool is generic over a [`Connector`] so its bookkeeping (reuse, liveness
//! eviction, per-key cap) is unit-testable without a live server; production
//! uses the `ssh2`-backed connector wired in `lib.rs`.

use std::collections::HashMap;
use std::sync::Mutex;

use workflow_forge_core::error::WorkflowError;

use crate::Connection;

/// Opens and liveness-checks the pooled connection type. Synchronous: every
/// call happens inside `spawn_blocking`.
pub trait Connector: Send + Sync + 'static {
    /// The pooled connection (an authenticated SSH session in production).
    type Conn: Send;
    /// Open a fresh connection.
    fn connect(&self, conn: &Connection) -> Result<Self::Conn, WorkflowError>;
    /// Is a pooled connection still usable? Dead ones are discarded on checkout.
    fn alive(&self, conn: &Self::Conn) -> bool;
}

/// A keyed pool of reusable connections with a per-key idle cap.
pub struct Pool<C: Connector> {
    connector: C,
    idle: Mutex<HashMap<String, Vec<C::Conn>>>,
    max_idle_per_key: usize,
}

impl<C: Connector> Pool<C> {
    /// A pool over `connector` that keeps at most `max_idle_per_key` idle
    /// connections per distinct connection identity.
    pub fn new(connector: C, max_idle_per_key: usize) -> Self {
        Self {
            connector,
            idle: Mutex::new(HashMap::new()),
            max_idle_per_key: max_idle_per_key.max(1),
        }
    }

    /// Take a live connection for `conn` (identified by `key`): reuse an idle
    /// one (discarding any that failed the liveness check) or open a fresh one.
    pub fn checkout(&self, key: &str, conn: &Connection) -> Result<C::Conn, WorkflowError> {
        loop {
            let candidate = {
                let mut idle = self.idle.lock().expect("sftp pool lock poisoned");
                idle.get_mut(key).and_then(Vec::pop)
            };
            match candidate {
                Some(c) if self.connector.alive(&c) => return Ok(c),
                Some(_) => continue, // dead: drop it and try the next idle one
                None => return self.connector.connect(conn),
            }
        }
    }

    /// Return a connection to the pool (dropped if the key is already at its
    /// idle cap). Only return connections left in a clean state.
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
        // connect() returns an incrementing id, so the test reads "did it
        // reconnect?" from the returned value rather than a side counter.
        let connector = FakeConnector {
            connects: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
        };
        Arc::new(Pool::new(connector, cap))
    }

    #[test]
    fn reuses_a_live_connection() {
        let pool = pool(4);
        let conn = dummy_connection();
        let key = "k";

        let first = pool.checkout(key, &conn).unwrap();
        pool.checkin(key, first);
        assert_eq!(pool.idle_count(key), 1);

        // Reused: connect() is not called again (id 0 reused, not a new id 1).
        let second = pool.checkout(key, &conn).unwrap();
        assert_eq!(second, 0);
        assert_eq!(pool.idle_count(key), 0);
    }

    #[test]
    fn discards_dead_connections_on_checkout() {
        let connector = FakeConnector {
            connects: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
        };
        let pool = Pool::new(connector, 4);
        let conn = dummy_connection();

        let c = pool.checkout("k", &conn).unwrap(); // id 0
        pool.checkin("k", c);
        // Mark idle connections dead: next checkout must open a fresh one.
        pool.connector.alive.store(false, Ordering::SeqCst);
        let fresh = pool.checkout("k", &conn).unwrap();
        assert_eq!(fresh, 1, "debió reconectar, no reusar el muerto");
    }

    #[test]
    fn honors_idle_cap() {
        let pool = pool(2);
        pool.checkin("k", 10);
        pool.checkin("k", 11);
        pool.checkin("k", 12); // over cap: dropped
        assert_eq!(pool.idle_count("k"), 2);
    }
}
