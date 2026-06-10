use std::sync::{Arc, RwLock};
use std::time::Instant;

use serde_json::{Value, json};
use uuid::Uuid;

use crate::blob::{BlobStore, TempDirBlobStore};
use crate::node::NodeId;
use crate::workflow::WorkflowDefinition;

/// Contexto de una ejecución. Contiene el documento de estado sobre el que
/// se resuelven mappings y condiciones:
///
/// ```text
/// $.trigger              → input inicial del workflow
/// $.nodes.<id>.output    → resultado de cada nodo ejecutado
/// $.workflow             → metadata (id, nombre, versión, execution_id)
/// ```
///
/// Thread-safe: las ramas paralelas leen y escriben concurrentemente.
pub struct WorkflowContext {
    /// Identificador único de la ejecución actual (UUID v7)
    execution_id: String,
    /// Instante en que inició la ejecución
    started_at: Instant,
    /// Documento de estado de la ejecución
    state: RwLock<Value>,
    /// Almacenamiento de blobs (`$blob`) con ciclo de vida de la ejecución
    blobs: Arc<dyn BlobStore>,
    /// Contador de eventos de observabilidad (orden total por ejecución)
    event_seq: std::sync::atomic::AtomicU64,
}

impl WorkflowContext {
    /// Crea el contexto de una nueva ejecución con su documento de estado inicial
    pub fn new(workflow: &WorkflowDefinition, trigger: Value) -> Self {
        let execution_id = Uuid::now_v7().to_string();
        let state = json!({
            "trigger": trigger,
            "nodes": {},
            "workflow": {
                "id": workflow.id,
                "name": workflow.name,
                "version": workflow.version,
                "execution_id": execution_id,
            }
        });
        let blobs = Arc::new(TempDirBlobStore::new(&execution_id));
        Self {
            execution_id,
            started_at: Instant::now(),
            state: RwLock::new(state),
            blobs,
            event_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Siguiente número de secuencia de evento (orden total por ejecución)
    pub fn next_event_seq(&self) -> u64 {
        self.event_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Identificador único de la ejecución
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Almacenamiento de blobs de esta ejecución (convención `$blob`)
    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    /// Tiempo transcurrido desde el inicio de la ejecución
    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }

    /// Lee el documento de estado bajo el lock, sin clonar
    pub fn with_state<R>(&self, f: impl FnOnce(&Value) -> R) -> R {
        let state = self.state.read().expect("WorkflowContext lock poisoned");
        f(&state)
    }

    /// Copia completa del documento de estado (para debugging/inspección)
    pub fn snapshot(&self) -> Value {
        self.with_state(Clone::clone)
    }

    /// Publica el output de un nodo en `$.nodes.<id>.output`
    pub fn set_node_output(&self, node_id: &NodeId, output: Value) {
        let mut state = self.state.write().expect("WorkflowContext lock poisoned");
        state["nodes"][node_id.0.as_str()] = json!({ "output": output });
    }

    /// Output de un nodo ya ejecutado, si existe
    pub fn node_output(&self, node_id: &NodeId) -> Option<Value> {
        self.with_state(|state| state["nodes"][node_id.0.as_str()].get("output").cloned())
    }

    /// Publica el error de un nodo en `$.nodes.<id>.error` (rutas on_error)
    pub fn set_node_error(&self, node_id: &NodeId, error: Value) {
        let mut state = self.state.write().expect("WorkflowContext lock poisoned");
        state["nodes"][node_id.0.as_str()]["error"] = error;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workflow() -> WorkflowDefinition {
        serde_json::from_value(json!({
            "name": "test",
            "version": "0.1.0",
            "nodes": [],
            "edges": []
        }))
        .unwrap()
    }

    #[test]
    fn documento_inicial_y_outputs() {
        let ctx = WorkflowContext::new(&workflow(), json!({ "user_id": 7 }));

        ctx.with_state(|state| {
            assert_eq!(state["trigger"]["user_id"], 7);
            assert_eq!(state["workflow"]["name"], "test");
            assert_eq!(
                state["workflow"]["execution_id"],
                ctx.execution_id().to_string().as_str()
            );
        });

        let node = NodeId::from("fetch");
        assert_eq!(ctx.node_output(&node), None);
        ctx.set_node_output(&node, json!({ "status": 200 }));
        assert_eq!(ctx.node_output(&node), Some(json!({ "status": 200 })));
        ctx.with_state(|state| {
            assert_eq!(state["nodes"]["fetch"]["output"]["status"], 200);
        });
    }
}
