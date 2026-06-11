//! La familia **observe**: observabilidad de ejecuciones.
//!
//! El executor emite eventos tipados y serializables a un
//! [`ExecutionObserver`] registrado con `WorkflowExecutor::with_observer`.
//! El host decide qué hacer con ellos (memoria, base de datos, OTLP, …).
//!
//! Los eventos son el contrato de observabilidad de la spec: serializan a
//! JSON estable (`type` discriminador snake_case) y están diseñados para ser,
//! a futuro, el journal de un executor durable (event sourcing) sin rediseño.
//!
//! [`InMemoryHistory`] es el observer integrado: acumula los eventos de una
//! ejecución y produce un [`ExecutionReport`] — la respuesta embebida a
//! "¿qué pasó con la ejecución X?".

pub mod history;

pub use history::{ExecutionReport, ExecutionStatus, InMemoryHistory, NodeReport, NodeStatus};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::WorkflowError;

/// Receptor de eventos de ejecución.
///
/// `on_event` se invoca de forma síncrona desde el executor y NO debe
/// bloquear: un host que persiste lento debe bufferizar (canal/spawn).
pub trait ExecutionObserver: Send + Sync {
    /// Recibe cada evento emitido por el executor, en orden de emisión.
    fn on_event(&self, event: &ExecutionEvent);
}

/// Evento de ejecución: metadata común + variante específica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvent {
    /// Id de la ejecución que emitió el evento
    pub execution_id: String,
    /// Id de la ejecución padre si el evento viene de un sub-workflow
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<String>,
    /// Orden total de emisión dentro de la ejecución (las ramas paralelas
    /// emiten concurrentemente; `seq` las ordena de forma estable). Los
    /// sub-workflows comparten el contador del padre: `seq` ordena el árbol
    /// completo de ejecuciones
    pub seq: u64,
    /// Milisegundos transcurridos desde el inicio de la ejecución
    pub elapsed_ms: u64,
    /// Variante específica del evento (`type` en JSON)
    #[serde(flatten)]
    pub kind: EventKind,
}

/// Variantes de evento. El campo `type` discrimina en JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(missing_docs)] // los campos repiten node_id/output/error; la variante documenta
pub enum EventKind {
    /// La ejecución arrancó (raíz o sub-workflow)
    WorkflowStarted { workflow: Value, trigger: Value },
    /// Un nodo comenzó a ejecutarse (los joins lo emiten al completar)
    NodeStarted { node_id: String, kind: String },
    /// Un intento de tarea arrancó (nodos task; los foreach no emiten attempts)
    TaskAttemptStarted {
        node_id: String,
        attempt: u32,
        /// Input resuelto de la tarea; solo en el primer intento (los
        /// reintentos reciben el mismo input)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
    },
    /// Un intento de tarea falló; `will_retry` indica si habrá otro
    TaskAttemptFailed {
        node_id: String,
        attempt: u32,
        error: WorkflowError,
        will_retry: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_delay_ms: Option<u64>,
    },
    /// Un nodo terminó exitosamente y publicó su output
    NodeCompleted {
        node_id: String,
        output: Value,
        duration_ms: u64,
    },
    /// Un nodo falló definitivamente (reintentos agotados)
    NodeFailed {
        node_id: String,
        error: WorkflowError,
        /// `true` si el flujo continuó por una arista `on: error`
        error_routed: bool,
    },
    /// Un elemento de un foreach terminó exitosamente
    ForeachItemCompleted {
        node_id: String,
        index: usize,
        output: Value,
    },
    /// Un elemento de un foreach falló definitivamente
    ForeachItemFailed {
        node_id: String,
        index: usize,
        error: WorkflowError,
    },
    /// La ejecución completó con output final
    WorkflowCompleted { output: Value, duration_ms: u64 },
    /// La ejecución falló
    WorkflowFailed {
        error: WorkflowError,
        duration_ms: u64,
    },
}

impl EventKind {
    /// Id del nodo al que refiere el evento, si aplica
    pub fn node_id(&self) -> Option<&str> {
        match self {
            EventKind::NodeStarted { node_id, .. }
            | EventKind::TaskAttemptStarted { node_id, .. }
            | EventKind::TaskAttemptFailed { node_id, .. }
            | EventKind::NodeCompleted { node_id, .. }
            | EventKind::NodeFailed { node_id, .. }
            | EventKind::ForeachItemCompleted { node_id, .. }
            | EventKind::ForeachItemFailed { node_id, .. } => Some(node_id),
            _ => None,
        }
    }
}
