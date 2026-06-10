//! Observabilidad de ejecuciones: el executor emite eventos tipados y
//! serializables a un [`ExecutionObserver`] registrado con
//! `WorkflowExecutor::with_observer`. El host decide qué hacer con ellos
//! (memoria, base de datos, OTLP, …).
//!
//! Los eventos son el contrato de observabilidad de la spec: serializan a
//! JSON estable (`type` discriminador snake_case) y están diseñados para ser,
//! a futuro, el journal de un executor durable (event sourcing) sin rediseño.
//!
//! [`InMemoryHistory`] es el observer integrado: acumula los eventos de una
//! ejecución y produce un [`ExecutionReport`] — la respuesta embebida a
//! "¿qué pasó con la ejecución X?".

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::WorkflowError;

/// Receptor de eventos de ejecución.
///
/// `on_event` se invoca de forma síncrona desde el executor y NO debe
/// bloquear: un host que persiste lento debe bufferizar (canal/spawn).
pub trait ExecutionObserver: Send + Sync {
    fn on_event(&self, event: &ExecutionEvent);
}

/// Evento de ejecución: metadata común + variante específica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvent {
    /// Id de la ejecución que emitió el evento
    pub execution_id: String,
    /// Orden total de emisión dentro de la ejecución (las ramas paralelas
    /// emiten concurrentemente; `seq` las ordena de forma estable)
    pub seq: u64,
    /// Milisegundos transcurridos desde el inicio de la ejecución
    pub elapsed_ms: u64,
    #[serde(flatten)]
    pub kind: EventKind,
}

/// Variantes de evento. El campo `type` discrimina en JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    WorkflowStarted {
        workflow: Value,
        trigger: Value,
    },
    NodeStarted {
        node_id: String,
        kind: String,
    },
    TaskAttemptStarted {
        node_id: String,
        attempt: u32,
        /// Input resuelto de la tarea; solo en el primer intento (los
        /// reintentos reciben el mismo input)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
    },
    TaskAttemptFailed {
        node_id: String,
        attempt: u32,
        error: WorkflowError,
        will_retry: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_delay_ms: Option<u64>,
    },
    NodeCompleted {
        node_id: String,
        output: Value,
        duration_ms: u64,
    },
    NodeFailed {
        node_id: String,
        error: WorkflowError,
        /// `true` si el flujo continuó por una arista `on: error`
        error_routed: bool,
    },
    ForeachItemCompleted {
        node_id: String,
        index: usize,
        output: Value,
    },
    ForeachItemFailed {
        node_id: String,
        index: usize,
        error: WorkflowError,
    },
    WorkflowCompleted {
        output: Value,
        duration_ms: u64,
    },
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

// ---------------------------------------------------------------------------
// InMemoryHistory + ExecutionReport
// ---------------------------------------------------------------------------

/// Observer integrado: acumula los eventos de una ejecución en memoria.
#[derive(Default)]
pub struct InMemoryHistory {
    events: Mutex<Vec<ExecutionEvent>>,
}

impl ExecutionObserver for InMemoryHistory {
    fn on_event(&self, event: &ExecutionEvent) {
        self.events
            .lock()
            .expect("InMemoryHistory lock poisoned")
            .push(event.clone());
    }
}

impl InMemoryHistory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Copia de los eventos acumulados, ordenados por `seq`
    pub fn events(&self) -> Vec<ExecutionEvent> {
        let mut events = self
            .events
            .lock()
            .expect("InMemoryHistory lock poisoned")
            .clone();
        events.sort_by_key(|e| e.seq);
        events
    }

    /// Resumen de la ejecución por nodo, construido de los eventos
    pub fn report(&self) -> ExecutionReport {
        ExecutionReport::from_events(&self.events())
    }
}

/// Estado terminal (o no) de una ejecución según sus eventos.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
}

/// Estado de un nodo según sus eventos.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Running,
    Completed,
    Failed,
    /// Falló pero el flujo continuó por una arista `on: error`
    ErrorRouted,
}

/// Resumen serializable de una ejecución: status global + timeline por nodo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<Value>,
    pub status: ExecutionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<WorkflowError>,
    /// Nodos en orden de primer arranque
    pub nodes: Vec<NodeReport>,
}

/// Resumen de un nodo dentro del reporte.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReport {
    pub node_id: String,
    pub kind: String,
    pub status: NodeStatus,
    /// Intentos de tarea registrados (0 para nodos que no son task/foreach)
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<WorkflowError>,
    /// Elementos exitosos/fallidos (solo foreach)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_ok: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_failed: Option<usize>,
}

impl ExecutionReport {
    /// Construye el reporte plegando eventos (deben venir ordenados por seq)
    pub fn from_events(events: &[ExecutionEvent]) -> Self {
        let mut report = ExecutionReport {
            execution_id: None,
            workflow: None,
            status: ExecutionStatus::Running,
            duration_ms: None,
            output: None,
            error: None,
            nodes: Vec::new(),
        };
        let mut index: HashMap<String, usize> = HashMap::new();

        for event in events {
            report
                .execution_id
                .get_or_insert_with(|| event.execution_id.clone());

            if let Some(node_id) = event.kind.node_id() {
                let i = *index.entry(node_id.to_string()).or_insert_with(|| {
                    report.nodes.push(NodeReport {
                        node_id: node_id.to_string(),
                        kind: String::new(),
                        status: NodeStatus::Running,
                        attempts: 0,
                        duration_ms: None,
                        output: None,
                        error: None,
                        items_ok: None,
                        items_failed: None,
                    });
                    report.nodes.len() - 1
                });
                let node = &mut report.nodes[i];
                match &event.kind {
                    EventKind::NodeStarted { kind, .. } => {
                        node.kind = kind.clone();
                    }
                    EventKind::TaskAttemptStarted { attempt, .. } => {
                        node.attempts = node.attempts.max(*attempt);
                    }
                    EventKind::NodeCompleted {
                        output,
                        duration_ms,
                        ..
                    } => {
                        node.status = NodeStatus::Completed;
                        node.output = Some(output.clone());
                        node.duration_ms = Some(*duration_ms);
                    }
                    EventKind::NodeFailed {
                        error,
                        error_routed,
                        ..
                    } => {
                        node.status = if *error_routed {
                            NodeStatus::ErrorRouted
                        } else {
                            NodeStatus::Failed
                        };
                        node.error = Some(error.clone());
                    }
                    EventKind::ForeachItemCompleted { .. } => {
                        *node.items_ok.get_or_insert(0) += 1;
                        node.items_failed.get_or_insert(0);
                    }
                    EventKind::ForeachItemFailed { .. } => {
                        *node.items_failed.get_or_insert(0) += 1;
                        node.items_ok.get_or_insert(0);
                    }
                    _ => {}
                }
            } else {
                match &event.kind {
                    EventKind::WorkflowStarted { workflow, .. } => {
                        report.workflow = Some(workflow.clone());
                    }
                    EventKind::WorkflowCompleted {
                        output,
                        duration_ms,
                    } => {
                        report.status = ExecutionStatus::Completed;
                        report.output = Some(output.clone());
                        report.duration_ms = Some(*duration_ms);
                    }
                    EventKind::WorkflowFailed { error, duration_ms } => {
                        report.status = ExecutionStatus::Failed;
                        report.error = Some(error.clone());
                        report.duration_ms = Some(*duration_ms);
                    }
                    _ => {}
                }
            }
        }
        report
    }
}
