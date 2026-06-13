//! Observer integrado en memoria y el reporte que produce.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::WorkflowError;
use crate::observe::{EventKind, ExecutionEvent, ExecutionObserver};

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
    /// Crea una historia vacía
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

    /// Resumen de la ejecución raíz por nodo, construido de los eventos.
    /// Los eventos de sub-workflows no se pliegan aquí: el nodo subworkflow
    /// del padre los resume; usa [`InMemoryHistory::report_for`] para el
    /// detalle de una ejecución hija.
    pub fn report(&self) -> ExecutionReport {
        let events = self.events();
        match events.first().map(|e| e.execution_id.clone()) {
            Some(root) => ExecutionReport::from_events(
                &events
                    .into_iter()
                    .filter(|e| e.execution_id == root)
                    .collect::<Vec<_>>(),
            ),
            None => ExecutionReport::from_events(&events),
        }
    }

    /// Resumen de una ejecución específica (raíz o sub-workflow)
    pub fn report_for(&self, execution_id: &str) -> ExecutionReport {
        ExecutionReport::from_events(
            &self
                .events()
                .into_iter()
                .filter(|e| e.execution_id == execution_id)
                .collect::<Vec<_>>(),
        )
    }

    /// Ids de las ejecuciones presentes en la historia, en orden de aparición
    /// (la raíz primero, luego cada sub-workflow conforme arrancó)
    pub fn executions(&self) -> Vec<String> {
        let mut seen = Vec::new();
        for event in self.events() {
            if !seen.contains(&event.execution_id) {
                seen.push(event.execution_id.clone());
            }
        }
        seen
    }
}

/// Estado terminal (o no) de una ejecución según sus eventos.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    /// No hay evento terminal todavía
    Running,
    /// Terminó exitosamente
    Completed,
    /// Terminó con error
    Failed,
}

/// Estado de un nodo según sus eventos.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Arrancó y no tiene evento terminal
    Running,
    /// Terminó exitosamente
    Completed,
    /// Falló definitivamente
    Failed,
    /// Falló pero el flujo continuó por una arista `on: error`
    ErrorRouted,
}

/// Resumen serializable de una ejecución: status global + timeline por nodo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    /// Id de la ejecución resumida
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    /// Metadata del workflow (id, nombre, versión)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<Value>,
    /// Estado global de la ejecución
    pub status: ExecutionStatus,
    /// Duración total en milisegundos, si terminó
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Output final, si completó
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Error final, si falló
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<WorkflowError>,
    /// Nodos en orden de primer arranque
    pub nodes: Vec<NodeReport>,
}

/// Resumen de un nodo dentro del reporte.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReport {
    /// Id del nodo
    pub node_id: String,
    /// Kind del nodo como aparece en la spec (`task`, `gateway`, …)
    pub kind: String,
    /// Estado del nodo según sus eventos
    pub status: NodeStatus,
    /// Intentos de tarea registrados (0 para nodos que no son task/foreach)
    pub attempts: u32,
    /// Duración en milisegundos, si terminó
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Output del nodo, si completó
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Error del nodo, si falló
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<WorkflowError>,
    /// Elementos/iteraciones exitosos (solo foreach y loop)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_ok: Option<usize>,
    /// Elementos/iteraciones fallidos (solo foreach y loop)
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
                    EventKind::ForeachItemCompleted { .. }
                    | EventKind::LoopIterationCompleted { .. } => {
                        *node.items_ok.get_or_insert(0) += 1;
                        node.items_failed.get_or_insert(0);
                    }
                    EventKind::ForeachItemFailed { .. } | EventKind::LoopIterationFailed { .. } => {
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
