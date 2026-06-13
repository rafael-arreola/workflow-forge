//! Nodos del grafo: el vocabulario de la spec 1.0.
//!
//! [`NodeKind`] es un enum **cerrado a propósito**: el vocabulario de nodos
//! lo define la spec, no el host. La extensibilidad del engine pasa por las
//! tareas ([`crate::task::Task`]), no por kinds nuevos. Agregar un kind es
//! un cambio de spec: variante aquí + handler en `runtime/handlers/` +
//! reglas en `validate/rules/` (checklist completo en la doc de `lib.rs`).

use serde::{Deserialize, Serialize};

pub mod event;
pub mod foreach;
pub mod gateway;
pub mod loop_node;
pub mod task;

/// Identificador único de un nodo dentro del grafo del workflow.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        NodeId(s)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        NodeId(s.to_string())
    }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Nodo del grafo de workflow. Contiene un identificador y una variante de comportamiento
/// que se resuelve en tiempo de ejecución.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Identificador único del nodo dentro del workflow
    pub id: NodeId,
    /// Tipo de nodo: tarea ejecutable, condicional, bucle, paralelo, etc.
    #[serde(flatten)]
    pub kind: NodeKind,
}

/// Clasificación de nodos. El campo `"kind"` actúa como discriminador en JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeKind {
    /// Nodo de arranque del workflow
    Start(event::StartNode),
    /// Nodo de terminación del workflow
    End(event::EndNode),
    /// Nodo de tarea ejecutable
    Task(task::TaskNode),
    /// Nodo que itera un array invocando una tarea por elemento
    Foreach(foreach::ForeachNode),
    /// Nodo que invoca una tarea repetidamente con condición de continuación
    /// y tope duro de iteraciones
    Loop(loop_node::LoopNode),
    /// Nodo de control de flujo (exclusive/parallel/join)
    Gateway(gateway::GatewayNode),
    /// Nodo que ejecuta otro workflow como si fuera una tarea
    Subworkflow(SubworkflowNode),
}

impl NodeKind {
    /// Nombre del kind como aparece en la spec JSON (el discriminador).
    pub fn name(&self) -> &'static str {
        match self {
            NodeKind::Start(_) => "start",
            NodeKind::End(_) => "end",
            NodeKind::Task(_) => "task",
            NodeKind::Foreach(_) => "foreach",
            NodeKind::Loop(_) => "loop",
            NodeKind::Gateway(_) => "gateway",
            NodeKind::Subworkflow(_) => "subworkflow",
        }
    }
}

/// Nodo que ejecuta otro workflow: el `input` resuelto (o el token del
/// predecesor) se convierte en el trigger del hijo y el output final del
/// hijo es el output del nodo. Las tres salidas ruteables aplican: un fallo
/// del hijo rutea por `on: error` y un panic dentro del hijo por `on: panic`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubworkflowNode {
    /// Nombre del workflow hijo. Se resuelve primero contra la sección
    /// `workflows` del documento y después contra el `WorkflowRegistry`
    /// compartido del executor.
    pub workflow: String,
    /// Mapping del trigger del hijo (reglas `$.` de los inputs);
    /// sin `input`, el hijo recibe el output del predecesor
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
}
