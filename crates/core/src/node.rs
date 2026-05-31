use serde::{Deserialize, Serialize};

mod event;
mod task;

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
    /// Tipo de nodo: tarea ejecutable, condicionalx, bucle, paralelo, etc.
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
}
