use serde::{Deserialize, Serialize};
mod event;
mod transformer;
/// Nodo del grafo de workflow. Contiene un identificador y una variante de comportamiento
/// que se resuelve en tiempo de ejecución.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Identificador único del nodo dentro del workflow
    pub id: String,
    /// Tipo de nodo: tarea ejecutable, condicionalx, bucle, paralelo, etc.
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
}
