use crate::node::Node;
use serde::{Deserialize, Serialize};

/// Definición completa de un workflow lista para ser serializada/deserializada.
/// Contiene nodos (módulos + control de flujo), aristas y configuración global.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDef {
    /// Identificador único del workflow (se asigna si no se provee)
    #[serde(default)]
    pub id: Option<String>,
    /// Nombre descriptivo del workflow
    pub name: String,
    /// Versión semántica
    pub version: String,
    /// Nodos que componen el grafo (módulos, condicionales, bucles, etc.)
    pub nodes: Vec<Node>,
    /// Aristas dirigidas que definen el flujo de datos y control
    #[serde(default)]
    pub edges: Vec<FlowEdge>,
}

/// Conexión dirigida entre dos nodos del workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowEdge {
    /// ID del nodo de origen
    pub from: String,
    /// ID del nodo de destino
    pub to: String,
}
