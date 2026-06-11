//! El documento raíz de la spec: definición del workflow y sus aristas.

use crate::spec::node::{Node, NodeId};
use crate::spec::profile::TaskProfile;
use serde::{Deserialize, Serialize};

/// Definición completa de un workflow lista para ser serializada/deserializada.
/// Contiene nodos y aristas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    /// Versión de la spec que cumple esta definición (ej. "1.0")
    #[serde(default = "default_spec")]
    pub spec: String,
    /// Identificador único del workflow (se asigna si no se provee)
    #[serde(default)]
    pub id: Option<String>,
    /// Nombre descriptivo del workflow
    pub name: String,
    /// Versión semántica
    pub version: String,
    /// Perfiles de tarea locales al workflow: instancias preconfiguradas de
    /// tareas registradas, visibles solo para esta definición
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tasks: Vec<TaskProfile>,
    /// Sub-workflows locales al documento, referenciables por nombre desde
    /// nodos `kind: "subworkflow"`. Tienen precedencia sobre el
    /// `WorkflowRegistry` compartido y son visibles solo para esta definición
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflows: Vec<WorkflowDefinition>,
    /// Nodos que componen el grafo
    pub nodes: Vec<Node>,
    /// Aristas dirigidas que definen el flujo de control
    #[serde(default)]
    pub edges: Vec<FlowEdge>,
}

fn default_spec() -> String {
    "1.0".to_string()
}

/// Conexión dirigida entre dos nodos del workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowEdge {
    /// ID del nodo de origen
    pub from: NodeId,
    /// ID del nodo de destino
    pub to: NodeId,
    /// Etiqueta de la arista; conecta una rama de gateway (`branches[].edge`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Disparador alternativo: `error` enruta el flujo cuando el nodo origen
    /// agota sus reintentos. Sin `on`, la arista es del flujo normal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<EdgeTrigger>,
}

/// Disparadores alternativos de una arista.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EdgeTrigger {
    /// La arista se sigue cuando el nodo origen falla definitivamente
    Error,
    /// La arista se sigue cuando la tarea del nodo origen panickea
    /// (bug en la extensión). Un panic no reintenta ni cae en `on: error`.
    Panic,
}
