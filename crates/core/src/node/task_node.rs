use serde::{Deserialize, Serialize};

/// Nodo que ejecuta una tarea registrada en el TaskRegistry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNode {
    /// Tipo de tarea a instanciar desde el registry
    #[serde(rename = "type")]
    pub task_type: String,
    /// Configuración específica de la tarea (JSON arbitrario)
    #[serde(default)]
    pub config: serde_json::Value,
}
