use serde::{Deserialize, Serialize};

/// Nodo de tarea genérico multipropósito.
/// Representa una unidad de trabajo que se resuelve en runtime
/// mediante el `ModuleRegistry` usando el campo `module_type`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNode {
    /// Identificador único de la tarea ejecutable registrada en el `ModuleRegistry`
    pub uuid: String,
}
