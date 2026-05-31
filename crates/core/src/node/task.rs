use serde::{Deserialize, Serialize};

use crate::task::TaskId;

/// Nodo de tarea genérico multipropósito.
/// Representa una unidad de trabajo que se resuelve en runtime
/// mediante el `TaskRegistry` usando el campo `task_type`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNode {
    /// Tipo de tarea a ejecutar. Debe corresponder a una tarea
    /// registrada en el `TaskRegistry`.
    pub task_id: TaskId,
}
