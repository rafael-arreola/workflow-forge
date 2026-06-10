use serde::{Deserialize, Serialize};

use crate::error::WorkflowError;

/// Datos que circulan entre nodos durante la ejecución de un workflow.
/// Envoltorio sobre `serde_json::Value` con conversiones implícitas.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowData(pub serde_json::Value);

impl std::ops::Deref for WorkflowData {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<serde_json::Value> for WorkflowData {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

impl From<WorkflowData> for serde_json::Value {
    fn from(data: WorkflowData) -> Self {
        data.0
    }
}

impl From<&WorkflowData> for serde_json::Value {
    fn from(data: &WorkflowData) -> Self {
        data.0.clone()
    }
}

/// Resultado de la ejecución de una tarea: datos exitosos o error estructurado
pub type WorkflowResult = Result<WorkflowData, WorkflowError>;
