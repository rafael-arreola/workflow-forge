use serde::{Deserialize, Serialize};

/// Representación de los datos de un flujo de trabajo
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowData(pub serde_json::Value);

/// Permite dereferenciar `WorkflowData` como `serde_json::Value`
impl std::ops::Deref for WorkflowData {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Permite convertir un `serde_json::Value` en `WorkflowData`
impl From<serde_json::Value> for WorkflowData {
    fn from(value: serde_json::Value) -> Self {
        WorkflowData(value)
    }
}

/// Permite convertir un `WorkflowData` en `serde_json::Value`
impl From<&WorkflowData> for serde_json::Value {
    fn from(data: &WorkflowData) -> Self {
        data.0.clone()
    }
}

pub struct WorkflowError {
    pub code: String,
    pub message: String,
    pub payload: Option<WorkflowData>,
    pub response: Option<WorkflowData>,
}

pub type WorkflowResult = Result<serde_json::Value, WorkflowError>;
