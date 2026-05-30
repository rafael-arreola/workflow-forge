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

/// Representación de un error en un flujo de trabajo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowError {
    pub code: String,
    pub message: String,
    pub payload: Option<WorkflowData>,
    pub response: Option<WorkflowData>,
}

/// Implementa `Display` para `WorkflowError` para mostrar un mensaje legible
impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

/// Implementa `Error` para `WorkflowError` para compatibilidad con `std::error::Error`
impl std::error::Error for WorkflowError {}

pub type WorkflowResult = Result<WorkflowData, WorkflowError>;
