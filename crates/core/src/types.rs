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

/// Definición de un puerto individual de entrada o salida de una tarea
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortDef {
    /// Nombre identificador del puerto dentro de la tarea
    pub name: String,
    /// Indica si el puerto es obligatorio para la ejecución
    #[serde(default)]
    pub required: bool,
    /// Esquema JSON que valida los datos que transitan por este puerto
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<schemars::Schema>,
    /// Descripción legible del propósito del puerto
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}
