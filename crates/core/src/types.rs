use serde::{Deserialize, Serialize};

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

/// Error estructurado que una tarea puede devolver durante la ejecución.
/// Incluye trazabilidad hasta la tarea origen y soporta encadenamiento.
#[derive(Debug, Serialize, Deserialize, thiserror::Error)]
pub struct WorkflowError {
    /// Código único que identifica el tipo de error
    pub code: String,
    /// Mensaje descriptivo para el operador o desarrollador
    pub message: String,
    /// Identificador de la tarea que originó el error
    #[serde(default)]
    pub source_task: Option<String>,
    /// Datos que originaron el error
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<WorkflowData>,
    /// Datos parciales generados antes del fallo
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<WorkflowData>,
    /// Causa raíz (error interno del sistema)
    #[serde(skip)]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

// Clone manual: `source` no es Clone, se omite en la copia
impl Clone for WorkflowError {
    fn clone(&self) -> Self {
        Self {
            code: self.code.clone(),
            message: self.message.clone(),
            source_task: self.source_task.clone(),
            payload: self.payload.clone(),
            response: self.response.clone(),
            source: None,
        }
    }
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source_task {
            Some(task) => write!(f, "[{}] {}", task, self.message),
            None => write!(f, "{}", self.message),
        }
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
