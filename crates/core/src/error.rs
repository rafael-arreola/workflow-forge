use serde::{Deserialize, Serialize};

use crate::task::WorkflowData;

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
    /// Datos que originaron el error (boxed para mantener el error barato de mover)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Box<WorkflowData>>,
    /// Datos parciales generados antes del fallo
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Box<WorkflowData>>,
    /// Causa raíz (error interno del sistema)
    #[serde(skip)]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl WorkflowError {
    /// Crea un error con código y mensaje; el resto de campos en `None`
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            source_task: None,
            payload: None,
            response: None,
            source: None,
        }
    }

    /// Asigna la tarea/nodo de origen
    pub fn with_source_task(mut self, source_task: impl Into<String>) -> Self {
        self.source_task = Some(source_task.into());
        self
    }
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
