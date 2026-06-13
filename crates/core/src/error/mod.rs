//! La familia **error**: el error estructurado del engine y su catálogo.
//!
//! [`WorkflowError`] es el único tipo de error del crate: viaja serializado
//! entre nodos (rutas `on: error`), en los eventos de observabilidad y como
//! resultado de validación. El módulo [`codes`] es el catálogo completo de
//! códigos que el engine puede emitir.

pub mod codes;

use serde::{Deserialize, Serialize};

use crate::task::WorkflowData;

/// Error estructurado que una tarea puede devolver durante la ejecución.
/// Incluye trazabilidad hasta la tarea origen y soporta encadenamiento.
#[derive(Debug, Serialize, Deserialize, thiserror::Error)]
pub struct WorkflowError {
    /// Código único que identifica el tipo de error (ver [`codes`])
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
    /// Pista para la política de reintentos: espera **al menos** estos
    /// milisegundos antes del siguiente intento. La fija la tarea cuando el
    /// destino indica cuándo reintentar (p. ej. el header HTTP `Retry-After`);
    /// el engine toma `max(backoff, retry_after_ms)`. Sin reintentos
    /// configurados en el nodo no tiene efecto.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
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
            retry_after_ms: None,
            source: None,
        }
    }

    /// Asigna la tarea/nodo de origen
    pub fn with_source_task(mut self, source_task: impl Into<String>) -> Self {
        self.source_task = Some(source_task.into());
        self
    }

    /// Fija la pista [`retry_after_ms`](Self::retry_after_ms): el engine
    /// esperará al menos este tiempo antes de reintentar.
    pub fn with_retry_after_ms(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
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
            retry_after_ms: self.retry_after_ms,
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
