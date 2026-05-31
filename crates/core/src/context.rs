use std::time::Instant;

use uuid::Uuid;

/// Contexto inmutable compartido con todos los módulos durante la ejecución.
/// Proporciona trazabilidad (ID de workflow) y medición de tiempo.
#[derive(Debug)]
pub struct WorkflowContext {
    /// Identificador único de la ejecución actual (UUID v7)
    workflow_id: String,
    /// Instante en que inició la ejecución
    execution_at: Instant,
}

impl WorkflowContext {
    /// Crea un nuevo contexto con un UUID v7 y marca de tiempo actual
    pub fn new() -> Self {
        let workflow_id = Uuid::now_v7().to_string();
        Self {
            workflow_id,
            execution_at: Instant::now(),
        }
    }

    /// Devuelve el identificador único de la ejecución
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Tiempo transcurrido desde el inicio de la ejecución
    pub fn elapsed(&self) -> std::time::Duration {
        self.execution_at.elapsed()
    }
}

impl Default for WorkflowContext {
    fn default() -> Self {
        Self::new()
    }
}
