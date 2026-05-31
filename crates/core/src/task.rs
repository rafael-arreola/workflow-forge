use async_trait::async_trait;

use crate::context::WorkflowContext;
use crate::types::{PortDef, WorkflowData, WorkflowResult};

/// Unidad mínima ejecutable dentro de un workflow.
/// Cada tarea declara sus puertos y define su lógica de transformación.
/// Debe implementarse para cada tipo de tarea concreta (addon).
#[async_trait]
pub trait Task: Send + Sync + 'static {
    /// Identificador del tipo de tarea (ej. "http_request", "transform").
    /// Usado por el registry para resolver definiciones en runtime.
    fn task_type(&self) -> &str;

    /// Puertos de entrada que reciben datos para la ejecución
    fn input_ports(&self) -> &[PortDef];

    /// Puertos de salida que emiten el resultado de la ejecución
    fn output_ports(&self) -> &[PortDef];

    /// Transforma los datos de entrada en la salida esperada.
    /// Recibe el contexto inmutable del workflow y los datos de entrada.
    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult;
}
