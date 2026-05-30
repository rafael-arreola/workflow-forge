use async_trait::async_trait;

use crate::{
    context::WorkflowContext,
    types::{WorkflowData, WorkflowResult},
};

#[async_trait]
pub trait WorkflowModule: Send + Sync {
    /// Identificador único del módulo
    fn uuid(&self) -> &str;

    /// Definición de los puertos de entrada
    fn input(&self) -> &[WorkflowDefinition];

    /// Definición de los puertos de salida
    fn output(&self) -> &[WorkflowDefinition];

    /// Lógica que transforma inputs → outputs
    async fn execute(&self, ctx: &WorkflowContext, inputs: &WorkflowData) -> WorkflowResult;
}

/// Definición de un puerto individual
#[derive(Debug, Clone)]
pub struct WorkflowDefinition {
    pub name: String,
    pub required: bool,
    pub schema: Option<schemars::Schema>,
    pub description: Option<String>,
}
