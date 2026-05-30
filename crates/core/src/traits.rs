use crate::context::ModuleContext;
use async_trait::async_trait;
use schemars::JsonSchema;

#[async_trait]
pub trait WorkflowModule: Send + Sync {
    /// Identificador único del módulo
    fn uuid(&self) -> &'static str;

    /// Definición de los puertos de entrada
    fn input_ports(&self) -> &[PortDef];
    /// Definición de los puertos de salida
    fn output_ports(&self) -> &[PortDef];

    /// Lógica que transforma inputs → outputs
    async fn execute(
        &self,
        ctx: WorkflowContext,
        inputs: PrimitiveMap,
    ) -> Result<PrimitiveMap, ModuleError>;
}

/// Definición de un puerto individual
pub struct PortDef {
    pub name: &'static str,
    pub schema: Option<JsonSchema>,
    pub required: bool,
}
