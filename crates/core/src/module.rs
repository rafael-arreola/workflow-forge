use crate::{context::ModuleContext, primitive::PrimitiveMap};
use async_trait::async_trait;

#[async_trait]
pub trait Module: Send + Sync {
    /// Identificador único del tipo
    fn module_type(&self) -> &'static str;

    /// Declaración de puertos (entrada/salida)
    fn ports(&self) -> ModulePorts;

    /// Lógica que transforma inputs → outputs
    async fn execute(
        &self,
        ctx: ModuleContext,
        inputs: PrimitiveMap,
    ) -> Result<PrimitiveMap, ModuleError>;
}

/// Describe los puertos de un módulo
pub struct ModulePorts {
    pub input: Vec<PortDef>,
    pub output: Vec<PortDef>,
}

/// Definición de un puerto individual
pub struct PortDef {
    pub name: &'static str,
    pub schema: Option<Schema>,
    pub required: bool,
}
