//! The new public entry point. Legacy spec-1 exports remain during migration and
//! will be retired when the service and official modules have moved to spec 2.
use std::sync::Arc;
pub use workflow_forge_engine::*;
pub use workflow_forge_modules as modules;
pub use workflow_forge_protocol::*;

/// Official composition over the same builder available to third-party hosts.
pub struct WorkflowBuilder(EngineBuilder);
impl WorkflowBuilder {
    pub fn standard() -> Self {
        Self::for_scope("default")
    }
    pub fn for_scope(scope: &str) -> Self {
        let mut builder = EngineBuilder::new(scope)
            .execution_store(Arc::new(modules::MemoryExecutionStore::default()))
            .secret_provider(Arc::new(modules::MemorySecrets::default()))
            .artifact_store(Arc::new(modules::MemoryArtifacts::default()))
            .observer(Arc::new(modules::NoopObserver));
        builder = builder.backoff_policy(Arc::new(modules::ExponentialBackoff));
        builder
            .register_bundle(modules::data_operations())
            .expect("official data descriptors are valid");
        Self(builder)
    }
    pub fn limits(self, limits: Limits) -> Self {
        Self(self.0.limits(limits))
    }
    pub fn execution_store(self, store: Arc<dyn ExecutionStore>) -> Self {
        Self(self.0.execution_store(store))
    }
    pub fn secret_provider(self, provider: Arc<dyn SecretProvider>) -> Self {
        Self(self.0.secret_provider(provider))
    }
    pub fn artifact_store(self, provider: Arc<dyn ArtifactStore>) -> Self {
        Self(self.0.artifact_store(provider))
    }
    pub fn observer(self, observer: Arc<dyn ExecutionObserver>) -> Self {
        Self(self.0.observer(observer))
    }
    pub fn backoff_policy(self, policy: Arc<dyn BackoffPolicy>) -> Self {
        Self(self.0.backoff_policy(policy))
    }
    pub fn register_bundle(&mut self, bundle: OperationBundle) -> Result<(), ForgeError> {
        self.0.register_bundle(bundle)
    }
    pub fn register_schemas(&mut self, schemas: Vec<SchemaResource>) -> Result<(), ForgeError> {
        self.0.register_schemas(schemas)
    }
    pub fn register_workflow(&mut self, definition: WorkflowDefinition) -> Result<(), ForgeError> {
        self.0.register_workflow(definition)
    }
    pub fn build(self) -> Result<EngineAssembly, ForgeError> {
        self.0.build()
    }
}
