pub struct ModuleContext {
    pub workflow_id: String,
    pub execution_id: String,
    pub logger: Logger,
    pub variables: Map<String, Value>,
}
