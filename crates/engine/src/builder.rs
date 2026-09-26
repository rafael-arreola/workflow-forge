use crate::{compiler::CompiledOperation, schema::OfflineSchemas};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use workflow_forge_protocol::*;

pub(crate) struct Composition {
    pub id: String,
    pub scope: String,
    pub limits: Limits,
    pub operations: BTreeMap<OperationRevision, Arc<CompiledOperation>>,
    pub schemas: OfflineSchemas,
    pub store: Arc<dyn ExecutionStore>,
    pub secrets: Arc<dyn SecretProvider>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub observer: Arc<dyn ExecutionObserver>,
}

pub struct EngineAssembly(pub(crate) Arc<Composition>);

/// Explicit composition. The facade supplies official defaults on top of this builder.
pub struct EngineBuilder {
    scope: String,
    limits: Limits,
    operations: BTreeMap<OperationRevision, Arc<CompiledOperation>>,
    modules: BTreeSet<String>,
    resources: BTreeMap<String, SchemaResource>,
    store: Option<Arc<dyn ExecutionStore>>,
    secrets: Option<Arc<dyn SecretProvider>>,
    artifacts: Option<Arc<dyn ArtifactStore>>,
    observer: Option<Arc<dyn ExecutionObserver>>,
}

impl EngineBuilder {
    pub fn new(scope: impl Into<String>) -> Self {
        Self {
            scope: scope.into(),
            limits: Limits::default(),
            operations: BTreeMap::new(),
            modules: BTreeSet::new(),
            resources: BTreeMap::new(),
            store: None,
            secrets: None,
            artifacts: None,
            observer: None,
        }
    }
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    pub fn execution_store(mut self, store: Arc<dyn ExecutionStore>) -> Self {
        self.store = Some(store);
        self
    }
    pub fn secret_provider(mut self, provider: Arc<dyn SecretProvider>) -> Self {
        self.secrets = Some(provider);
        self
    }
    pub fn artifact_store(mut self, provider: Arc<dyn ArtifactStore>) -> Self {
        self.artifacts = Some(provider);
        self
    }
    pub fn observer(mut self, observer: Arc<dyn ExecutionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn register_schemas(&mut self, resources: Vec<SchemaResource>) -> Result<(), ForgeError> {
        let mut staged = BTreeMap::new();
        let mut bytes = 0usize;
        for resource in resources {
            bytes = bytes.saturating_add(
                serde_json::to_vec(&resource)
                    .expect("schema resource serializes")
                    .len(),
            );
            if resource.revision.is_empty()
                || jsonschema::Uri::parse(resource.uri.as_str()).is_err()
                || resource.uri.contains('#')
                || self.resources.contains_key(&resource.uri)
                || staged.contains_key(&resource.uri)
            {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Schema identities must be absolute, versioned and unique",
                ));
            }
            crate::schema::check_value(
                &resource.schema,
                self.limits.document_bytes,
                self.limits.json_depth,
            )?;
            if resource
                .schema
                .get("$id")
                .is_some_and(|id| id.as_str() != Some(resource.uri.as_str()))
            {
                return Err(ForgeError::new(
                    "schema.invalid",
                    "Schema resource ID does not match its registered URI",
                ));
            }
            staged.insert(resource.uri.clone(), resource);
        }
        if bytes > self.limits.document_bytes
            || staged.len() + self.resources.len() > self.limits.schema_resources
        {
            return Err(ForgeError::new(
                "schema.invalid",
                "Schema package exceeds limits",
            ));
        }
        self.resources.extend(staged);
        Ok(())
    }

    /// Validate the whole contribution before changing any registry entries.
    pub fn register_bundle(&mut self, bundle: OperationBundle) -> Result<(), ForgeError> {
        let module = &bundle.module;
        if module.protocol_version != PROTOCOL_VERSION {
            return Err(ForgeError::new(
                "capability.unsupported",
                "Module protocol version is unsupported",
            ));
        }
        if module.id.is_empty() || module.version.is_empty() || self.modules.contains(&module.id) {
            return Err(ForgeError::new(
                "state.conflict",
                "Module identity is empty or already registered",
            ));
        }
        let mut staged = BTreeMap::new();
        let schemas = self.offline_schemas();
        for operation in bundle.operations {
            let descriptor = operation.descriptor();
            let key = descriptor.revision.clone();
            if self.operations.contains_key(&key) || staged.contains_key(&key) {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Operation revision is already registered",
                ));
            }
            let compiled = CompiledOperation::new(operation, &schemas, &self.limits)?;
            staged.insert(key, Arc::new(compiled));
        }
        let declared: BTreeSet<_> = module.exports.iter().cloned().collect();
        let actual: BTreeSet<_> = staged.keys().cloned().collect();
        if declared.len() != module.exports.len() || declared != actual {
            return Err(ForgeError::new(
                "definition.invalid",
                "Module exports do not match its operations",
            ));
        }
        self.operations.extend(staged);
        self.modules.insert(module.id.clone());
        Ok(())
    }

    fn offline_schemas(&self) -> OfflineSchemas {
        OfflineSchemas(Arc::new(
            self.resources
                .iter()
                .map(|(id, r)| (id.clone(), r.schema.clone()))
                .collect(),
        ))
    }

    pub fn build(self) -> Result<EngineAssembly, ForgeError> {
        let l = &self.limits;
        if self.scope.is_empty()
            || [
                l.document_bytes,
                l.schema_resources,
                l.nodes,
                l.json_depth,
                l.binding_depth,
                l.binding_steps,
                l.value_bytes,
                l.run_bytes,
                l.active_runs,
                l.concurrent_attempts,
                l.terminal_runs,
                l.receipt_count,
            ]
            .contains(&0)
            || l.attempt_timeout_ms == 0
            || l.run_timeout_ms == 0
            || l.retention_ms == 0
            || l.receipt_ttl_ms == 0
            || l.active_runs.checked_add(l.pending_runs).is_none()
        {
            return Err(ForgeError::new(
                "definition.invalid",
                "Composition scope and limits must be valid and nonzero",
            ));
        }
        let missing = || {
            ForgeError::new(
                "capability.unsupported",
                "Composition is missing an infrastructure provider",
            )
        };
        let schemas = self.offline_schemas();
        let store = self.store.ok_or_else(missing)?;
        if store.capabilities().checkpoint_format != 1 || store.capabilities().durable {
            return Err(ForgeError::new(
                "capability.unsupported",
                "This engine profile requires an ephemeral format-1 store",
            ));
        }
        let secrets = self.secrets.ok_or_else(missing)?;
        for operation in self.operations.values() {
            for resource in &operation.descriptor.required_resources {
                if resource == "artifacts" {
                    continue;
                }
                if !resource
                    .strip_prefix("secret:")
                    .is_some_and(|name| secrets.contains(&self.scope, name))
                {
                    return Err(ForgeError::new(
                        "resource.missing",
                        "A required module resource is unavailable",
                    ));
                }
            }
        }
        let mut operations = BTreeMap::new();
        for (key, operation) in self.operations {
            let compiled =
                CompiledOperation::new(operation.operation.clone(), &schemas, &self.limits)?;
            if compiled.descriptor.revision != key {
                return Err(ForgeError::new(
                    "state.conflict",
                    "An operation changed its descriptor during composition",
                ));
            }
            operations.insert(key, Arc::new(compiled));
        }
        Ok(EngineAssembly(Arc::new(Composition {
            id: uuid::Uuid::now_v7().to_string(),
            scope: self.scope,
            limits: self.limits,
            operations,
            schemas,
            store,
            secrets,
            artifacts: self.artifacts.ok_or_else(missing)?,
            observer: self.observer.ok_or_else(missing)?,
        })))
    }
}
