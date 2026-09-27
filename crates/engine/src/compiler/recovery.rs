use super::*;

fn unavailable(message: &str) -> ForgeError {
    ForgeError::new("recovery.unavailable", message)
}

fn schemas(package: &ResolvedPackage) -> Result<OfflineSchemas, ForgeError> {
    let mut resources = BTreeMap::new();
    for resource in &package.schemas {
        if resource.revision.is_empty()
            || resources
                .insert(resource.uri.clone(), resource.schema.clone())
                .is_some()
        {
            return Err(unavailable(
                "Checkpoint contains conflicting schema identities",
            ));
        }
    }
    Ok(OfflineSchemas(Arc::new(resources)))
}

pub(crate) fn recover_schema(
    package: &ResolvedPackage,
    value: &Value,
    limits: &Limits,
) -> Result<CompiledSchema, ForgeError> {
    if package.schemas.len() > limits.schema_resources {
        return Err(unavailable("Accepted schemas exceed the host budget"));
    }
    CompiledSchema::compile(value, &schemas(package)?, limits)
}

/// Used by both recovered execution and explicit reconciliation. Validators use
/// the accepted schema package, even if authoring resources changed at boot.
pub(crate) fn recover_operation(
    composition: &Composition,
    package: &ResolvedPackage,
    revision: &OperationRevision,
) -> Result<Arc<CompiledOperation>, ForgeError> {
    let descriptor = package
        .operations
        .iter()
        .find(|d| &d.revision == revision)
        .ok_or_else(|| unavailable("Checkpoint is missing an operation descriptor"))?;
    let current = composition
        .operations
        .get(revision)
        .ok_or_else(|| unavailable("The accepted operation implementation is unavailable"))?;
    if descriptor.semantic_value() != current.operation.descriptor().semantic_value() {
        return Err(unavailable(
            "Operation contract changed without a new revision",
        ));
    }
    Ok(Arc::new(CompiledOperation::new(
        current.operation.clone(),
        &schemas(package)?,
        &composition.limits,
    )?))
}

pub(crate) fn recover(
    composition: &Composition,
    run: &RunSnapshot,
) -> Result<PreparedWorkflow, ForgeError> {
    if run.checkpoint_format != CHECKPOINT_FORMAT {
        return Err(unavailable(
            "Unsupported checkpoint format; dependencies cannot be inferred",
        ));
    }
    if run.package.definitions.len() > 1000
        || run.package.schemas.len() > composition.limits.schema_resources
        || serde_json::to_vec(&run.package)
            .expect("package serializes")
            .len()
            > composition.limits.plan_bytes
    {
        return Err(unavailable("Checkpoint package exceeds the host budget"));
    }
    let mut restored = composition.clone();
    restored.workflows.clear();
    for definition in &run.package.definitions {
        let revision = WorkflowRevision {
            id: definition.id.clone(),
            revision: definition.revision.clone(),
        };
        if restored
            .workflows
            .insert(revision, definition.clone())
            .is_some()
        {
            return Err(unavailable(
                "Checkpoint contains duplicate workflow revisions",
            ));
        }
    }
    let root = WorkflowRevision {
        id: run.definition.id.clone(),
        revision: run.definition.revision.clone(),
    };
    if restored.workflows.get(&root) != Some(&run.definition) {
        return Err(unavailable(
            "Root definition differs from its accepted package",
        ));
    }
    restored.schemas = schemas(&run.package)?;
    restored.schema_resources = run
        .package
        .schemas
        .iter()
        .map(|r| (r.uri.clone(), r.clone()))
        .collect();
    restored.operations.clear();
    for descriptor in &run.package.operations {
        let operation = recover_operation(composition, &run.package, &descriptor.revision)?;
        if restored
            .operations
            .insert(descriptor.revision.clone(), operation)
            .is_some()
        {
            return Err(unavailable(
                "Checkpoint contains duplicate operation revisions",
            ));
        }
    }
    let plan = prepare(&restored, run.definition.clone())?;
    if plan.0.package.semantic_value() != run.package.semantic_value() {
        return Err(unavailable(
            "Checkpoint does not contain the exact resolved dependency set",
        ));
    }
    if plan
        .0
        .resources
        .iter()
        .any(|r| !run.resources.contains("*") && !run.resources.contains(r))
    {
        return Err(unavailable(
            "Checkpoint lacks the resources required by its accepted plan",
        ));
    }
    Ok(plan)
}
