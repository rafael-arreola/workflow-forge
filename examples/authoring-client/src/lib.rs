//! An authoring consumer of public contracts, with no engine dependency.
//!
//! These helpers build drafts. The host's prepare operation remains authoritative
//! for configuration, graph, schema and capability validation.
use serde_json::{Value, json};
use workflow_forge_protocol::*;

/// Select exactly one catalog revision. No "latest" fallback or inferred config.
pub fn one_operation(
    catalog: &[OperationDescriptor],
    selected: &OperationRevision,
    workflow_id: &str,
    revision: &str,
    config: Value,
) -> Result<WorkflowDefinition, ForgeError> {
    let mut matches = catalog.iter().filter(|d| &d.revision == selected);
    let descriptor = matches.next().ok_or_else(|| {
        authoring_error(
            "authoring.operation_missing",
            "Select an available exact revision",
            None,
        )
    })?;
    if matches.next().is_some() {
        return Err(authoring_error(
            "authoring.catalog_ambiguous",
            "The catalog contains duplicate operation revisions",
            None,
        ));
    }
    if descriptor.schema_dialect != SCHEMA_DIALECT {
        return Err(authoring_error(
            "authoring.dialect_unsupported",
            "This consumer does not support the catalog schema dialect",
            None,
        ));
    }
    Ok(WorkflowDefinition {
        format: WORKFLOW_FORMAT.into(),
        id: workflow_id.into(),
        revision: revision.into(),
        schema_dialect: descriptor.schema_dialect.clone(),
        input_schema: descriptor.input_schema.clone(),
        output_schema: descriptor.output_schema.clone(),
        entry: "step".into(),
        nodes: vec![NodeDefinition {
            id: "step".into(),
            input: select(DataSource::Input, None),
            instruction: Instruction::Operation {
                operation: descriptor.revision.clone(),
                config,
                retry: RetryPolicy::default(),
            },
        }],
        edges: Vec::new(),
        output: select(DataSource::Node, Some("step")),
        presentation: Default::default(),
    })
}

/// This client's layout convention is opaque to the engine. Preserve other
/// presentation keys, and reject a namespace collision instead of overwriting it.
pub fn move_node(
    document: &mut WorkflowDefinition,
    node: &str,
    x: f64,
    y: f64,
) -> Result<(), ForgeError> {
    if !document.nodes.iter().any(|n| n.id == node) {
        return Err(authoring_error(
            "authoring.node_missing",
            "The node is absent from the document",
            Some(node),
        ));
    }
    if !x.is_finite() || !y.is_finite() {
        return Err(authoring_error(
            "authoring.position_invalid",
            "Node coordinates must be finite",
            Some(node),
        ));
    }
    let nodes = document
        .presentation
        .entry("nodes".into())
        .or_insert_with(|| json!({}));
    let nodes = nodes.as_object_mut().ok_or_else(|| {
        authoring_error(
            "authoring.presentation_conflict",
            "Node metadata is not an object",
            Some(node),
        )
    })?;
    let metadata = nodes.entry(node).or_insert_with(|| json!({}));
    let metadata = metadata.as_object_mut().ok_or_else(|| {
        authoring_error(
            "authoring.presentation_conflict",
            "Node metadata is not an object",
            Some(node),
        )
    })?;
    metadata.insert("position".into(), json!({"x":x,"y":y}));
    Ok(())
}

fn select(source: DataSource, node: Option<&str>) -> Binding {
    Binding::Select(Selection {
        source,
        node: node.map(str::to_owned),
        pointer: String::new(),
        fallback: None,
    })
}

fn authoring_error(code: &str, message: &str, node: Option<&str>) -> ForgeError {
    Diagnostic::new(code, message)
        .at(
            "authoring",
            node,
            if node.is_some() {
                "/presentation"
            } else {
                "/catalog"
            },
        )
        .into()
}
