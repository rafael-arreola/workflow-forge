use crate::{
    binding,
    builder::Composition,
    schema::{CompiledSchema, OfflineSchemas, check_value},
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use workflow_forge_protocol::*;

pub(crate) struct CompiledOperation {
    pub descriptor: OperationDescriptor,
    pub operation: Arc<dyn Operation>,
    pub config: CompiledSchema,
    pub input: CompiledSchema,
    pub output: CompiledSchema,
}
impl CompiledOperation {
    pub fn new(
        operation: Arc<dyn Operation>,
        schemas: &OfflineSchemas,
        limits: &Limits,
    ) -> Result<Self, ForgeError> {
        let d = operation.descriptor().clone();
        let r = &d.revision;
        if !r.id.contains('.')
            || [&r.id, &r.contract, &r.implementation]
                .iter()
                .any(|s| s.is_empty() || s.as_str() == "latest" || s.contains('*'))
            || d.schema_dialect != SCHEMA_DIALECT
            || (d.effect == EffectKind::Pure && d.repetition != Repetition::Safe)
        {
            return Err(ForgeError::new(
                "definition.invalid",
                "Operation descriptor has invalid identities, dialect or effect guarantees",
            ));
        }
        for schema in [&d.config_schema, &d.input_schema, &d.output_schema] {
            if schema.is_object()
                && schema.get("$schema").and_then(Value::as_str) != Some(SCHEMA_DIALECT)
            {
                return Err(ForgeError::new(
                    "schema.invalid",
                    "Object schemas in operation descriptors must declare their dialect",
                ));
            }
        }
        Ok(Self {
            config: CompiledSchema::compile(&d.config_schema, schemas, limits)?,
            input: CompiledSchema::compile(&d.input_schema, schemas, limits)?,
            output: CompiledSchema::compile(&d.output_schema, schemas, limits)?,
            descriptor: d,
            operation,
        })
    }
}

pub(crate) struct PreparedNode {
    pub definition: NodeDefinition,
    pub operation: Arc<CompiledOperation>,
}
pub(crate) struct Plan {
    pub composition: String,
    pub definition: WorkflowDefinition,
    pub sequence: Vec<PreparedNode>,
    pub input: CompiledSchema,
    pub output: CompiledSchema,
    pub warnings: Vec<Diagnostic>,
}

#[derive(Clone)]
pub struct PreparedWorkflow(pub(crate) Arc<Plan>);
impl PreparedWorkflow {
    pub fn definition(&self) -> &WorkflowDefinition {
        &self.0.definition
    }
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.0.warnings
    }
}

fn located(mut error: ForgeError, node: Option<&str>, field: &str) -> ForgeError {
    for d in &mut error.diagnostics {
        d.phase = "prepare".into();
        d.location.node = node.map(str::to_owned);
        d.location.field = field.into();
    }
    error
}

pub(crate) fn prepare(
    composition: &Composition,
    definition: WorkflowDefinition,
) -> Result<PreparedWorkflow, ForgeError> {
    let limits = &composition.limits;
    check_value(
        &serde_json::to_value(&definition).expect("definition serializes"),
        limits.document_bytes,
        limits.json_depth,
    )?;
    if definition.format != WORKFLOW_FORMAT
        || definition.schema_dialect != SCHEMA_DIALECT
        || definition.id.is_empty()
        || definition.revision.is_empty()
    {
        return Err(ForgeError::new(
            "definition.invalid",
            "Definition format, dialect and identity must be explicit and supported",
        ));
    }
    if definition.nodes.is_empty() || definition.nodes.len() > limits.nodes {
        return Err(ForgeError::new(
            "definition.invalid",
            "Definition has an invalid number of nodes",
        ));
    }
    let mut nodes = BTreeMap::new();
    let mut diagnostics = Vec::new();
    for node in &definition.nodes {
        if node.id.is_empty() || nodes.insert(node.id.clone(), node).is_some() {
            diagnostics.push(
                Diagnostic::new(
                    "definition.invalid",
                    "Node identities must be nonempty and unique",
                )
                .at("prepare", Some(&node.id), "/nodes"),
            );
        }
        if node.kind != "operation" {
            diagnostics.push(
                Diagnostic::new(
                    "capability.unsupported",
                    "This profile supports operation sequences",
                )
                .at("prepare", Some(&node.id), "/kind"),
            );
        }
    }
    if !diagnostics.is_empty() {
        return Err(ForgeError { diagnostics });
    }
    let mut next = BTreeMap::new();
    let mut predecessors = BTreeMap::new();
    let mut edges = BTreeSet::new();
    for edge in &definition.edges {
        if !nodes.contains_key(&edge.from) || !nodes.contains_key(&edge.to) {
            diagnostics.push(
                Diagnostic::new("reference.missing", "Edge references an unknown node")
                    .at("prepare", None, "/edges"),
            );
            continue;
        }
        if !edges.insert(edge.clone()) {
            diagnostics.push(
                Diagnostic::new("definition.invalid", "Duplicate edge")
                    .at("prepare", None, "/edges"),
            );
        }
        if next.insert(edge.from.clone(), edge.to.clone()).is_some()
            || predecessors
                .insert(edge.to.clone(), edge.from.clone())
                .is_some()
        {
            diagnostics.push(
                Diagnostic::new(
                    "capability.unsupported",
                    "Forks and joins require the advanced control profile",
                )
                .at("prepare", None, "/edges"),
            );
        }
    }
    if !nodes.contains_key(&definition.entry) || predecessors.contains_key(&definition.entry) {
        diagnostics.push(
            Diagnostic::new(
                "definition.invalid",
                "Entry must exist and have no predecessor",
            )
            .at("prepare", None, "/entry"),
        );
    }
    if !diagnostics.is_empty() {
        return Err(ForgeError { diagnostics });
    }
    let mut order = Vec::new();
    let mut visited = BTreeSet::new();
    let mut cursor = Some(definition.entry.clone());
    while let Some(id) = cursor {
        if !visited.insert(id.clone()) {
            return Err(ForgeError::new(
                "definition.invalid",
                "Control cycles are not supported",
            ));
        }
        order.push(id.clone());
        cursor = next.get(&id).cloned();
    }
    if visited.len() != nodes.len() {
        return Err(ForgeError::new(
            "definition.invalid",
            "All nodes must be reachable from entry",
        ));
    }
    let input = CompiledSchema::compile(&definition.input_schema, &composition.schemas, limits)
        .map_err(|e| located(e, None, "/input_schema"))?;
    let output = CompiledSchema::compile(&definition.output_schema, &composition.schemas, limits)
        .map_err(|e| located(e, None, "/output_schema"))?;
    let mut available = BTreeSet::new();
    let mut sequence = Vec::new();
    let mut warnings = Vec::new();
    for id in order {
        let node = nodes[&id];
        let Some(operation) = composition.operations.get(&node.operation) else {
            diagnostics.push(
                Diagnostic::new("reference.missing", "Operation revision is unavailable").at(
                    "prepare",
                    Some(&id),
                    "/operation",
                ),
            );
            available.insert(id);
            continue;
        };
        if operation.descriptor.effect == EffectKind::Write
            || operation.descriptor.repetition != Repetition::Safe
        {
            diagnostics.push(
                Diagnostic::new(
                    "capability.unsupported",
                    "F-1 supports pure operations and repeatable reads",
                )
                .at("prepare", Some(&id), "/operation"),
            );
        }
        if let Err(e) = operation.config.validate(&node.config, limits) {
            diagnostics.extend(located(e, Some(&id), "/config").diagnostics);
        }
        if let Err(e) = binding::check(&node.input, &available, limits) {
            diagnostics.extend(located(e, Some(&id), "/input").diagnostics);
        }
        if let Binding::Literal(value) = &node.input {
            if let Err(e) = operation.input.validate(value, limits) {
                diagnostics.extend(located(e, Some(&id), "/input").diagnostics);
            }
        } else {
            warnings.push(
                Diagnostic::new(
                    "compatibility.unknown",
                    "Data compatibility will be validated at invocation",
                )
                .at("prepare", Some(&id), "/input"),
            );
        }
        sequence.push(PreparedNode {
            definition: node.clone(),
            operation: operation.clone(),
        });
        available.insert(id);
    }
    if let Err(e) = binding::check(&definition.output, &available, limits) {
        diagnostics.extend(located(e, None, "/output").diagnostics);
    }
    if !diagnostics.is_empty() {
        return Err(ForgeError { diagnostics });
    }
    Ok(PreparedWorkflow(Arc::new(Plan {
        composition: composition.id.clone(),
        definition,
        sequence,
        input,
        output,
        warnings,
    })))
}
