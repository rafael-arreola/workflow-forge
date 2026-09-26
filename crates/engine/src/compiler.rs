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

mod control;

pub(crate) struct PreparedOperation {
    pub operation: Arc<CompiledOperation>,
    pub config: Value,
    pub retry: RetryPolicy,
}
pub(crate) struct PreparedNode {
    pub id: String,
    pub input: Binding,
    pub instruction: PreparedInstruction,
}
pub(crate) struct PreparedBody {
    pub sequence: Vec<PreparedNode>,
    pub output: Binding,
}
pub(crate) struct PreparedCase {
    pub id: String,
    pub when: Binding,
    pub body: Arc<PreparedBody>,
}
pub(crate) enum PreparedInstruction {
    Operation(PreparedOperation),
    Decision {
        cases: Vec<PreparedCase>,
        fallback: Option<(String, Arc<PreparedBody>)>,
    },
    Parallel {
        branches: BTreeMap<String, Arc<PreparedBody>>,
        concurrency: usize,
        errors: GroupErrors,
    },
    Foreach {
        items: Binding,
        body: Arc<PreparedBody>,
        concurrency: usize,
        errors: GroupErrors,
    },
    Loop {
        condition: Binding,
        body: Arc<PreparedBody>,
        max_iterations: usize,
        on_limit: LoopLimit,
    },
    Subworkflow(PreparedWorkflow),
}
pub(crate) struct Plan {
    pub composition: String,
    pub definition: WorkflowDefinition,
    pub body: Arc<PreparedBody>,
    pub input: CompiledSchema,
    pub output: CompiledSchema,
    pub warnings: Vec<Diagnostic>,
    pub resources: BTreeSet<String>,
    pub definitions: BTreeMap<WorkflowRevision, WorkflowDefinition>,
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
    control::Compiler::new(composition).workflow(definition, 0)
}
