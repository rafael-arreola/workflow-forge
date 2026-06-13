use std::collections::HashMap;

use serde_json::Value;

use crate::error::{WorkflowError, codes};
use crate::spec::node::{NodeId, NodeKind};
use crate::spec::workflow::WorkflowDefinition;
use crate::task::{TaskId, TaskRegistry};

/// Precompiled JSON Schema validators, built when constructing the executor.
/// Schemas are static by definition: compiling them on every node execution
/// (and on every retry) is wasted work.
#[derive(Default)]
pub(crate) struct CompiledSchemas {
    /// Validators for start nodes (trigger) and end nodes (final result)
    pub(crate) nodes: HashMap<NodeId, jsonschema::Validator>,
    /// (input, output) validators per task referenced in the workflow
    pub(crate) tasks:
        HashMap<TaskId, (Option<jsonschema::Validator>, Option<jsonschema::Validator>)>,
}

impl CompiledSchemas {
    pub(crate) fn build(
        workflow: &WorkflowDefinition,
        registry: &TaskRegistry,
    ) -> Result<Self, Vec<WorkflowError>> {
        let mut compiled = Self::default();
        let mut errors: Vec<WorkflowError> = Vec::new();

        let mut compile =
            |schema: &schemars::Schema, where_: String| match serde_json::to_value(schema)
                .map_err(|e| e.to_string())
                .and_then(|json| jsonschema::validator_for(&json).map_err(|e| e.to_string()))
            {
                Ok(validator) => Some(validator),
                Err(e) => {
                    errors.push(WorkflowError::new(
                        codes::INVALID_SCHEMA,
                        format!("Invalid schema at {where_}: {e}"),
                    ));
                    None
                }
            };

        for node in &workflow.nodes {
            let schema = match &node.kind {
                NodeKind::Start(start) => start.schema.as_ref(),
                NodeKind::End(end) => end.schema.as_ref(),
                _ => None,
            };
            if let Some(schema) = schema
                && let Some(validator) = compile(schema, format!("node '{}'", node.id))
            {
                compiled.nodes.insert(node.id.clone(), validator);
            }

            let task_ref = match &node.kind {
                NodeKind::Task(task_node) => Some(&task_node.task),
                NodeKind::Foreach(foreach) => Some(&foreach.task),
                NodeKind::Loop(lp) => Some(&lp.task),
                _ => None,
            };
            if let Some(task_ref) = task_ref
                && !compiled.tasks.contains_key(task_ref)
                && let Some(task) = registry.get(task_ref)
            {
                let manifest = task.manifest();
                let input = manifest
                    .input_schema
                    .as_ref()
                    .and_then(|s| compile(s, format!("task '{}' input", manifest.id)));
                let output = manifest
                    .output_schema
                    .as_ref()
                    .and_then(|s| compile(s, format!("task '{}' output", manifest.id)));
                compiled.tasks.insert(task_ref.clone(), (input, output));
            }
        }

        if errors.is_empty() {
            Ok(compiled)
        } else {
            Err(errors)
        }
    }
}

/// Validates a `Value` against a precompiled validator.
pub(crate) fn validate_compiled(
    validator: &jsonschema::Validator,
    data: &Value,
) -> Result<(), String> {
    if validator.is_valid(data) {
        Ok(())
    } else {
        let errors: Vec<String> = validator.iter_errors(data).map(|e| e.to_string()).collect();
        Err(errors.join("; "))
    }
}
