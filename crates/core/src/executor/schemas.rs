use std::collections::HashMap;

use serde_json::Value;

use crate::error::WorkflowError;
use crate::node::{NodeId, NodeKind};
use crate::registry::TaskRegistry;
use crate::task::TaskId;
use crate::workflow::WorkflowDefinition;

/// Validadores JSON Schema precompilados al construir el executor.
/// Los schemas son estáticos por definición: compilarlos en cada ejecución
/// de nodo (y en cada retry) es trabajo repetido.
#[derive(Default)]
pub(crate) struct CompiledSchemas {
    /// Validadores de nodos start (trigger) y end (resultado final)
    pub(crate) nodes: HashMap<NodeId, jsonschema::Validator>,
    /// Validadores (input, output) por tarea referenciada en el workflow
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
                        "INVALID_SCHEMA",
                        format!("Schema inválido en {where_}: {e}"),
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
                && let Some(validator) = compile(schema, format!("el nodo '{}'", node.id))
            {
                compiled.nodes.insert(node.id.clone(), validator);
            }

            if let NodeKind::Task(task_node) = &node.kind
                && !compiled.tasks.contains_key(&task_node.task)
                && let Some(task) = registry.get(&task_node.task)
            {
                let manifest = task.manifest();
                let input = manifest
                    .input_schema
                    .as_ref()
                    .and_then(|s| compile(s, format!("el input de la tarea '{}'", manifest.id)));
                let output = manifest
                    .output_schema
                    .as_ref()
                    .and_then(|s| compile(s, format!("el output de la tarea '{}'", manifest.id)));
                compiled
                    .tasks
                    .insert(task_node.task.clone(), (input, output));
            }
        }

        if errors.is_empty() {
            Ok(compiled)
        } else {
            Err(errors)
        }
    }
}

/// Valida un `Value` contra un validador precompilado.
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
