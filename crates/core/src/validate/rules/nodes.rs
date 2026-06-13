//! Reglas específicas de kinds de nodo (foreach, loop, subworkflow) y de la
//! sección `workflows` inline.

use std::collections::HashSet;

use crate::error::{WorkflowError, codes};
use crate::spec::node::NodeKind;
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{ValidationCtx, ValidationRule};

/// Un foreach debe declarar `concurrency >= 1`.
pub struct ForeachConcurrency;

impl ValidationRule for ForeachConcurrency {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::FOREACH_INVALID_CONCURRENCY]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        _ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for node in &workflow.nodes {
            if let NodeKind::Foreach(foreach) = &node.kind
                && foreach.concurrency == 0
            {
                errors.push(
                    WorkflowError::new(
                        codes::FOREACH_INVALID_CONCURRENCY,
                        format!(
                            "El foreach '{}' declara concurrency 0; debe ser >= 1",
                            node.id
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// Un loop debe declarar `max_iterations >= 1`.
pub struct LoopMaxIterations;

impl ValidationRule for LoopMaxIterations {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::LOOP_INVALID_MAX_ITERATIONS]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        _ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for node in &workflow.nodes {
            if let NodeKind::Loop(lp) = &node.kind
                && lp.max_iterations == 0
            {
                errors.push(
                    WorkflowError::new(
                        codes::LOOP_INVALID_MAX_ITERATIONS,
                        format!(
                            "El loop '{}' declara max_iterations 0; debe ser >= 1",
                            node.id
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// Un nodo subworkflow debe declarar el nombre del workflow hijo.
pub struct SubworkflowName;

impl ValidationRule for SubworkflowName {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::SUBWORKFLOW_MISSING_NAME]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        _ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for node in &workflow.nodes {
            if let NodeKind::Subworkflow(sub) = &node.kind
                && sub.workflow.is_empty()
            {
                errors.push(
                    WorkflowError::new(
                        codes::SUBWORKFLOW_MISSING_NAME,
                        format!(
                            "El nodo subworkflow '{}' no declara el nombre del workflow hijo",
                            node.id
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// La sección `workflows` inline no admite nombres repetidos.
/// (La validación recursiva de cada hijo la hace el pipeline.)
pub struct InlineWorkflowNames;

impl ValidationRule for InlineWorkflowNames {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::SUBWORKFLOW_DUPLICATE_NAME]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        _ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        let mut names: HashSet<&str> = HashSet::new();
        for child in &workflow.workflows {
            if !names.insert(child.name.as_str()) {
                errors.push(WorkflowError::new(
                    codes::SUBWORKFLOW_DUPLICATE_NAME,
                    format!(
                        "La sección `workflows` declara más de un workflow llamado '{}'",
                        child.name
                    ),
                ));
            }
        }
    }
}
