//! Rules specific to node kinds (foreach, loop, subworkflow) and the
//! inline `workflows` section.

use std::collections::HashSet;

use crate::error::{WorkflowError, codes};
use crate::spec::node::NodeKind;
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{ValidationCtx, ValidationRule};

/// A foreach must declare `concurrency >= 1`.
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
                            "Foreach '{}' declares concurrency 0; must be >= 1",
                            node.id
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// A loop must declare `max_iterations >= 1`.
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
                            "Loop '{}' declares max_iterations 0; must be >= 1",
                            node.id
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// A subworkflow node must declare the child workflow name.
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
                            "Subworkflow node '{}' does not declare the child workflow name",
                            node.id
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// The inline `workflows` section disallows duplicate names.
/// (Recursive validation of each child is done by the pipeline.)
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
                        "The `workflows` section declares more than one workflow named '{}'",
                        child.name
                    ),
                ));
            }
        }
    }
}
