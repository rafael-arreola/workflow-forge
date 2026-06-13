//! The **validate** family: static rules on the definition.
//!
//! Everything that can be detected before execution is detected here.
//! Validation is a pipeline of rules ([`ValidationRule`]): each rule is a
//! unit with its declared codes, testable and documentable separately.
//! The built-in rules live in [`rules`] and run in order
//! ([`rules::BUILTIN`]); a host can add its own rules with
//! [`validate_with`].
//!
//! Validation **accumulates** all errors found (does not stop at the first
//! one), so a document can be corrected in a single pass.

mod context;
pub mod rules;

pub use context::ValidationCtx;

use crate::error::WorkflowError;
use crate::spec::workflow::WorkflowDefinition;

/// Spec versions that this core can execute
pub const SUPPORTED_SPECS: &[&str] = &["1.0"];

/// A validation rule: a verifiable invariant on the definition.
///
/// Rules run in order and share the error vector: a rule can inspect what has
/// already been reported (e.g. graph rules disable themselves if there are
/// broken references). Host custom rules run after the built-in ones.
pub trait ValidationRule: Send + Sync {
    /// Error codes this rule can emit (see [`crate::error::codes`]).
    /// It is living documentation: the rule set declares what is validated.
    fn codes(&self) -> &'static [&'static str];

    /// Verifies the invariant and appends any found errors to `errors`.
    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    );
}

/// Validates a workflow's structure with the built-in rules.
/// Accumulates and returns all errors found, not just the first.
pub fn validate(workflow: &WorkflowDefinition) -> Result<(), Vec<WorkflowError>> {
    validate_with(workflow, &[])
}

/// Like [`validate`], with additional host rules that run after the built-in
/// ones (also on every inline sub-workflow).
pub fn validate_with(
    workflow: &WorkflowDefinition,
    extra: &[&dyn ValidationRule],
) -> Result<(), Vec<WorkflowError>> {
    let mut errors: Vec<WorkflowError> = Vec::new();

    let ctx = ValidationCtx::build(workflow);
    for rule in rules::BUILTIN.iter().chain(extra.iter()) {
        rule.check(workflow, &ctx, &mut errors);
    }

    // Inline sub-workflows are validated recursively with the same rules;
    // their errors bubble up with the child's name as context
    for child in &workflow.workflows {
        if let Err(child_errors) = validate_with(child, extra) {
            errors.extend(child_errors.into_iter().map(|mut e| {
                e.message = format!("in inline sub-workflow '{}': {}", child.name, e.message);
                e
            }));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Verifies that every task referenced by the workflow is registered.
/// This is a separate validation because it depends on the
/// [`crate::task::TaskRegistry`], not just the document.
pub fn validate_tasks(
    workflow: &WorkflowDefinition,
    registry: &crate::task::TaskRegistry,
) -> Result<(), Vec<WorkflowError>> {
    use crate::error::codes;
    use crate::spec::node::NodeKind;

    let errors: Vec<WorkflowError> = workflow
        .nodes
        .iter()
        .filter_map(|node| {
            let task = match &node.kind {
                NodeKind::Task(task_node) => &task_node.task,
                NodeKind::Foreach(foreach) => &foreach.task,
                NodeKind::Loop(lp) => &lp.task,
                _ => return None,
            };
            (!registry.contains(task)).then(|| {
                WorkflowError::new(
                    codes::TASK_NOT_FOUND,
                    format!(
                        "Node '{}' references task '{}' which is not registered",
                        node.id, task
                    ),
                )
                .with_source_task(node.id.to_string())
            })
        })
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn wf(value: serde_json::Value) -> WorkflowDefinition {
        serde_json::from_value(value).expect("workflow deserializable")
    }

    fn codes(workflow: &WorkflowDefinition) -> Vec<String> {
        match validate(workflow) {
            Ok(()) => vec![],
            Err(errors) => errors.into_iter().map(|e| e.code).collect(),
        }
    }

    #[test]
    fn minimal_valid_workflow() {
        let workflow = wf(json!({
            "name": "ok", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [ { "from": "start", "to": "end" } ]
        }));
        assert_eq!(codes(&workflow), Vec::<String>::new());
    }

    #[test]
    fn detects_basic_problems() {
        let workflow = wf(json!({
            "spec": "9.9",
            "name": "bad", "version": "0.1.0",
            "nodes": [
                { "id": "a", "kind": "start" },
                { "id": "a", "kind": "end" }
            ],
            "edges": [ { "from": "a", "to": "ghost" } ]
        }));
        let found = codes(&workflow);
        for expected in ["UNSUPPORTED_SPEC", "DUPLICATE_NODE_ID", "UNKNOWN_NODE_REF"] {
            assert!(
                found.contains(&expected.to_string()),
                "missing {expected} in {found:?}"
            );
        }
    }

    #[test]
    fn exclusive_gateway_coherent_with_edges() {
        let workflow = wf(json!({
            "name": "gw", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "check", "kind": "gateway", "gateway": "exclusive", "branches": [
                    { "when": { "path": "$.trigger.x", "eq": 1 }, "edge": "ok" },
                    { "else": true, "edge": "fail" }
                ]},
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "check" },
                { "from": "check", "to": "end", "label": "ok" },
                { "from": "check", "to": "end", "label": "orphan" }
            ]
        }));
        let found = codes(&workflow);
        assert!(found.contains(&"GATEWAY_BRANCH_WITHOUT_EDGE".to_string())); // missing "fail"
        assert!(found.contains(&"GATEWAY_EDGE_WITHOUT_BRANCH".to_string())); // extra "orphan"
    }

    #[test]
    fn exclusive_gateway_rejects_duplicate_labels() {
        // Two edges with the same label: the handler would follow both
        // concurrently (accidental fan-out from an exclusive)
        let workflow = wf(json!({
            "name": "dup", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "check", "kind": "gateway", "gateway": "exclusive", "branches": [
                    { "when": { "path": "$.trigger.x", "eq": 1 }, "edge": "ok" },
                    { "else": true, "edge": "fail" }
                ]},
                { "id": "a", "kind": "task", "task": "noop" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "check" },
                { "from": "check", "to": "a", "label": "ok" },
                { "from": "check", "to": "end", "label": "ok" },
                { "from": "check", "to": "end", "label": "fail" },
                { "from": "a", "to": "end" }
            ]
        }));
        let found = codes(&workflow);
        assert_eq!(
            found
                .iter()
                .filter(|c| *c == "GATEWAY_DUPLICATE_EDGE_LABEL")
                .count(),
            1,
            "one error per duplicate label, found: {found:?}"
        );

        // Two branches toward the same label is also ambiguous
        let workflow = wf(json!({
            "name": "dup-branch", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "check", "kind": "gateway", "gateway": "exclusive", "branches": [
                    { "when": { "path": "$.trigger.x", "eq": 1 }, "edge": "ok" },
                    { "when": { "path": "$.trigger.y", "eq": 2 }, "edge": "ok" }
                ]},
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "check" },
                { "from": "check", "to": "end", "label": "ok" }
            ]
        }));
        assert!(codes(&workflow).contains(&"GATEWAY_DUPLICATE_EDGE_LABEL".to_string()));
    }

    #[test]
    fn detects_cycles_and_orphans() {
        let workflow = wf(json!({
            "name": "cyc", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "a", "kind": "task", "task": "noop" },
                { "id": "b", "kind": "task", "task": "noop" },
                { "id": "island", "kind": "task", "task": "noop" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "a" },
                { "from": "a", "to": "b" },
                { "from": "b", "to": "a" },
                { "from": "a", "to": "end" }
            ]
        }));
        let found = codes(&workflow);
        assert!(found.contains(&"CYCLE_DETECTED".to_string()));
        assert!(found.contains(&"UNREACHABLE_NODE".to_string()));
    }

    #[test]
    fn error_edge_cannot_enter_a_join() {
        let workflow = wf(json!({
            "name": "ej", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "fan", "kind": "gateway", "gateway": "parallel" },
                { "id": "a", "kind": "task", "task": "noop" },
                { "id": "b", "kind": "task", "task": "noop" },
                { "id": "meet", "kind": "gateway", "gateway": "join" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "fan" },
                { "from": "fan", "to": "a" },
                { "from": "fan", "to": "b" },
                { "from": "a", "to": "meet" },
                { "from": "b", "to": "meet" },
                { "from": "a", "on": "error", "to": "meet" },
                { "from": "meet", "to": "end" }
            ]
        }));
        assert!(codes(&workflow).contains(&"ERROR_EDGE_TO_JOIN".to_string()));
    }

    #[test]
    fn subworkflow_structurally_valid() {
        let workflow = wf(json!({
            "name": "sub", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "child", "kind": "subworkflow", "workflow": "other" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "child" },
                { "from": "child", "to": "end" }
            ]
        }));
        // The name is resolved when building the executor, not here
        assert_eq!(codes(&workflow), Vec::<String>::new());
    }

    #[test]
    fn inline_subworkflows_with_duplicate_name_or_invalid() {
        let inner_ok = json!({
            "name": "child", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [ { "from": "start", "to": "end" } ]
        });
        let inner_bad = json!({
            "name": "child", "version": "0.1.0",
            "nodes": [ { "id": "start", "kind": "start" } ],
            "edges": []
        });
        let workflow = wf(json!({
            "name": "parent", "version": "0.1.0",
            "workflows": [inner_ok, inner_bad],
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "child", "kind": "subworkflow", "workflow": "child" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "child" },
                { "from": "child", "to": "end" }
            ]
        }));
        let found = codes(&workflow);
        assert!(found.contains(&"SUBWORKFLOW_DUPLICATE_NAME".to_string()));
        // The second inline has no end: the error bubbles up with context
        assert!(found.contains(&"NO_END_NODE".to_string()));
    }

    #[test]
    fn join_requires_two_inputs_and_parallel_two_outputs() {
        let workflow = wf(json!({
            "name": "pj", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "fan", "kind": "gateway", "gateway": "parallel" },
                { "id": "meet", "kind": "gateway", "gateway": "join" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "fan" },
                { "from": "fan", "to": "meet" },
                { "from": "meet", "to": "end" }
            ]
        }));
        let found = codes(&workflow);
        assert!(found.contains(&"PARALLEL_TOO_FEW_OUTPUTS".to_string()));
        assert!(found.contains(&"JOIN_TOO_FEW_INPUTS".to_string()));
    }

    #[test]
    fn every_builtin_rule_declares_codes_from_the_catalog() {
        use crate::error::codes::ALL;
        for rule in rules::BUILTIN {
            for code in rule.codes() {
                assert!(
                    ALL.contains(code),
                    "rule declares code '{code}', which is not in error::codes::ALL"
                );
            }
        }
    }

    #[test]
    fn host_custom_rules_are_executed() {
        struct NoForbiddenNodes;
        impl ValidationRule for NoForbiddenNodes {
            fn codes(&self) -> &'static [&'static str] {
                &["HOST_FORBIDDEN_ID"]
            }
            fn check(
                &self,
                workflow: &WorkflowDefinition,
                _ctx: &ValidationCtx<'_>,
                errors: &mut Vec<WorkflowError>,
            ) {
                for node in &workflow.nodes {
                    if node.id.0 == "forbidden" {
                        errors.push(WorkflowError::new(
                            "HOST_FORBIDDEN_ID",
                            "node id forbidden by the host",
                        ));
                    }
                }
            }
        }

        let workflow = wf(json!({
            "name": "custom", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "forbidden", "kind": "task", "task": "noop" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "forbidden" },
                { "from": "forbidden", "to": "end" }
            ]
        }));
        let errors = validate_with(&workflow, &[&NoForbiddenNodes]).unwrap_err();
        assert!(errors.iter().any(|e| e.code == "HOST_FORBIDDEN_ID"));
    }
}
