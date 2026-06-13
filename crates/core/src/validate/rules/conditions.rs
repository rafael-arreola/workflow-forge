//! Condition vocabulary rule: every custom operator used in the document
//! (gateway branches, loop `while`) must be registered before building the
//! executor.

use std::sync::Arc;

use crate::error::{WorkflowError, codes};
use crate::expr::operators::{self, OperatorRegistry};
use crate::spec::condition::{CompareOp, Condition};
use crate::spec::node::{NodeId, NodeKind};
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{ValidationCtx, ValidationRule};

/// Operators outside the spec ([`CompareOp::Custom`]) must exist in the
/// operator registry. If a registry is provided via [`KnownConditionOperators::with_registry`],
/// it is used; otherwise falls back to the global registry.
pub struct KnownConditionOperators {
    pub(crate) registry: Option<Arc<OperatorRegistry>>,
}

impl KnownConditionOperators {
    /// Creates a rule that checks against the global operator registry.
    pub fn new() -> Self {
        Self { registry: None }
    }

    /// Creates a rule that checks against the given operator registry.
    pub fn with_registry(registry: Arc<OperatorRegistry>) -> Self {
        Self {
            registry: Some(registry),
        }
    }

    fn is_registered(&self, key: &str) -> bool {
        #[allow(deprecated)]
        {
            self.registry
                .as_ref()
                .map(|r| r.contains(key))
                .unwrap_or_else(|| operators::global().contains(key))
        }
    }
}

impl Default for KnownConditionOperators {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationRule for KnownConditionOperators {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::UNKNOWN_CONDITION_OPERATOR]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        _ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for node in &workflow.nodes {
            match &node.kind {
                NodeKind::Gateway(gw) => {
                    for branch in &gw.branches {
                        let Some(when) = &branch.when else { continue };
                        check_condition(
                            when,
                            &node.id,
                            &format!("Branch '{}' of gateway", branch.edge),
                            errors,
                            self,
                        );
                    }
                }
                NodeKind::Loop(lp) => {
                    check_condition(&lp.while_, &node.id, "The loop `while`", errors, self);
                }
                _ => {}
            }
        }
    }
}

/// Reports each unregistered custom operator within a condition.
fn check_condition(
    condition: &Condition,
    node_id: &NodeId,
    where_: &str,
    errors: &mut Vec<WorkflowError>,
    registry: &KnownConditionOperators,
) {
    walk(condition, &mut |op| {
        if let CompareOp::Custom { key, .. } = op
            && !registry.is_registered(key)
        {
            errors.push(
                WorkflowError::new(
                    codes::UNKNOWN_CONDITION_OPERATOR,
                    format!(
                        "{where_} '{node_id}' uses operator '{key}', which is not part of \
                         spec 1.0 nor registered in the operator registry"
                    ),
                )
                .with_source_task(node_id.to_string()),
            );
        }
    });
}

/// Traverses all comparisons in a condition tree.
fn walk(cond: &Condition, visit: &mut impl FnMut(&CompareOp)) {
    match cond {
        Condition::And { and } => and.iter().for_each(|c| walk(c, visit)),
        Condition::Or { or } => or.iter().for_each(|c| walk(c, visit)),
        Condition::Not { not } => walk(not, visit),
        Condition::Compare(cmp) => visit(&cmp.op),
    }
}

#[cfg(test)]
mod tests {
    use crate::validate::validate;
    use serde_json::json;

    #[test]
    fn unknown_operator_detected_during_validation() {
        let workflow = serde_json::from_value(json!({
            "name": "ops", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "check", "kind": "gateway", "gateway": "exclusive", "branches": [
                    { "when": { "and": [
                        { "path": "$.trigger.x", "eq": 1 },
                        { "path": "$.trigger.y", "nonexistent_operator_xyz": 2 }
                    ]}, "edge": "ok" },
                    { "else": true, "edge": "fail" }
                ]},
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "check" },
                { "from": "check", "to": "end", "label": "ok" },
                { "from": "check", "to": "end", "label": "fail" }
            ]
        }))
        .unwrap();
        let errors = validate(&workflow).unwrap_err();
        assert!(errors.iter().any(|e| e.code == "UNKNOWN_CONDITION_OPERATOR"
            && e.message.contains("nonexistent_operator_xyz")));
    }
}
