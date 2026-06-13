//! Gateway coherence: branches, edge labels, and minimum arity
//! depending on the type (`exclusive`, `parallel`, `join`).

use std::collections::HashSet;

use crate::error::{WorkflowError, codes};
use crate::spec::node::NodeKind;
use crate::spec::node::gateway::GatewayKind;
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{ValidationCtx, ValidationRule};

/// Rules for the three gateway types:
/// - `exclusive`: declares branches, each branch has `when` or is `else`
///   (only one), branches ↔ outgoing edge labels are bijective and
///   **without duplicate labels**: the handler follows all edges with the
///   winning label, so a duplicate would turn the exclusive into an
///   accidental fan-out.
/// - `parallel`: no branches and at least 2 outputs.
/// - `join`: no branches and at least 2 normal-flow inputs.
pub struct GatewayCoherence;

impl ValidationRule for GatewayCoherence {
    fn codes(&self) -> &'static [&'static str] {
        &[
            codes::GATEWAY_NO_BRANCHES,
            codes::GATEWAY_MULTIPLE_ELSE,
            codes::GATEWAY_DUPLICATE_EDGE_LABEL,
            codes::GATEWAY_BRANCH_WITHOUT_WHEN,
            codes::GATEWAY_BRANCH_WITHOUT_EDGE,
            codes::GATEWAY_EDGE_WITHOUT_BRANCH,
            codes::GATEWAY_BRANCHES_IGNORED,
            codes::PARALLEL_TOO_FEW_OUTPUTS,
            codes::JOIN_TOO_FEW_INPUTS,
        ]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for node in &workflow.nodes {
            let NodeKind::Gateway(gw) = &node.kind else {
                continue;
            };
            let out = ctx.outgoing(&node.id);
            let inc = ctx.incoming(&node.id);

            match gw.gateway {
                GatewayKind::Exclusive => {
                    if gw.branches.is_empty() {
                        errors.push(
                            WorkflowError::new(
                                codes::GATEWAY_NO_BRANCHES,
                                format!(
                                    "Exclusive gateway '{}' does not declare branches",
                                    node.id
                                ),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if gw.branches.iter().filter(|b| b.is_else).count() > 1 {
                        errors.push(
                            WorkflowError::new(
                                codes::GATEWAY_MULTIPLE_ELSE,
                                format!("Gateway '{}' has more than one else branch", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    for branch in &gw.branches {
                        if !branch.is_else && branch.when.is_none() {
                            errors.push(
                                WorkflowError::new(
                                    codes::GATEWAY_BRANCH_WITHOUT_WHEN,
                                    format!(
                                        "A branch of gateway '{}' has no `when` and is not `else`",
                                        node.id
                                    ),
                                )
                                .with_source_task(node.id.to_string()),
                            );
                        }
                        if !out.iter().any(|e| e.label.as_deref() == Some(&branch.edge)) {
                            errors.push(
                                WorkflowError::new(
                                    codes::GATEWAY_BRANCH_WITHOUT_EDGE,
                                    format!(
                                        "Branch '{}' of gateway '{}' has no outgoing edge with that label",
                                        branch.edge, node.id
                                    ),
                                )
                                .with_source_task(node.id.to_string()),
                            );
                        }
                    }
                    for edge in out {
                        let label = edge.label.as_deref();
                        if !gw.branches.iter().any(|b| Some(b.edge.as_str()) == label) {
                            errors.push(
                                WorkflowError::new(
                                    codes::GATEWAY_EDGE_WITHOUT_BRANCH,
                                    format!(
                                        "Edge {}→{} (label {:?}) does not correspond to any gateway branch",
                                        edge.from, edge.to, label
                                    ),
                                )
                                .with_source_task(node.id.to_string()),
                            );
                        }
                    }
                    // Duplicate labels: the winning branch would follow all
                    // edges with that label concurrently
                    let mut seen: HashSet<&str> = HashSet::new();
                    let mut reported: HashSet<&str> = HashSet::new();
                    for edge in out {
                        if let Some(label) = edge.label.as_deref()
                            && !seen.insert(label)
                            && reported.insert(label)
                        {
                            errors.push(
                                WorkflowError::new(
                                    codes::GATEWAY_DUPLICATE_EDGE_LABEL,
                                    format!(
                                        "Exclusive gateway '{}' has more than one outgoing \
                                         edge with label '{label}'; an exclusive follows a \
                                         single edge (use a parallel gateway for fan-out)",
                                        node.id
                                    ),
                                )
                                .with_source_task(node.id.to_string()),
                            );
                        }
                    }
                    let mut seen_branches: HashSet<&str> = HashSet::new();
                    for branch in &gw.branches {
                        if !seen_branches.insert(branch.edge.as_str()) {
                            errors.push(
                                WorkflowError::new(
                                    codes::GATEWAY_DUPLICATE_EDGE_LABEL,
                                    format!(
                                        "Exclusive gateway '{}' declares more than one branch \
                                         toward label '{}'",
                                        node.id, branch.edge
                                    ),
                                )
                                .with_source_task(node.id.to_string()),
                            );
                        }
                    }
                }
                GatewayKind::Parallel => {
                    if !gw.branches.is_empty() {
                        errors.push(
                            WorkflowError::new(
                                codes::GATEWAY_BRANCHES_IGNORED,
                                format!("Parallel gateway '{}' does not accept branches", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if out.len() < 2 {
                        errors.push(
                            WorkflowError::new(
                                codes::PARALLEL_TOO_FEW_OUTPUTS,
                                format!(
                                    "Parallel gateway '{}' needs at least 2 outgoing edges",
                                    node.id
                                ),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                }
                GatewayKind::Join => {
                    if !gw.branches.is_empty() {
                        errors.push(
                            WorkflowError::new(
                                codes::GATEWAY_BRANCHES_IGNORED,
                                format!("Join gateway '{}' does not accept branches", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if inc.iter().filter(|e| e.on.is_none()).count() < 2 {
                        errors.push(
                            WorkflowError::new(
                                codes::JOIN_TOO_FEW_INPUTS,
                                format!(
                                    "Join gateway '{}' needs at least 2 incoming edges",
                                    node.id
                                ),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                }
            }
        }
    }
}
