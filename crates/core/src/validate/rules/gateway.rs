//! Coherencia de gateways: ramas, labels de aristas y aridad mínima
//! según el tipo (`exclusive`, `parallel`, `join`).

use crate::error::{WorkflowError, codes};
use crate::spec::node::NodeKind;
use crate::spec::node::gateway::GatewayKind;
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{ValidationCtx, ValidationRule};

/// Reglas de los tres tipos de gateway:
/// - `exclusive`: declara branches, cada rama tiene `when` o es `else`
///   (solo una), y branches ↔ labels de aristas salientes son biyectivos.
/// - `parallel`: sin branches y al menos 2 salidas.
/// - `join`: sin branches y al menos 2 entradas del flujo normal.
pub struct GatewayCoherence;

impl ValidationRule for GatewayCoherence {
    fn codes(&self) -> &'static [&'static str] {
        &[
            codes::GATEWAY_NO_BRANCHES,
            codes::GATEWAY_MULTIPLE_ELSE,
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
                                format!("El gateway exclusive '{}' no declara branches", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if gw.branches.iter().filter(|b| b.is_else).count() > 1 {
                        errors.push(
                            WorkflowError::new(
                                codes::GATEWAY_MULTIPLE_ELSE,
                                format!("El gateway '{}' tiene más de una rama else", node.id),
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
                                        "Una rama del gateway '{}' no tiene `when` ni es `else`",
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
                                        "La rama '{}' del gateway '{}' no tiene arista saliente con ese label",
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
                                        "La arista {}→{} (label {:?}) no corresponde a ninguna rama del gateway",
                                        edge.from, edge.to, label
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
                                format!("El gateway parallel '{}' no acepta branches", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if out.len() < 2 {
                        errors.push(
                            WorkflowError::new(
                                codes::PARALLEL_TOO_FEW_OUTPUTS,
                                format!(
                                    "El gateway parallel '{}' necesita al menos 2 aristas salientes",
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
                                format!("El gateway join '{}' no acepta branches", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if inc.iter().filter(|e| e.on.is_none()).count() < 2 {
                        errors.push(
                            WorkflowError::new(
                                codes::JOIN_TOO_FEW_INPUTS,
                                format!(
                                    "El gateway join '{}' necesita al menos 2 aristas entrantes",
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
