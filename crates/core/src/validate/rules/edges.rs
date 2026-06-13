//! Rules for edges with a trigger (`on: error` / `on: panic`).

use crate::error::{WorkflowError, codes};
use crate::spec::node::NodeKind;
use crate::spec::node::gateway::GatewayKind;
use crate::spec::workflow::{EdgeTrigger, WorkflowDefinition};
use crate::validate::{ValidationCtx, ValidationRule};

/// An edge with `on:` may only originate from nodes that execute tasks
/// (task, foreach, loop, subworkflow) and may never target a gateway join
/// (the join's arrival count only considers the normal flow).
pub struct EdgeTriggers;

impl ValidationRule for EdgeTriggers {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::ERROR_EDGE_INVALID_SOURCE, codes::ERROR_EDGE_TO_JOIN]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for node in &workflow.nodes {
            for edge in ctx.outgoing(&node.id) {
                if edge.on.is_some()
                    && !matches!(
                        node.kind,
                        NodeKind::Task(_)
                            | NodeKind::Foreach(_)
                            | NodeKind::Loop(_)
                            | NodeKind::Subworkflow(_)
                    )
                {
                    errors.push(
                        WorkflowError::new(
                            codes::ERROR_EDGE_INVALID_SOURCE,
                            format!(
                                "Edge {}→{} with `on: {}` must originate from a task, foreach, loop, or subworkflow node",
                                edge.from,
                                edge.to,
                                trigger_name(edge.on)
                            ),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
            }

            for edge in ctx.incoming(&node.id) {
                if edge.on.is_some()
                    && matches!(
                        &node.kind,
                        NodeKind::Gateway(gw) if gw.gateway == GatewayKind::Join
                    )
                {
                    errors.push(
                        WorkflowError::new(
                            codes::ERROR_EDGE_TO_JOIN,
                            format!(
                                "Edge {}→{} with `on: {}` cannot target a gateway join",
                                edge.from,
                                edge.to,
                                trigger_name(edge.on)
                            ),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
            }
        }
    }
}

fn trigger_name(on: Option<EdgeTrigger>) -> &'static str {
    match on {
        Some(EdgeTrigger::Error) => "error",
        Some(EdgeTrigger::Panic) => "panic",
        None => "",
    }
}
