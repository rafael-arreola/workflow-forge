//! Reglas de aristas con disparador (`on: error` / `on: panic`).

use crate::error::{WorkflowError, codes};
use crate::spec::node::NodeKind;
use crate::spec::node::gateway::GatewayKind;
use crate::spec::workflow::{EdgeTrigger, WorkflowDefinition};
use crate::validate::{ValidationCtx, ValidationRule};

/// Una arista con `on:` solo puede salir de nodos que ejecutan tareas
/// (task, foreach, subworkflow) y nunca puede entrar a un gateway join
/// (el conteo de llegadas del join solo considera el flujo normal).
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
                        NodeKind::Task(_) | NodeKind::Foreach(_) | NodeKind::Subworkflow(_)
                    )
                {
                    errors.push(
                        WorkflowError::new(
                            codes::ERROR_EDGE_INVALID_SOURCE,
                            format!(
                                "La arista {}→{} con `on: {}` debe originarse en un nodo task, foreach o subworkflow",
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
                                "La arista {}→{} con `on: {}` no puede apuntar a un gateway join",
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
