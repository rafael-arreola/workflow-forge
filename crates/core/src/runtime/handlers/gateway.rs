//! `gateway` nodes: BPMN-style flow control.
//!
//! - `exclusive`: evaluates its branches in order and follows only the edge
//!   whose label corresponds to the first matching branch (or the `else`
//!   branch).
//! - `parallel`: concurrent fan-out through all outgoing edges.
//! - `join`: fan-in; accumulates arrivals and continues when all normal-flow
//!   branches have arrived, with output `{origin_node: output}`.

use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use tracing::debug;

use crate::error::{WorkflowError, codes};
use crate::observe::EventKind;
use crate::runtime::context::WorkflowContext;
use crate::runtime::executor::{RunState, WorkflowExecutor};
use crate::spec::node::gateway::{GatewayKind, GatewayNode};
use crate::spec::node::{Node, NodeId};
use crate::spec::workflow::FlowEdge;

impl WorkflowExecutor {
    /// Executes a gateway according to its type.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, node, gateway, carried, ctx, state))]
    pub(crate) async fn run_gateway(
        &self,
        node: &Node,
        gateway: &GatewayNode,
        carried: Arc<Value>,
        origin: Option<NodeId>,
        node_started: Instant,
        ctx: &WorkflowContext,
        state: &RunState,
    ) -> Result<(), WorkflowError> {
        let node_id = &node.id;
        match gateway.gateway {
            GatewayKind::Exclusive => {
                let winner = self
                    .pick_branch(gateway, ctx)
                    .map_err(|e| e.with_source_task(node_id.to_string()))?
                    .ok_or_else(|| {
                        WorkflowError::new(
                            codes::NO_BRANCH_MATCHED,
                            format!(
                                "No branch of gateway '{node_id}' matched and there is no else branch"
                            ),
                        )
                        .with_source_task(node_id.to_string())
                    })?;

                debug!(node_id = %node_id, branch = %winner, "Exclusive gateway resolved");
                ctx.set_node_output(node_id, (*carried).clone());
                self.emit(
                    ctx,
                    EventKind::NodeCompleted {
                        node_id: node_id.0.clone(),
                        output: (*carried).clone(),
                        duration_ms: node_started.elapsed().as_millis() as u64,
                    },
                );
                let edges: Vec<FlowEdge> = self
                    .index
                    .outgoing_edges(node_id)
                    .iter()
                    .filter(|e| e.label.as_deref() == Some(winner.as_str()))
                    .cloned()
                    .collect();
                self.continue_through(&edges, carried, ctx, state).await
            }

            GatewayKind::Parallel => {
                ctx.set_node_output(node_id, (*carried).clone());
                self.emit(
                    ctx,
                    EventKind::NodeCompleted {
                        node_id: node_id.0.clone(),
                        output: (*carried).clone(),
                        duration_ms: node_started.elapsed().as_millis() as u64,
                    },
                );
                self.continue_through(self.index.outgoing_edges(node_id), carried, ctx, state)
                    .await
            }

            GatewayKind::Join => {
                let expected = *self
                    .index
                    .incoming_count
                    .get(node_id)
                    .expect("validation requires >= 2 inputs");
                let from = origin.expect("a join always has a predecessor");
                let complete = {
                    let mut joins = state.joins.lock();
                    let arrivals = joins.entry(node_id.clone()).or_default();
                    arrivals.push((from, carried));
                    if arrivals.len() == expected {
                        // Remove the entry: whatever remains in the map at the
                        // end are joins that never completed
                        joins.remove(node_id)
                    } else {
                        None
                    }
                };

                match complete {
                    None => Ok(()), // this branch ends; the last arrival continues
                    Some(arrivals) => {
                        // Deterministic output: object {origin_node: output}
                        let output = Value::Object(
                            arrivals
                                .into_iter()
                                .map(|(id, v)| (id.0, Arc::unwrap_or_clone(v)))
                                .collect(),
                        );
                        ctx.set_node_output(node_id, output.clone());
                        self.emit(
                            ctx,
                            EventKind::NodeStarted {
                                node_id: node_id.0.clone(),
                                kind: "gateway".to_string(),
                            },
                        );
                        self.emit(
                            ctx,
                            EventKind::NodeCompleted {
                                node_id: node_id.0.clone(),
                                output: output.clone(),
                                duration_ms: node_started.elapsed().as_millis() as u64,
                            },
                        );
                        self.continue_through(
                            self.index.outgoing_edges(node_id),
                            Arc::new(output),
                            ctx,
                            state,
                        )
                        .await
                    }
                }
            }
        }
    }

    /// Evaluates the branches of an exclusive gateway in order and returns the
    /// label of the first matching branch, or the else branch.
    fn pick_branch(
        &self,
        gateway: &GatewayNode,
        ctx: &WorkflowContext,
    ) -> Result<Option<String>, WorkflowError> {
        ctx.with_state(|context| {
            for branch in &gateway.branches {
                if let Some(when) = &branch.when
                    && when.evaluate(context)?
                {
                    return Ok(Some(branch.edge.clone()));
                }
            }
            Ok(gateway
                .branches
                .iter()
                .find(|b| b.is_else)
                .map(|b| b.edge.clone()))
        })
    }
}
