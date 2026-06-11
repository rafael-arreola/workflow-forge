//! Nodos `gateway`: control de flujo estilo BPMN.
//!
//! - `exclusive`: evalúa sus ramas en orden y sigue solo la arista cuyo
//!   label corresponde a la primera rama cumplida (o la rama `else`).
//! - `parallel`: fan-out concurrente por todas las aristas salientes.
//! - `join`: fan-in; acumula llegadas y continúa cuando llegaron todas las
//!   ramas del flujo normal, con output `{nodo_origen: output}`.

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
    /// Ejecuta un gateway según su tipo.
    #[allow(clippy::too_many_arguments)]
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
                                "Ninguna rama del gateway '{node_id}' se cumplió y no hay rama else"
                            ),
                        )
                        .with_source_task(node_id.to_string())
                    })?;

                debug!(node_id = %node_id, branch = %winner, "Gateway exclusive resuelto");
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
                    .expect("validación exige >=2 entradas");
                let from = origin.expect("un join siempre tiene predecesor");
                let complete = {
                    let mut joins = state.joins.lock().expect("RunState lock poisoned");
                    let arrivals = joins.entry(node_id.clone()).or_default();
                    arrivals.push((from, carried));
                    if arrivals.len() == expected {
                        // Se remueve la entrada: lo que quede en el mapa al
                        // final son joins que nunca completaron
                        joins.remove(node_id)
                    } else {
                        None
                    }
                };

                match complete {
                    None => Ok(()), // esta rama termina; la última llegada continúa
                    Some(arrivals) => {
                        // Output determinista: objeto {nodo_origen: output}
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

    /// Evalúa las ramas de un gateway exclusive en orden y devuelve el label
    /// de la primera que se cumple, o la rama else.
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
