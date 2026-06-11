//! Nodos `start` y `end`: la frontera de entrada/salida del workflow.

use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;

use crate::error::{WorkflowError, codes};
use crate::expr::mapping;
use crate::observe::EventKind;
use crate::runtime::context::WorkflowContext;
use crate::runtime::executor::{RunState, WorkflowExecutor};
use crate::runtime::schemas::validate_compiled;
use crate::spec::node::NodeId;
use crate::spec::node::event::{EndNode, StartNode};

impl WorkflowExecutor {
    /// Ejecuta el nodo `start`: valida el trigger contra el schema (si hay),
    /// fusiona los `defaults` y continúa por las aristas salientes.
    pub(crate) async fn run_start(
        &self,
        node_id: &NodeId,
        start_node: &StartNode,
        carried: Arc<Value>,
        node_started: Instant,
        ctx: &WorkflowContext,
        state: &RunState,
    ) -> Result<(), WorkflowError> {
        if let Some(validator) = self.schemas.nodes.get(node_id) {
            validate_compiled(validator, &carried).map_err(|e| {
                WorkflowError::new(
                    codes::SCHEMA_VALIDATION_FAILED,
                    format!("El trigger no cumple el schema del start: {e}"),
                )
                .with_source_task(node_id.to_string())
            })?;
        }
        let mut output = Arc::unwrap_or_clone(carried);
        if let Some(defaults) = &start_node.defaults {
            let mut map = match output {
                Value::Object(m) => m,
                other => {
                    let mut m = serde_json::Map::new();
                    m.insert("_input".to_string(), other);
                    m
                }
            };
            for (key, value) in defaults {
                map.entry(key.clone()).or_insert_with(|| value.0.clone());
            }
            output = Value::Object(map);
        }
        ctx.set_node_output(node_id, output.clone());
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

    /// Ejecuta un nodo `end`: resuelve el mapping de output (o usa el token
    /// del predecesor), valida contra el schema y registra el resultado
    /// terminal. La rama se detiene aquí.
    pub(crate) async fn run_end(
        &self,
        node_id: &NodeId,
        end_node: &EndNode,
        carried: Arc<Value>,
        node_started: Instant,
        ctx: &WorkflowContext,
        state: &RunState,
    ) -> Result<(), WorkflowError> {
        let result = match &end_node.output {
            Some(mapping_def) => ctx
                .with_state(|s| mapping::resolve(mapping_def, s))
                .map_err(|e| e.with_source_task(node_id.to_string()))?,
            None => Arc::unwrap_or_clone(carried),
        };
        if let Some(validator) = self.schemas.nodes.get(node_id) {
            validate_compiled(validator, &result).map_err(|e| {
                WorkflowError::new(
                    codes::OUTPUT_SCHEMA_VALIDATION_FAILED,
                    format!("El resultado no cumple el schema del end: {e}"),
                )
                .with_source_task(node_id.to_string())
            })?;
        }
        ctx.set_node_output(node_id, result.clone());
        self.emit(
            ctx,
            EventKind::NodeCompleted {
                node_id: node_id.0.clone(),
                output: result.clone(),
                duration_ms: node_started.elapsed().as_millis() as u64,
            },
        );
        state
            .ends
            .lock()
            .expect("RunState lock poisoned")
            .push((node_id.clone(), result));
        Ok(())
    }
}
