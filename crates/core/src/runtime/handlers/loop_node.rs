//! Nodos `loop`: iteración acotada de una tarea con condición de
//! continuación. El caso canónico es la paginación: "pide la siguiente
//! página hasta que `next` sea null".

use std::sync::Arc;

use serde_json::{Value, json};

use crate::error::{WorkflowError, codes};
use crate::expr::{mapping, shape};
use crate::observe::EventKind;
use crate::runtime::context::WorkflowContext;
use crate::runtime::executor::WorkflowExecutor;
use crate::runtime::policy::ExecPolicy;
use crate::spec::node::Node;
use crate::spec::node::loop_node::{Collect, LoopNode, OnMax};

impl WorkflowExecutor {
    /// Ejecuta un nodo loop. La primera iteración corre con `input` (o el
    /// token del predecesor); después de cada iteración se evalúa `while`
    /// contra el documento `{ input, output, index }` y, si continúa, el
    /// shape `next` construye el input siguiente contra ese mismo documento.
    /// El resultado lo cierra `after_task_result` (ruteo de error incluido).
    pub(crate) async fn run_loop(
        &self,
        node: &Node,
        lp: &LoopNode,
        carried: Arc<Value>,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let task = self
            .registry
            .get(&lp.task)
            .expect("validate_tasks garantiza el registro");

        let mut current_input = match &lp.input {
            Some(mapping_def) => ctx
                .with_state(|s| mapping::resolve(mapping_def, s))
                .map_err(|e| e.with_source_task(node.id.to_string()))?,
            None => Arc::unwrap_or_clone(carried),
        };

        // Solo se llena con `collect: all`
        let mut collected: Vec<Value> = Vec::new();
        let mut index: u32 = 0;

        loop {
            // El input se clona porque el documento de iteración (para
            // `while`/`next`) lo necesita después de invocar la tarea
            let result = self
                .execute_with_policy(
                    &task,
                    &node.id,
                    current_input.clone(),
                    ExecPolicy {
                        retry: lp.retry.as_ref(),
                        timeout_ms: lp.timeout_ms,
                        emit_attempts: false,
                    },
                    ctx,
                )
                .await;
            let output = match result {
                Ok(output) => {
                    self.emit(
                        ctx,
                        EventKind::LoopIterationCompleted {
                            node_id: node.id.0.clone(),
                            index: index as usize,
                            output: output.clone(),
                        },
                    );
                    output
                }
                Err(error) => {
                    self.emit(
                        ctx,
                        EventKind::LoopIterationFailed {
                            node_id: node.id.0.clone(),
                            index: index as usize,
                            error: error.clone(),
                        },
                    );
                    return Err(error);
                }
            };

            // Documento de iteración: raíz local de `while` ($.) y `next` (@.)
            let mut doc = json!({
                "input": current_input,
                "output": output,
                "index": index,
            });

            let keep_going = lp
                .while_
                .evaluate(&doc)
                .map_err(|e| e.with_source_task(node.id.to_string()))?;

            let reached_cap = index + 1 >= lp.max_iterations;
            if keep_going && reached_cap && lp.on_max == OnMax::Fail {
                return Err(WorkflowError::new(
                    codes::LOOP_MAX_ITERATIONS_EXCEEDED,
                    format!(
                        "El loop '{}' alcanzó max_iterations ({}) con su condición \
                         `while` aún verdadera; sube el tope o usa on_max: \"stop\"",
                        node.id, lp.max_iterations
                    ),
                )
                .with_source_task(node.id.to_string()));
            }

            if !keep_going || reached_cap {
                let output = doc["output"].take();
                return Ok(match lp.collect {
                    Collect::Last => output,
                    Collect::All => {
                        collected.push(output);
                        Value::Array(collected)
                    }
                });
            }

            // Preparar la siguiente iteración (el shape lee el doc completo,
            // así que se resuelve antes de desarmarlo)
            let next_input = match &lp.next {
                Some(shape_def) => shape::apply_shape(shape_def, &doc)
                    .map_err(|e| e.with_source_task(node.id.to_string()))?,
                None => doc["output"].clone(),
            };
            if lp.collect == Collect::All {
                collected.push(doc["output"].take());
            }
            current_input = next_input;
            index += 1;
        }
    }
}
