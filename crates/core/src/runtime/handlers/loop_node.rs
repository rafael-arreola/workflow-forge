//! `loop` nodes: bounded iteration of a task with a continuation condition.
//! The canonical use case is pagination: "fetch the next page until `next`
//! is null".

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
    /// Executes a loop node. The first iteration runs with `input` (or the
    /// predecessor's token); after each iteration `while` is evaluated against
    /// the document `{ input, output, index }` and, if it continues, the
    /// `next` shape builds the next input against that same document.
    /// The result is finalized by `after_task_result` (error routing included).
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
            .expect("validate_tasks guarantees registration");

        let mut current_input = match &lp.input {
            Some(mapping_def) => ctx
                .with_state(|s| mapping::resolve(mapping_def, s))
                .map_err(|e| e.with_source_task(node.id.to_string()))?,
            None => Arc::unwrap_or_clone(carried),
        };

        // Only filled when `collect: all`
        let mut collected: Vec<Value> = Vec::new();
        let mut index: u32 = 0;

        loop {
            // The input is cloned because the iteration document (for
            // `while`/`next`) needs it after invoking the task
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

            // Iteration document: local root for `while` ($.) and `next` (@.)
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
                        "Loop '{}' reached max_iterations ({}) with its `while` \
                         condition still true; raise the limit or use on_max: \"stop\"",
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

            // Prepare the next iteration (the shape reads the full document,
            // so it is resolved before dismantling it)
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
