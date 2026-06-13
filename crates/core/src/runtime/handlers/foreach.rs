//! `foreach` nodes: iteration over an array invoking a task per element,
//! with bounded concurrency, throttle between launches, and per-element
//! error policy (`fail` cuts short, `collect` separates successes from
//! failures).

use std::sync::Arc;

use parking_lot::Mutex;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::stream::{self, StreamExt};
use serde_json::{Value, json};

use crate::error::{WorkflowError, codes};
use crate::expr::mapping;
use crate::observe::EventKind;
use crate::runtime::context::WorkflowContext;
use crate::runtime::executor::WorkflowExecutor;
use crate::runtime::policy::ExecPolicy;
use crate::spec::node::Node;
use crate::spec::node::foreach::{ForeachNode, OnItemError};
use crate::task::Task;

impl WorkflowExecutor {
    /// Executes a foreach node: resolves `items` and executes the task for each
    /// element with the configured concurrency/throttle.
    pub(crate) async fn run_foreach(
        &self,
        node: &Node,
        foreach: &ForeachNode,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let task = self
            .registry
            .get(&foreach.task)
            .expect("validate_tasks guarantees registration");

        let items_value = ctx
            .with_state(|s| mapping::resolve(&foreach.items, s))
            .map_err(|e| e.with_source_task(node.id.to_string()))?;
        let Value::Array(items) = items_value else {
            return Err(WorkflowError::new(
                codes::FOREACH_ITEMS_NOT_ARRAY,
                format!(
                    "The `items` mapping of foreach '{}' did not resolve to an array",
                    node.id
                ),
            )
            .with_source_task(node.id.to_string()));
        };

        // Throttle: separates element LAUNCHES by at least `throttle_ms`
        // from each other, even under concurrency (rate limit toward the
        // target)
        let gate = Arc::new(Mutex::new(Instant::now()));

        let futures: Vec<_> = items
            .into_iter()
            .enumerate()
            .map(|(index, item)| {
                self.foreach_item(
                    node,
                    foreach,
                    Arc::clone(&task),
                    Arc::clone(&gate),
                    index,
                    item,
                    ctx,
                )
            })
            .collect();
        let mut buffered = stream::iter(futures).buffered(foreach.concurrency);

        match foreach.on_item_error {
            // Fast fail: the first error cuts the stream and cancels in-flight
            // elements (drop of the buffered stream)
            OnItemError::Fail => {
                let mut outputs = Vec::new();
                while let Some((_, _, result)) = buffered.next().await {
                    outputs.push(result?);
                }
                Ok(Value::Array(outputs))
            }
            // Runs everything and separates successes from failures; the node
            // does not fail... unless an element panics: that aborts the whole
            // node
            OnItemError::Collect => {
                let mut ok = Vec::new();
                let mut failed = Vec::new();
                while let Some((index, item, result)) = buffered.next().await {
                    match result {
                        Ok(output) => ok.push(output),
                        Err(error) if error.code == codes::TASK_PANIC => return Err(error),
                        Err(error) => failed.push(json!({
                            "index": index,
                            "item": item,
                            "error": serde_json::to_value(&error).unwrap_or(Value::Null),
                        })),
                    }
                }
                Ok(json!({ "ok": ok, "failed": failed }))
            }
        }
    }

    /// Executes ONE element of a foreach: respects the throttle, applies the
    /// retry/timeout policy, and emits element events.
    /// Returns a boxed future with explicit lifetimes (rustc cannot prove
    /// `Send` for async closures inside the recursive executor).
    #[allow(clippy::too_many_arguments)]
    fn foreach_item<'a>(
        &'a self,
        node: &'a Node,
        foreach: &'a ForeachNode,
        task: Arc<dyn Task>,
        gate: Arc<Mutex<Instant>>,
        index: usize,
        item: Value,
        ctx: &'a WorkflowContext,
    ) -> BoxFuture<'a, (usize, Value, Result<Value, WorkflowError>)> {
        Box::pin(self.foreach_item_inner(node, foreach, task, gate, index, item, ctx))
    }

    #[allow(clippy::too_many_arguments)]
    async fn foreach_item_inner(
        &self,
        node: &Node,
        foreach: &ForeachNode,
        task: Arc<dyn Task>,
        gate: Arc<Mutex<Instant>>,
        index: usize,
        item: Value,
        ctx: &WorkflowContext,
    ) -> (usize, Value, Result<Value, WorkflowError>) {
        let throttle = Duration::from_millis(foreach.throttle_ms);
        if !throttle.is_zero() {
            let wait = {
                let mut next = gate.lock();
                let now = Instant::now();
                let start_at = (*next).max(now);
                *next = start_at + throttle;
                start_at - now
            };
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
        }

        let result = self
            .execute_with_policy(
                &task,
                &node.id,
                item.clone(),
                ExecPolicy {
                    retry: foreach.retry.as_ref(),
                    timeout_ms: foreach.timeout_ms,
                    emit_attempts: false,
                },
                ctx,
            )
            .await;
        match &result {
            Ok(output) => self.emit(
                ctx,
                EventKind::ForeachItemCompleted {
                    node_id: node.id.0.clone(),
                    index,
                    output: output.clone(),
                },
            ),
            Err(error) => self.emit(
                ctx,
                EventKind::ForeachItemFailed {
                    node_id: node.id.0.clone(),
                    index,
                    error: error.clone(),
                },
            ),
        }
        (index, item, result)
    }
}
