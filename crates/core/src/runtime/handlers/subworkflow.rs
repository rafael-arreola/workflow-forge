//! `subworkflow` nodes: execution of another workflow as if it were a task.

use std::sync::Arc;

use serde_json::Value;
use tracing::debug;

use crate::error::WorkflowError;
use crate::expr::mapping;
use crate::runtime::context::WorkflowContext;
use crate::runtime::executor::WorkflowExecutor;
use crate::spec::node::{Node, SubworkflowNode};
use crate::task::WorkflowData;

impl WorkflowExecutor {
    /// Executes a subworkflow node: resolves its input, delivers it as the
    /// child's trigger, and returns the child's final output. The child's
    /// error (including `TASK_PANIC`) bubbles up as-is: `after_task_result`
    /// routes it through the node's `on: error` / `on: panic` edges.
    pub(crate) async fn run_subworkflow(
        &self,
        node: &Node,
        sub: &SubworkflowNode,
        carried: Arc<Value>,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let child = self
            .subworkflows
            .get(&node.id)
            .expect("resolved when building the executor");

        let input = match &sub.input {
            Some(mapping_def) => ctx
                .with_state(|s| mapping::resolve(mapping_def, s))
                .map_err(|e| e.with_source_task(node.id.to_string()))?,
            None => Arc::unwrap_or_clone(carried),
        };

        let child_ctx = WorkflowContext::child_of(&child.workflow, input.clone(), ctx);
        debug!(
            node_id = %node.id,
            child = %child.workflow.name,
            child_execution_id = %child_ctx.execution_id(),
            "Executing sub-workflow"
        );
        child
            .run_with_ctx(WorkflowData(input), &child_ctx)
            .await
            .map(|data| data.0)
            .map_err(|mut err| {
                if err.source_task.is_none() {
                    err.source_task = Some(node.id.to_string());
                }
                err
            })
    }
}
