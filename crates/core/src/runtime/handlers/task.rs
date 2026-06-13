//! `task` nodes: invocation of a registered task with mapped input.

use std::sync::Arc;

use serde_json::Value;

use crate::error::WorkflowError;
use crate::expr::mapping;
use crate::runtime::context::WorkflowContext;
use crate::runtime::executor::WorkflowExecutor;
use crate::runtime::policy::ExecPolicy;
use crate::spec::node::Node;
use crate::spec::node::task::TaskNode;

impl WorkflowExecutor {
    /// Executes a task node: resolves its input, applies timeout and retries.
    /// The result is finalized by `after_task_result` (error routing included).
    pub(crate) async fn run_task(
        &self,
        node: &Node,
        task_node: &TaskNode,
        carried: Arc<Value>,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let task = self
            .registry
            .get(&task_node.task)
            .expect("validate_tasks guarantees registration");

        let input = match &task_node.input {
            Some(mapping_def) => ctx
                .with_state(|s| mapping::resolve(mapping_def, s))
                .map_err(|e| e.with_source_task(node.id.to_string()))?,
            None => Arc::unwrap_or_clone(carried),
        };

        self.execute_with_policy(
            &task,
            &node.id,
            input,
            ExecPolicy {
                retry: task_node.retry.as_ref(),
                timeout_ms: task_node.timeout_ms,
                emit_attempts: true,
            },
            ctx,
        )
        .await
    }
}
