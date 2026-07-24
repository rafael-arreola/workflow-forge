//! The **observe** family: execution observability.
//!
//! The executor emits typed, serializable events to an
//! [`ExecutionObserver`] registered via `WorkflowExecutor::with_observer`.
//! The host decides what to do with them (memory, database, OTLP, ...).
//!
//! Events are the spec's observability contract: they serialize to stable
//! JSON (`type` discriminator in snake_case) and are designed to eventually
//! become the journal of a durable executor (event sourcing) without a
//! redesign.
//!
//! [`InMemoryHistory`] is the built-in observer: it accumulates the events
//! of an execution and produces an [`ExecutionReport`] — the embedded
//! answer to "what happened with execution X?".

pub mod adapters;
pub mod history;

pub use adapters::{JsonlObserver, TracingObserver};
pub use history::{ExecutionReport, ExecutionStatus, InMemoryHistory, NodeReport, NodeStatus};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::WorkflowError;

/// Receiver of execution events.
///
/// `on_event` is invoked synchronously by the executor and MUST NOT block:
/// a host with slow persistence should buffer (channel/spawn).
pub trait ExecutionObserver: Send + Sync {
    /// Receives each event emitted by the executor, in emission order.
    fn on_event(&self, event: &ExecutionEvent);
}

/// Execution event: common metadata + specific variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvent {
    /// Id of the execution that emitted the event
    pub execution_id: String,
    /// Parent execution id if the event comes from a sub-workflow
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<String>,
    /// Total emission order within the execution (parallel branches emit
    /// concurrently; `seq` orders them stably). Sub-workflows share the
    /// parent's counter: `seq` orders the full execution tree
    pub seq: u64,
    /// Milliseconds elapsed since the execution started
    pub elapsed_ms: u64,
    /// Specific event variant (`type` in JSON)
    #[serde(flatten)]
    pub kind: EventKind,
}

/// Event variants. The `type` field discriminates in JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(missing_docs)] // fields repeat node_id/output/error; the variant documents itself
pub enum EventKind {
    /// The execution started (root or sub-workflow)
    WorkflowStarted { workflow: Value, trigger: Value },
    /// A node began executing (joins emit this when they complete)
    NodeStarted { node_id: String, kind: String },
    /// A task attempt started (task nodes; foreach does not emit attempts)
    TaskAttemptStarted {
        node_id: String,
        attempt: u32,
        /// Resolved task input; only on the first attempt (retries receive
        /// the same input)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
    },
    /// A task attempt failed; `will_retry` indicates whether another will
    /// follow
    TaskAttemptFailed {
        node_id: String,
        attempt: u32,
        error: WorkflowError,
        will_retry: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_delay_ms: Option<u64>,
    },
    /// A node completed successfully and published its output
    NodeCompleted {
        node_id: String,
        output: Value,
        duration_ms: u64,
    },
    /// A node failed definitively (retries exhausted)
    NodeFailed {
        node_id: String,
        error: WorkflowError,
        /// `true` if the flow continued through an `on: error` edge
        error_routed: bool,
    },
    /// A foreach element completed successfully
    ForeachItemCompleted {
        node_id: String,
        index: usize,
        output: Value,
    },
    /// A foreach element failed definitively
    ForeachItemFailed {
        node_id: String,
        index: usize,
        error: WorkflowError,
    },
    /// A loop iteration completed successfully
    LoopIterationCompleted {
        node_id: String,
        index: usize,
        output: Value,
    },
    /// A loop iteration failed definitively (retries exhausted);
    /// the whole loop node fails with this error
    LoopIterationFailed {
        node_id: String,
        index: usize,
        error: WorkflowError,
    },
    /// The execution completed with final output
    WorkflowCompleted { output: Value, duration_ms: u64 },
    /// The execution failed
    WorkflowFailed {
        error: WorkflowError,
        duration_ms: u64,
    },
}

impl EventKind {
    /// Id of the node the event refers to, if applicable
    pub fn node_id(&self) -> Option<&str> {
        match self {
            EventKind::NodeStarted { node_id, .. }
            | EventKind::TaskAttemptStarted { node_id, .. }
            | EventKind::TaskAttemptFailed { node_id, .. }
            | EventKind::NodeCompleted { node_id, .. }
            | EventKind::NodeFailed { node_id, .. }
            | EventKind::ForeachItemCompleted { node_id, .. }
            | EventKind::ForeachItemFailed { node_id, .. }
            | EventKind::LoopIterationCompleted { node_id, .. }
            | EventKind::LoopIterationFailed { node_id, .. } => Some(node_id),
            _ => None,
        }
    }
}

/// Redacts sensitive values in JSON payloads according to a [`crate::spec::node::SecureConfig`].
pub fn redact_value(value: &Value, config: Option<&crate::spec::node::SecureConfig>) -> Value {
    use crate::spec::node::SecureConfig;

    match config {
        Some(SecureConfig::Full(true)) => Value::String("[REDACTED]".to_string()),
        Some(SecureConfig::Fields(fields)) => {
            let mut val = value.clone();
            if let Value::Object(ref mut map) = val {
                for f in fields {
                    if map.contains_key(f) {
                        map.insert(f.clone(), Value::String("[REDACTED]".to_string()));
                    }
                }
            }
            val
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::node::SecureConfig;
    use serde_json::json;

    #[test]
    fn test_redact_value_full_and_fields() {
        let val = json!({ "user": "alice", "token": "secret123" });

        let redacted_full = redact_value(&val, Some(&SecureConfig::Full(true)));
        assert_eq!(redacted_full, json!("[REDACTED]"));

        let redacted_fields = redact_value(
            &val,
            Some(&SecureConfig::Fields(vec!["token".to_string()])),
        );
        assert_eq!(
            redacted_fields,
            json!({ "user": "alice", "token": "[REDACTED]" })
        );
    }
}

