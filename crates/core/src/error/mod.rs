//! The **error** family: the engine's structured error and its catalog.
//!
//! [`WorkflowError`] is the crate's only error type: it travels serialized
//! between nodes (`on: error` routes), in observability events, and as
//! a validation result. The [`codes`] module is the full catalog of
//! codes the engine can emit.

pub mod codes;

use serde::{Deserialize, Serialize};

use crate::task::WorkflowData;

/// Structured error that a task can return during execution.
/// Includes traceability to the originating task and supports chaining.
#[derive(Debug, Serialize, Deserialize, thiserror::Error)]
pub struct WorkflowError {
    /// Unique code identifying the error type (see [`codes`])
    pub code: String,
    /// Descriptive message for the operator or developer
    pub message: String,
    /// Identifier of the task that originated the error
    #[serde(default)]
    pub source_task: Option<String>,
    /// Data that originated the error (boxed to keep the error cheap to move)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Box<WorkflowData>>,
    /// Partial data generated before the failure
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Box<WorkflowData>>,
    /// Hint for the retry policy: wait **at least** these
    /// milliseconds before the next attempt. Set by the task when the
    /// destination indicates when to retry (e.g., the HTTP `Retry-After` header);
    /// the engine uses `max(backoff, retry_after_ms)`. Without retries
    /// configured on the node, it has no effect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    /// Root cause (internal system error)
    #[serde(skip)]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl WorkflowError {
    /// Creates an error with code and message; the rest of the fields are `None`
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            source_task: None,
            payload: None,
            response: None,
            retry_after_ms: None,
            source: None,
        }
    }

    /// Assigns the originating task/node
    pub fn with_source_task(mut self, source_task: impl Into<String>) -> Self {
        self.source_task = Some(source_task.into());
        self
    }

    /// Sets the [`retry_after_ms`](Self::retry_after_ms) hint: the engine
    /// will wait at least this time before retrying.
    pub fn with_retry_after_ms(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
        self
    }
}

// Manual Clone: `source` is not Clone, it is omitted in the copy
impl Clone for WorkflowError {
    fn clone(&self) -> Self {
        Self {
            code: self.code.clone(),
            message: self.message.clone(),
            source_task: self.source_task.clone(),
            payload: self.payload.clone(),
            response: self.response.clone(),
            retry_after_ms: self.retry_after_ms,
            source: None,
        }
    }
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source_task {
            Some(task) => write!(f, "[{}] {}", task, self.message),
            None => write!(f, "{}", self.message),
        }
    }
}
