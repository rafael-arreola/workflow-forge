//! The **task** family: the engine's extension contract.
//!
//! Everything executable within a workflow is a task: an implementation of
//! the [`Task`] trait that publishes its contract ([`TaskManifest`]) and lives
//! in a [`TaskRegistry`]. Extensions (`workflow-forge-ext-*` crates) exist to
//! register tasks; the engine knows none in advance.
//!
//! Profiles ([`ProfileTask`]) are derived tasks: they specialize a registered
//! base task with baked-in configuration and their own schemas, and once
//! registered they are indistinguishable from any other task.

pub mod profile;
pub mod registry;
pub mod typed;

pub use profile::ProfileTask;
pub use registry::TaskRegistry;
pub use typed::{FnTask, TaskCtx, TypedTask};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::WorkflowError;
use crate::runtime::context::WorkflowContext;

/// Data that flows between nodes during workflow execution.
/// Wrapper around `serde_json::Value` with implicit conversions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowData(pub serde_json::Value);

impl std::ops::Deref for WorkflowData {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<serde_json::Value> for WorkflowData {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

impl From<WorkflowData> for serde_json::Value {
    fn from(data: WorkflowData) -> Self {
        data.0
    }
}

impl From<&WorkflowData> for serde_json::Value {
    fn from(data: &WorkflowData) -> Self {
        data.0.clone()
    }
}

/// Result of a task execution: successful data or structured error
pub type WorkflowResult = Result<WorkflowData, WorkflowError>;

impl WorkflowData {
    /// Returns the inner value as a string slice, if it is a JSON string.
    pub fn as_str(&self) -> Option<&str> {
        self.0.as_str()
    }
    /// Returns the inner value as i64, if it is a JSON integer fitting that range.
    pub fn as_i64(&self) -> Option<i64> {
        self.0.as_i64()
    }
    /// Returns the inner value as u64, if it is a JSON integer fitting that range.
    pub fn as_u64(&self) -> Option<u64> {
        self.0.as_u64()
    }
    /// Returns the inner value as f64, if it is a JSON number.
    pub fn as_f64(&self) -> Option<f64> {
        self.0.as_f64()
    }
    /// Returns the inner value as bool, if it is a JSON boolean.
    pub fn as_bool(&self) -> Option<bool> {
        self.0.as_bool()
    }
    /// Returns a reference to the inner JSON array, if it is one.
    pub fn as_array(&self) -> Option<&Vec<serde_json::Value>> {
        self.0.as_array()
    }
    /// Returns a reference to the inner JSON object, if it is one.
    pub fn as_object(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.0.as_object()
    }
    /// Returns true if the inner value is JSON null.
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

/// Namespaced identifier of a task (e.g. `"http.request"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId(pub String);

impl From<String> for TaskId {
    fn from(s: String) -> Self {
        TaskId(s)
    }
}

impl From<&str> for TaskId {
    fn from(s: &str) -> Self {
        TaskId(s.to_string())
    }
}

impl From<TaskId> for String {
    fn from(task_id: TaskId) -> Self {
        task_id.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for TaskId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for TaskId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(TaskId(s))
    }
}

/// Public contract of a task: the spec's "extension schema".
/// It is serializable, so the full catalog of available tasks can be exported
/// as JSON and validated/documented without the engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskManifest {
    /// Unique namespaced id (e.g. `"http.request"`)
    pub id: TaskId,
    /// Human-readable description of the task's purpose
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema that validates the task's input
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<schemars::Schema>,
    /// JSON Schema that validates the task's output
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<schemars::Schema>,
}

impl TaskManifest {
    /// Minimal manifest: only id, no schemas
    pub fn new(id: impl Into<TaskId>) -> Self {
        Self {
            id: id.into(),
            description: None,
            input_schema: None,
            output_schema: None,
        }
    }
}

/// Minimal executable unit within a workflow.
/// Each task publishes its manifest and defines its transformation logic.
#[async_trait]
pub trait Task: Send + Sync + 'static {
    /// Public contract of the task: id, description, and input/output schemas
    fn manifest(&self) -> &TaskManifest;

    /// Transforms input data into the expected output.
    /// Receives the immutable workflow context and the input data.
    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult;

    /// Task id, taken from the manifest
    fn task_id(&self) -> &TaskId {
        &self.manifest().id
    }
}
