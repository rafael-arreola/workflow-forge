use crate::{ForgeError, WorkflowDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub String);

impl std::fmt::Display for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Prepare,
    Start,
    Read,
    Cancel,
    Signal,
    Reconcile,
}

/// Constructed by the trusted host, never deserialized from an untrusted request.
#[derive(Clone, Debug)]
pub struct AccessContext {
    pub scope: String,
    pub actor: String,
    pub permissions: BTreeSet<Permission>,
    pub resources: BTreeSet<String>,
}

impl AccessContext {
    pub fn trusted(scope: &str) -> Self {
        Self {
            scope: scope.into(),
            actor: "host".into(),
            permissions: [
                Permission::Prepare,
                Permission::Start,
                Permission::Read,
                Permission::Cancel,
                Permission::Signal,
                Permission::Reconcile,
            ]
            .into(),
            resources: ["*".to_owned()].into(),
        }
    }
    pub fn permits_resource(&self, name: &str) -> bool {
        self.resources.contains("*") || self.resources.contains(name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Accepted,
    Running,
    Waiting,
    Blocked,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InvocationRecord {
    pub id: String,
    pub attempt_id: String,
    pub attempts: u32,
    pub state: InvocationState,
    pub input: Value,
    pub output: Option<Value>,
    pub error: Option<ForgeError>,
}

/// Serializable state, not an in-process prepared plan or future.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub checkpoint_format: u32,
    pub id: RunId,
    pub scope: String,
    pub actor: String,
    pub resources: BTreeSet<String>,
    pub revision: u64,
    pub definition: WorkflowDefinition,
    pub input: Value,
    pub state: RunState,
    pub invocations: BTreeMap<String, InvocationRecord>,
    pub output: Option<Value>,
    pub error: Option<ForgeError>,
    pub created_at_ms: u64,
    pub deadline_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub cancel_requested: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartOptions {
    pub require_durable: bool,
    pub timeout_ms: Option<u64>,
    pub receipt_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartReceipt {
    pub run_id: RunId,
    pub durable: bool,
    pub duplicate: bool,
    pub deduplicated_until_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub run_id: RunId,
    pub scope: String,
    pub revision: u64,
    pub state: RunState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Limits {
    pub document_bytes: usize,
    pub schema_resources: usize,
    pub nodes: usize,
    pub json_depth: usize,
    pub binding_depth: usize,
    pub binding_steps: usize,
    pub value_bytes: usize,
    pub run_bytes: usize,
    pub active_runs: usize,
    pub pending_runs: usize,
    pub concurrent_attempts: usize,
    pub attempt_timeout_ms: u64,
    pub run_timeout_ms: u64,
    pub terminal_runs: usize,
    pub retention_ms: u64,
    pub receipt_count: usize,
    pub receipt_ttl_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            document_bytes: 1024 * 1024,
            schema_resources: 64,
            nodes: 256,
            json_depth: 64,
            binding_depth: 32,
            binding_steps: 10_000,
            value_bytes: 1024 * 1024,
            run_bytes: 16 * 1024 * 1024,
            active_runs: 32,
            pending_runs: 128,
            concurrent_attempts: 32,
            attempt_timeout_ms: 30_000,
            run_timeout_ms: 300_000,
            terminal_runs: 1000,
            retention_ms: 3_600_000,
            receipt_count: 10_000,
            receipt_ttl_ms: 3_600_000,
        }
    }
}
