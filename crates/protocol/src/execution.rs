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
    StopTracking,
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
                Permission::StopTracking,
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
    RetryScheduled,
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
    #[serde(default)]
    pub operation: Option<crate::OperationRevision>,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub effect_key: Option<String>,
    #[serde(default)]
    pub retry: crate::RetryPolicy,
    #[serde(default)]
    pub next_attempt_at_ms: Option<u64>,
    #[serde(default)]
    pub certainty: crate::EffectCertainty,
    #[serde(default)]
    pub control: Option<ControlFrame>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlFrame {
    Wait {
        id: String,
    },
    Decision {
        selected: String,
    },
    Parallel {
        branches: Vec<String>,
        next_index: usize,
        stopped: bool,
        error: Option<ForgeError>,
    },
    Foreach {
        total: usize,
        next_index: usize,
        stopped: bool,
        error: Option<ForgeError>,
    },
    Loop {
        iteration: usize,
        state: Value,
    },
    Subworkflow {
        workflow: crate::WorkflowRevision,
    },
}

/// Serializable state, not an in-process prepared plan or future.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedPackage {
    pub definitions: Vec<WorkflowDefinition>,
    pub operations: Vec<crate::OperationDescriptor>,
    pub schemas: Vec<crate::SchemaResource>,
}

impl ResolvedPackage {
    /// Receipt identity excludes presentation metadata, just like a definition.
    pub fn semantic_value(&self) -> Value {
        serde_json::json!({
            "definitions": self.definitions.iter().map(WorkflowDefinition::semantic_value).collect::<Vec<_>>(),
            "operations": self.operations.iter().map(crate::OperationDescriptor::semantic_value).collect::<Vec<_>>(),
            "schemas": self.schemas,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunSnapshot {
    #[serde(default)]
    pub waits: BTreeMap<String, crate::WaitRecord>,
    pub checkpoint_format: u32,
    pub id: RunId,
    pub scope: String,
    pub actor: String,
    pub resources: BTreeSet<String>,
    pub revision: u64,
    pub definition: WorkflowDefinition,
    pub package: ResolvedPackage,
    pub input: Value,
    pub state: RunState,
    /// Immutable references pinned atomically during acceptance. JSON-only
    /// format-2 checkpoints from SQL schema 1 have no attachments.
    #[serde(default)]
    pub artifacts: Vec<crate::ArtifactRef>,
    pub invocations: BTreeMap<String, InvocationRecord>,
    pub output: Option<Value>,
    pub error: Option<ForgeError>,
    pub created_at_ms: u64,
    pub deadline_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub cancel_requested: bool,
    #[serde(default)]
    pub audit: Vec<crate::AuditEntry>,
    #[serde(default)]
    pub unresolved_effects: Vec<crate::UnresolvedEffect>,
}

/// A coherent projection of one committed revision, without historical payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunHead {
    pub next_wakeup_at_ms: Option<u64>,
    pub id: RunId,
    pub scope: String,
    pub revision: u64,
    pub state: RunState,
    pub cancel_requested: bool,
    pub deadline_at_ms: u64,
    pub invocation_count: usize,
    pub retained_data_bytes: usize,
    pub unresolved_invocations: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionView {
    pub head: RunHead,
    pub invocation: Option<InvocationRecord>,
    pub unresolved_descendants: bool,
}

impl InvocationRecord {
    pub fn is_wait(&self) -> bool {
        matches!(self.control, Some(ControlFrame::Wait { .. }))
    }
    pub fn is_unresolved(&self) -> bool {
        self.state == InvocationState::Unknown
            || (self.operation.is_some()
                && self.state != InvocationState::Succeeded
                && self.certainty != crate::EffectCertainty::NotApplied)
    }
    /// Serialized payload budget; metadata allocation and RSS are separate limits.
    pub fn retained_data_bytes(&self) -> usize {
        json_bytes(&self.input)
            .saturating_add(json_bytes(&self.config))
            .saturating_add(self.output.as_ref().map_or(0, json_bytes))
            .saturating_add(self.error.as_ref().map_or(0, json_bytes))
            .saturating_add(self.control.as_ref().map_or(0, json_bytes))
    }
}

impl RunSnapshot {
    /// Output, diagnostics and audit payload; excludes immutable input and nodes.
    pub fn retained_result_bytes(&self) -> usize {
        self.output
            .as_ref()
            .map_or(0, json_bytes)
            .saturating_add(self.error.as_ref().map_or(0, json_bytes))
            .saturating_add(json_bytes(&self.audit))
            .saturating_add(json_bytes(&self.unresolved_effects))
            .saturating_add(self.waits.values().map(json_bytes).sum::<usize>())
    }
    pub fn retained_data_bytes(&self) -> usize {
        self.invocations.values().fold(
            json_bytes(&self.input)
                .saturating_add(self.artifacts.iter().map(json_bytes).sum::<usize>())
                .saturating_add(self.retained_result_bytes()),
            |bytes, record| bytes.saturating_add(record.retained_data_bytes()),
        )
    }
    pub fn head(&self) -> RunHead {
        RunHead {
            next_wakeup_at_ms: self.next_wakeup_at_ms(),
            id: self.id.clone(),
            scope: self.scope.clone(),
            revision: self.revision,
            state: self.state,
            cancel_requested: self.cancel_requested,
            deadline_at_ms: self.deadline_at_ms,
            invocation_count: self.invocations.len(),
            retained_data_bytes: self.retained_data_bytes(),
            unresolved_invocations: self
                .invocations
                .values()
                .filter(|r| r.is_unresolved())
                .count(),
        }
    }
    pub fn view(&self, node: Option<&str>) -> ExecutionView {
        let prefix = node.map(|key| format!("{key}/"));
        ExecutionView {
            head: self.head(),
            invocation: node.and_then(|key| self.invocations.get(key)).cloned(),
            unresolved_descendants: self.invocations.iter().any(|(key, record)| {
                record.is_unresolved()
                    && prefix.as_ref().is_none_or(|prefix| key.starts_with(prefix))
            }),
        }
    }
}

fn json_bytes(value: &impl Serialize) -> usize {
    serde_json::to_vec(value)
        .expect("public JSON values serialize")
        .len()
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartOptions {
    pub require_durable: bool,
    pub timeout_ms: Option<u64>,
    pub receipt_key: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<crate::ArtifactRef>,
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
    pub waits_per_run: usize,
    pub signal_bytes: usize,
    pub wait_timeout_ms: u64,
    pub document_bytes: usize,
    pub plan_bytes: usize,
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
    pub control_depth: usize,
    pub group_branches: usize,
    pub group_concurrency: usize,
    pub active_scopes: usize,
    pub foreach_items: usize,
    pub loop_iterations: usize,
    pub activations: usize,
    pub max_retry_attempts: u32,
    pub audit_entries: usize,
    pub evidence_bytes: usize,
    pub late_response_grace_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            waits_per_run: 256,
            signal_bytes: 64 * 1024,
            wait_timeout_ms: 24 * 60 * 60 * 1000,
            document_bytes: 1024 * 1024,
            plan_bytes: 16 * 1024 * 1024,
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
            control_depth: 8,
            group_branches: 32,
            group_concurrency: 32,
            active_scopes: 128,
            foreach_items: 10_000,
            loop_iterations: 1024,
            activations: 10_000,
            max_retry_attempts: 5,
            audit_entries: 256,
            evidence_bytes: 64 * 1024,
            late_response_grace_ms: 1000,
        }
    }
}
