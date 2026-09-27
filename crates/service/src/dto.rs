//! Wire DTOs intentionally exclude inputs, operation config and checkpoint packages.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use workflow_forge::v2::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareRequest {
    pub definition: WorkflowDefinition,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareResponse {
    pub workflow: WorkflowRevision,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartRequest {
    pub workflow: WorkflowRevision,
    pub input: Value,
    #[serde(default)]
    pub options: HttpStartOptions,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpStartOptions {
    pub require_durable: bool,
    pub timeout_ms: Option<u64>,
    pub receipt_key: Option<String>,
    pub artifacts: Vec<ArtifactRef>,
}
impl Default for HttpStartOptions {
    fn default() -> Self {
        Self {
            require_durable: true,
            timeout_ms: None,
            receipt_key: None,
            artifacts: Vec::new(),
        }
    }
}
impl From<HttpStartOptions> for StartOptions {
    fn from(value: HttpStartOptions) -> Self {
        Self {
            require_durable: value.require_durable,
            timeout_ms: value.timeout_ms,
            receipt_key: value.receipt_key,
            artifacts: value.artifacts,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectRequest {
    pub invocation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadArtifactRequest {
    pub artifact: ArtifactRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogResponse {
    pub capabilities: ApplicationCapabilities,
    pub items: Vec<OperationDescriptor>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunStatus {
    pub id: RunId,
    pub workflow: WorkflowRevision,
    pub revision: u64,
    pub state: RunState,
    pub cancel_requested: bool,
    pub created_at_ms: u64,
    pub deadline_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub invocation_count: usize,
    pub wait_count: usize,
    pub audit_count: usize,
    pub error: Option<ForgeError>,
    pub unresolved_effects: Vec<UnresolvedEffect>,
}
impl From<RunSnapshot> for RunStatus {
    fn from(run: RunSnapshot) -> Self {
        Self {
            id: run.id,
            workflow: WorkflowRevision {
                id: run.definition.id,
                revision: run.definition.revision,
            },
            revision: run.revision,
            state: run.state,
            cancel_requested: run.cancel_requested,
            created_at_ms: run.created_at_ms,
            deadline_at_ms: run.deadline_at_ms,
            finished_at_ms: run.finished_at_ms,
            invocation_count: run.invocations.len(),
            wait_count: run.waits.len(),
            audit_count: run.audit.len(),
            error: run.error.map(crate::error::sanitize),
            unresolved_effects: run
                .unresolved_effects
                .into_iter()
                .map(|mut effect| {
                    effect.reason = "Effect outcome remains unresolved".into();
                    effect
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InvocationStatus {
    pub path: String,
    pub id: String,
    pub attempt_id: String,
    pub attempts: u32,
    pub state: InvocationState,
    pub operation: Option<OperationRevision>,
    pub certainty: EffectCertainty,
    pub error: Option<ForgeError>,
    pub next_attempt_at_ms: Option<u64>,
}
impl From<(String, InvocationRecord)> for InvocationStatus {
    fn from((path, record): (String, InvocationRecord)) -> Self {
        Self {
            path,
            id: record.id,
            attempt_id: record.attempt_id,
            attempts: record.attempts,
            state: record.state,
            operation: record.operation,
            certainty: record.certainty,
            error: record.error.map(crate::error::sanitize),
            next_attempt_at_ms: record.next_attempt_at_ms,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WaitStatus {
    pub id: String,
    pub node: String,
    pub kind: String,
    pub correlation: Option<String>,
    pub created_at_ms: u64,
    pub deadline_at_ms: u64,
    pub start_confirmed: bool,
    pub state: WaitState,
    pub receipt: Option<SignalReceipt>,
}
impl From<WaitRecord> for WaitStatus {
    fn from(wait: WaitRecord) -> Self {
        let (kind, correlation) = match wait.kind {
            WaitKind::Timer => ("timer", None),
            WaitKind::Signal { correlation, .. } => ("signal", Some(correlation)),
        };
        Self {
            id: wait.id,
            node: wait.node,
            kind: kind.into(),
            correlation,
            created_at_ms: wait.created_at_ms,
            deadline_at_ms: wait.deadline_at_ms,
            start_confirmed: wait.start_confirmed,
            state: wait.state,
            receipt: wait.delivery.map(|delivery| delivery.receipt),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditStatus {
    pub kind: String,
    pub at_ms: u64,
    pub invocation_id: String,
    pub attempt_id: String,
    pub actor: Option<String>,
    pub command_id: Option<String>,
    pub decision: Option<String>,
    pub status: Option<ResolutionStatus>,
    pub error: Option<ForgeError>,
}
impl From<AuditEntry> for AuditStatus {
    fn from(entry: AuditEntry) -> Self {
        match entry {
            AuditEntry::Resolution(audit) => Self {
                kind: "resolution".into(),
                at_ms: audit.at_ms,
                invocation_id: audit.command.invocation_id,
                attempt_id: audit.command.observed_attempt,
                actor: Some(audit.actor),
                command_id: Some(audit.command.command_id),
                decision: Some(
                    match audit.command.resolution {
                        EffectResolution::ConfirmApplied { .. } => "confirm_applied",
                        EffectResolution::ConfirmNotApplied { .. } => "confirm_not_applied",
                        EffectResolution::RecordInconclusive { .. } => "record_inconclusive",
                        EffectResolution::StopTracking { .. } => "stop_tracking",
                    }
                    .into(),
                ),
                status: Some(audit.receipt.status),
                error: audit.receipt.diagnostic.map(crate::error::sanitize),
            },
            AuditEntry::Late(late) => Self {
                kind: "late".into(),
                at_ms: late.at_ms,
                invocation_id: late.invocation_id,
                attempt_id: late.attempt_id,
                actor: None,
                command_id: None,
                decision: None,
                status: None,
                error: late.error.map(crate::error::sanitize),
            },
        }
    }
}
