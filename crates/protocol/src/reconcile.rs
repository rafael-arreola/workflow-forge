use crate::{ForgeError, Invocation, OperationContext, OperationRevision, PortFuture, RunId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Attestation supplied by the trusted host/adapter, not a remotely verified proof.
/// Large evidence lives in an authorized artifact; no credentials belong in note.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectEvidence {
    pub authority: String,
    pub reference: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "finding", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectInspection {
    Applied {
        output: Value,
        evidence: EffectEvidence,
    },
    NotApplied {
        evidence: EffectEvidence,
        quiescent: bool,
    },
    Inconclusive {
        reason: String,
        evidence: Option<EffectEvidence>,
    },
}

/// Read-only investigation. Only the coordinator can apply a resolution.
pub trait EffectInspector: Send + Sync {
    fn operation(&self) -> &OperationRevision;
    fn inspect<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> PortFuture<'a, EffectInspection>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectResolution {
    ConfirmApplied {
        output: Value,
        evidence: EffectEvidence,
    },
    ConfirmNotApplied {
        evidence: EffectEvidence,
        quiescent: bool,
        retry: bool,
    },
    RecordInconclusive {
        reason: String,
        evidence: Option<EffectEvidence>,
    },
    StopTracking {
        reason: String,
        evidence: Option<EffectEvidence>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileCommand {
    pub command_id: String,
    pub run_id: RunId,
    pub invocation_id: String,
    pub expected_revision: u64,
    pub observed_attempt: String,
    pub resolution: EffectResolution,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionStatus {
    Applied,
    RetryReady,
    Failed,
    StillBlocked,
    StoppedTracking,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReconcileReceipt {
    pub command_id: String,
    pub run_id: RunId,
    pub revision: u64,
    pub status: ResolutionStatus,
    pub diagnostic: Option<ForgeError>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolutionAudit {
    pub command: ReconcileCommand,
    pub actor: String,
    pub at_ms: u64,
    pub receipt: ReconcileReceipt,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LateObservation {
    pub invocation_id: String,
    pub attempt_id: String,
    pub at_ms: u64,
    pub output: Option<Value>,
    pub error: Option<ForgeError>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditEntry {
    Resolution(Box<ResolutionAudit>),
    Late(LateObservation),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedEffect {
    pub invocation_id: String,
    pub attempt_id: String,
    pub effect_key: Option<String>,
    pub reason: String,
}
