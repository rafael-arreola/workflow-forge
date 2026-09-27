use crate::{
    ArtifactAccess, ArtifactRef, ArtifactStore, ByteStream, ForgeError, OperationRevision,
    PortFuture, RunId, SecretProvider, SecretValue,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    Pure,
    Read,
    Write,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Repetition {
    Safe,
    Keyed,
    Unsafe,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectCertainty {
    #[default]
    NotApplied,
    Applied,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    InvalidInput,
    Rejected,
    Transient,
    Resource,
    Internal,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationDescriptor {
    pub revision: OperationRevision,
    pub schema_dialect: String,
    pub config_schema: Value,
    pub input_schema: Value,
    pub output_schema: Value,
    pub effect: EffectKind,
    pub repetition: Repetition,
    pub reconciliation: bool,
    pub required_resources: BTreeSet<String>,
    pub description: String,
    pub examples: Vec<Value>,
}

impl OperationDescriptor {
    pub fn semantic_value(&self) -> Value {
        let mut value = serde_json::to_value(self).expect("descriptor serializes");
        let object = value.as_object_mut().expect("descriptor is an object");
        object.remove("description");
        object.remove("examples");
        value
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationError {
    pub code: String,
    pub class: ErrorClass,
    pub certainty: EffectCertainty,
    /// Authors must supply a safe message; do not include credentials or raw payloads.
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct Invocation {
    pub id: String,
    pub attempt_id: String,
    pub operation: OperationRevision,
    pub config: Value,
    pub input: Value,
    pub effect_key: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OperationOutput {
    pub value: Value,
}
impl OperationOutput {
    pub fn json(value: Value) -> Self {
        Self { value }
    }
}
pub type OperationFuture<'a> = PortFuture<'a, OperationOutput, OperationError>;

pub trait Operation: Send + Sync {
    fn descriptor(&self) -> &OperationDescriptor;
    fn execute<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> OperationFuture<'a>;
}

#[derive(Clone)]
pub struct OperationContext {
    pub run_id: RunId,
    pub deadline_at_ms: u64,
    pub cancellation: CancellationToken,
    scope: String,
    allowed: BTreeSet<String>,
    secrets: Arc<dyn SecretProvider>,
    artifacts: Arc<dyn ArtifactStore>,
    artifact_access: ArtifactAccess,
}

impl OperationContext {
    pub fn new(
        artifact_access: ArtifactAccess,
        deadline_at_ms: u64,
        cancellation: CancellationToken,
        scope: String,
        allowed: BTreeSet<String>,
        secrets: Arc<dyn SecretProvider>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            run_id: artifact_access.run_id.clone(),
            artifact_access,
            deadline_at_ms,
            cancellation,
            scope,
            allowed,
            secrets,
            artifacts,
        }
    }
    pub fn scope(&self) -> &str {
        &self.scope
    }
    pub async fn secret(&self, name: &str) -> Result<SecretValue, ForgeError> {
        if !self.allowed.contains(&format!("secret:{name}")) {
            return Err(ForgeError::new(
                "access.denied",
                "Secret is not declared for this operation",
            ));
        }
        self.secrets.resolve(&self.scope, name).await
    }
    pub async fn read_artifact(&self, reference: &ArtifactRef) -> Result<ByteStream, ForgeError> {
        if !self.allowed.contains("artifacts") || reference.scope != self.scope {
            return Err(ForgeError::new(
                "access.denied",
                "Artifact is not accessible in this scope",
            ));
        }
        self.artifacts
            .read_for_run(&self.artifact_access, reference)
            .await
    }
    pub async fn write_artifact(
        &self,
        content: ByteStream,
        media_type: &str,
    ) -> Result<ArtifactRef, ForgeError> {
        if !self.allowed.contains("artifacts") {
            return Err(ForgeError::new(
                "access.denied",
                "Artifacts are not declared for this operation",
            ));
        }
        self.artifacts
            .write_for_run(&self.artifact_access, &self.scope, content, media_type)
            .await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModuleDescriptor {
    pub id: String,
    pub version: String,
    pub protocol_version: u32,
    pub exports: Vec<OperationRevision>,
}

pub struct OperationBundle {
    pub module: ModuleDescriptor,
    pub operations: Vec<Arc<dyn Operation>>,
    pub inspectors: Vec<Arc<dyn crate::EffectInspector>>,
}
