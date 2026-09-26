use crate::{
    ExecutionEvent, ExecutionView, ForgeError, InvocationRecord, InvocationState, Limits,
    PortFuture, RunHead, RunId, RunSnapshot,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;

#[derive(Clone, Copy, Debug)]
pub struct StoreCapabilities {
    pub durable: bool,
    pub checkpoint_format: u32,
}

#[derive(Clone, Debug)]
pub struct ReceiptReservation {
    pub key: String,
    pub request: Value,
    pub expires_at_ms: u64,
}

pub enum CreateOutcome {
    Created,
    Duplicate { run_id: RunId, expires_at_ms: u64 },
}

/// Stores enforce exclusive ownership, atomic receipt+run creation, and CAS commits.
/// An implementation must never acknowledge durable acceptance without persisting it.
/// Creation requires a clean Accepted snapshot at revision zero. Commits preserve
/// identity, input, definition, scope, actor, grants, timestamps and confirmed outputs;
/// cancellation is monotonic and terminal results cannot be overwritten.
pub trait ExecutionStore: Send + Sync {
    fn capabilities(&self) -> StoreCapabilities;
    fn claim<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()>;
    fn release<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()>;
    fn create<'a>(
        &'a self,
        owner: &'a str,
        run: RunSnapshot,
        receipt: Option<ReceiptReservation>,
        limits: &'a Limits,
    ) -> PortFuture<'a, CreateOutcome>;
    fn get<'a>(&'a self, id: &'a RunId) -> PortFuture<'a, Option<RunSnapshot>>;
    /// The head, requested node and descendant uncertainty must share a revision.
    /// `None` requests only the head and uncertainty of the whole run.
    fn view<'a>(
        &'a self,
        id: &'a RunId,
        node: Option<&'a str>,
    ) -> PortFuture<'a, Option<ExecutionView>> {
        Box::pin(async move { Ok(self.get(id).await?.map(|run| run.view(node))) })
    }
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>>;
    fn unfinished_heads(&self) -> PortFuture<'_, Vec<RunHead>> {
        Box::pin(async move {
            Ok(self
                .unfinished()
                .await?
                .iter()
                .map(RunSnapshot::head)
                .collect())
        })
    }
    /// `next.revision` must equal expected+1. Every check and write is atomic.
    fn commit<'a>(&'a self, owner: &'a str, expected: u64, next: RunSnapshot)
    -> PortFuture<'a, ()>;
    /// Atomic replacement of one invocation under the same run CAS as `commit`.
    /// This cannot alter run identity, head state, audit or any other invocation.
    /// The caller checks admission/data budgets against the coherent view first;
    /// an implementation updates its projection counters in the same commit.
    fn commit_invocation<'a>(
        &'a self,
        owner: &'a str,
        id: &'a RunId,
        expected: u64,
        node: &'a str,
        record: InvocationRecord,
    ) -> PortFuture<'a, ExecutionView> {
        Box::pin(async move {
            let mut run = self
                .get(id)
                .await?
                .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
            if run.revision != expected
                || run.state.is_terminal()
                || run
                    .invocations
                    .get(node)
                    .is_some_and(|old| old.state == InvocationState::Succeeded && old != &record)
            {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Invocation replacement does not match the committed revision",
                ));
            }
            run.revision = expected
                .checked_add(1)
                .ok_or_else(|| ForgeError::new("state.conflict", "Revision exhausted"))?;
            run.invocations.insert(node.into(), record);
            let view = run.view(Some(node));
            self.commit(owner, expected, run).await?;
            Ok(view)
        })
    }
    fn collect<'a>(&'a self, owner: &'a str, now_ms: u64, limits: &'a Limits)
    -> PortFuture<'a, ()>;
}

pub struct SecretValue(String);
impl SecretValue {
    pub fn new(value: String) -> Self {
        Self(value)
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretValue([REDACTED])")
    }
}
pub trait SecretProvider: Send + Sync {
    fn contains(&self, scope: &str, name: &str) -> bool;
    fn resolve<'a>(&'a self, scope: &'a str, name: &'a str) -> PortFuture<'a, SecretValue>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub id: String,
    pub scope: String,
    pub bytes: u64,
    pub media_type: String,
}
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, ForgeError>> + Send>>;
pub trait ArtifactStore: Send + Sync {
    fn durable(&self) -> bool;
    fn write<'a>(
        &'a self,
        scope: &'a str,
        content: ByteStream,
        media_type: &'a str,
    ) -> PortFuture<'a, ArtifactRef>;
    fn read<'a>(&'a self, reference: &'a ArtifactRef) -> PortFuture<'a, ByteStream>;
}

pub trait ExecutionObserver: Send + Sync {
    fn observe(&self, event: ExecutionEvent) -> PortFuture<'_, ()>;
}
