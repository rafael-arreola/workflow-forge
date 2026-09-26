use futures::{StreamExt, stream};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};
use workflow_forge_protocol::*;

fn locked<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, ForgeError> {
    mutex
        .lock()
        .map_err(|_| ForgeError::new("store.failed", "Provider lock was poisoned"))
}

struct Receipt {
    run_id: RunId,
    reservation: ReceiptReservation,
}
#[derive(Default)]
struct State {
    owner: Option<String>,
    runs: BTreeMap<RunId, RunSnapshot>,
    receipts: BTreeMap<(String, String), Receipt>,
}
impl State {
    fn authorize(&self, owner: &str) -> Result<(), ForgeError> {
        if self.owner.as_deref() == Some(owner) {
            Ok(())
        } else {
            Err(ForgeError::new(
                "state.conflict",
                "Store is owned by a different runtime",
            ))
        }
    }
}

#[derive(Default)]
pub struct MemoryExecutionStore {
    state: Mutex<State>,
}

impl ExecutionStore for MemoryExecutionStore {
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            durable: false,
            checkpoint_format: 1,
        }
    }
    fn claim<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut state = locked(&self.state)?;
            if state.owner.is_some() {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Store already has an owner",
                ));
            }
            state.owner = Some(owner.into());
            Ok(())
        })
    }
    fn release<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut s = locked(&self.state)?;
            s.authorize(owner)?;
            s.owner = None;
            Ok(())
        })
    }
    fn create<'a>(
        &'a self,
        owner: &'a str,
        run: RunSnapshot,
        receipt: Option<ReceiptReservation>,
        limits: &'a Limits,
    ) -> PortFuture<'a, CreateOutcome> {
        Box::pin(async move {
            let mut s = locked(&self.state)?;
            s.authorize(owner)?;
            s.receipts
                .retain(|_, r| r.reservation.expires_at_ms > run.created_at_ms);
            if let Some(reservation) = &receipt {
                if let Some(old) = s
                    .receipts
                    .get(&(run.scope.clone(), reservation.key.clone()))
                {
                    if old.reservation.request != reservation.request {
                        return Err(ForgeError::new(
                            "state.conflict",
                            "Receipt key was used with different content",
                        ));
                    }
                    return Ok(CreateOutcome::Duplicate {
                        run_id: old.run_id.clone(),
                        expires_at_ms: old.reservation.expires_at_ms,
                    });
                }
                if s.receipts.len() >= limits.receipt_count {
                    return Err(ForgeError::new(
                        "admission.full",
                        "Receipt retention is full",
                    ));
                }
            }
            if s.runs.values().filter(|r| !r.state.is_terminal()).count()
                >= limits.active_runs.saturating_add(limits.pending_runs)
            {
                return Err(ForgeError::new("admission.full", "Run capacity is full"));
            }
            if run.revision != 0
                || s.runs.contains_key(&run.id)
                || run.checkpoint_format != 1
                || run.state != RunState::Accepted
                || !run.invocations.is_empty()
                || run.output.is_some()
                || run.error.is_some()
                || run.finished_at_ms.is_some()
                || run.cancel_requested
                || run.deadline_at_ms < run.created_at_ms
            {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Run identity already exists or has an invalid initial revision",
                ));
            }
            if let Some(reservation) = receipt {
                s.receipts.insert(
                    (run.scope.clone(), reservation.key.clone()),
                    Receipt {
                        run_id: run.id.clone(),
                        reservation,
                    },
                );
            }
            s.runs.insert(run.id.clone(), run);
            Ok(CreateOutcome::Created)
        })
    }
    fn get<'a>(&'a self, id: &'a RunId) -> PortFuture<'a, Option<RunSnapshot>> {
        Box::pin(async move { Ok(locked(&self.state)?.runs.get(id).cloned()) })
    }
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>> {
        Box::pin(async move {
            Ok(locked(&self.state)?
                .runs
                .values()
                .filter(|r| !r.state.is_terminal())
                .cloned()
                .collect())
        })
    }
    fn commit<'a>(
        &'a self,
        owner: &'a str,
        expected: u64,
        next: RunSnapshot,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut s = locked(&self.state)?;
            s.authorize(owner)?;
            let current = s
                .runs
                .get(&next.id)
                .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
            if current.revision != expected
                || next.revision
                    != expected
                        .checked_add(1)
                        .ok_or_else(|| ForgeError::new("state.conflict", "Revision exhausted"))?
                || current.scope != next.scope
                || current.actor != next.actor
                || current.resources != next.resources
                || current.definition != next.definition
                || current.input != next.input
                || current.created_at_ms != next.created_at_ms
                || current.deadline_at_ms != next.deadline_at_ms
                || current.checkpoint_format != next.checkpoint_format
                || (current.cancel_requested && !next.cancel_requested)
                || current.invocations.iter().any(|(id, invocation)| {
                    invocation.state == InvocationState::Succeeded
                        && next.invocations.get(id) != Some(invocation)
                })
                || current.state.is_terminal()
            {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Transition does not match the current run revision",
                ));
            }
            s.runs.insert(next.id.clone(), next);
            Ok(())
        })
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut s = locked(&self.state)?;
            s.authorize(owner)?;
            s.receipts.retain(|_, r| r.reservation.expires_at_ms > now);
            let mut finished: Vec<_> = s
                .runs
                .values()
                .filter_map(|r| {
                    r.finished_at_ms
                        .filter(|_| r.state.is_terminal())
                        .map(|at| (at, r.id.clone()))
                })
                .collect();
            finished.sort();
            let excess = finished.len().saturating_sub(limits.terminal_runs);
            for (index, (at, id)) in finished.into_iter().enumerate() {
                if index < excess || now.saturating_sub(at) >= limits.retention_ms {
                    s.runs.remove(&id);
                }
            }
            Ok(())
        })
    }
}

/// A host-owned map. Debug/serialization deliberately do not expose its values.
#[derive(Default)]
pub struct MemorySecrets {
    values: BTreeMap<(String, String), String>,
}
impl MemorySecrets {
    pub fn insert(&mut self, scope: &str, name: &str, value: String) {
        self.values.insert((scope.into(), name.into()), value);
    }
}
impl SecretProvider for MemorySecrets {
    fn contains(&self, scope: &str, name: &str) -> bool {
        self.values.contains_key(&(scope.into(), name.into()))
    }
    fn resolve<'a>(&'a self, scope: &'a str, name: &'a str) -> PortFuture<'a, SecretValue> {
        Box::pin(async move {
            self.values
                .get(&(scope.into(), name.into()))
                .cloned()
                .map(SecretValue::new)
                .ok_or_else(|| ForgeError::new("resource.missing", "Secret is unavailable"))
        })
    }
}

struct Artifact {
    reference: ArtifactRef,
    content: Arc<Vec<u8>>,
}
pub struct MemoryArtifacts {
    artifacts: Mutex<BTreeMap<String, Artifact>>,
    max_bytes: usize,
}
impl Default for MemoryArtifacts {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024)
    }
}
impl MemoryArtifacts {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            artifacts: Mutex::new(BTreeMap::new()),
            max_bytes,
        }
    }
}
impl ArtifactStore for MemoryArtifacts {
    fn durable(&self) -> bool {
        false
    }
    fn write<'a>(
        &'a self,
        scope: &'a str,
        mut content: ByteStream,
        media_type: &'a str,
    ) -> PortFuture<'a, ArtifactRef> {
        Box::pin(async move {
            if scope.len() > 256 || media_type.len() > 256 {
                return Err(ForgeError::new(
                    "resource.limit",
                    "Artifact metadata exceeds budget",
                ));
            }
            let mut data = Vec::new();
            let mut bytes = 0usize;
            while let Some(chunk) = content.next().await {
                let chunk = chunk?;
                bytes = bytes
                    .checked_add(chunk.len())
                    .ok_or_else(|| ForgeError::new("resource.limit", "Artifact exceeds budget"))?;
                if bytes > self.max_bytes {
                    return Err(ForgeError::new("resource.limit", "Artifact exceeds budget"));
                }
                data.extend_from_slice(&chunk);
            }
            let mut artifacts = locked(&self.artifacts)?;
            let used: u64 = artifacts.values().map(|a| a.reference.bytes).sum();
            if used.saturating_add(bytes as u64) > self.max_bytes as u64 || artifacts.len() >= 1024
            {
                return Err(ForgeError::new(
                    "resource.limit",
                    "Artifact storage is full",
                ));
            }
            let reference = ArtifactRef {
                id: uuid::Uuid::now_v7().to_string(),
                scope: scope.into(),
                bytes: bytes as u64,
                media_type: media_type.into(),
            };
            artifacts.insert(
                reference.id.clone(),
                Artifact {
                    reference: reference.clone(),
                    content: Arc::new(data),
                },
            );
            Ok(reference)
        })
    }
    fn read<'a>(&'a self, reference: &'a ArtifactRef) -> PortFuture<'a, ByteStream> {
        Box::pin(async move {
            let content = {
                let artifacts = locked(&self.artifacts)?;
                let artifact = artifacts
                    .get(&reference.id)
                    .filter(|a| a.reference == *reference)
                    .ok_or_else(|| {
                        ForgeError::new("resource.missing", "Artifact is unavailable")
                    })?;
                artifact.content.clone()
            };
            Ok(Box::pin(stream::unfold(
                (content, 0),
                |(content, offset)| async move {
                    if offset == content.len() {
                        return None;
                    }
                    let end = (offset + 64 * 1024).min(content.len());
                    let chunk = content[offset..end].to_vec();
                    Some((Ok(chunk), (content, end)))
                },
            )) as ByteStream)
        })
    }
}

pub struct NoopObserver;
impl ExecutionObserver for NoopObserver {
    fn observe(&self, _: ExecutionEvent) -> PortFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}
