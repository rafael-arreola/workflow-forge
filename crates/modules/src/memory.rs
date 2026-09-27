use futures::{StreamExt, stream};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard, Weak},
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
// Counters and the uncertainty index change under the same lock as the snapshot.
struct StoredRun {
    // Private header: package is materialized only at the public snapshot boundary.
    snapshot: RunSnapshot,
    package: Arc<ResolvedPackage>,
    bytes: usize,
    unresolved: BTreeSet<String>,
}
impl std::ops::Deref for StoredRun {
    type Target = RunSnapshot;
    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}
impl StoredRun {
    fn new(snapshot: RunSnapshot, package: Arc<ResolvedPackage>) -> Self {
        let bytes = snapshot.retained_data_bytes();
        let unresolved = snapshot
            .invocations
            .iter()
            .filter(|(_, r)| r.is_unresolved())
            .map(|(key, _)| key.clone())
            .collect();
        Self {
            snapshot,
            package,
            bytes,
            unresolved,
        }
    }
    fn materialize(&self) -> RunSnapshot {
        let mut snapshot = self.snapshot.clone();
        snapshot.package = self.package.as_ref().clone();
        snapshot
    }
    fn head(&self) -> RunHead {
        RunHead {
            next_wakeup_at_ms: self.snapshot.next_wakeup_at_ms(),
            id: self.id.clone(),
            scope: self.scope.clone(),
            revision: self.revision,
            state: self.snapshot.state,
            cancel_requested: self.cancel_requested,
            deadline_at_ms: self.deadline_at_ms,
            invocation_count: self.invocations.len(),
            retained_data_bytes: self.bytes,
            unresolved_invocations: self.unresolved.len(),
        }
    }
    fn after(current: &Self, snapshot: RunSnapshot) -> Self {
        // Head-only transitions reuse payload sizes and uncertainty already checked
        // for the same nodes. Full commit validates immutable input before this.
        if current.invocations == snapshot.invocations {
            Self {
                bytes: current
                    .bytes
                    .saturating_sub(current.snapshot.retained_result_bytes())
                    .saturating_add(snapshot.retained_result_bytes()),
                unresolved: current.unresolved.clone(),
                snapshot,
                package: current.package.clone(),
            }
        } else {
            Self::new(snapshot, current.package.clone())
        }
    }
    fn view(&self, node: Option<&str>) -> ExecutionView {
        let unresolved_descendants = match node {
            Some(key) => {
                let prefix = format!("{key}/");
                self.unresolved
                    .range(prefix.clone()..)
                    .next()
                    .is_some_and(|key| key.starts_with(&prefix))
            }
            None => !self.unresolved.is_empty(),
        };
        ExecutionView {
            head: self.head(),
            invocation: node.and_then(|key| self.invocations.get(key)).cloned(),
            unresolved_descendants,
        }
    }
}

#[derive(Default)]
struct State {
    owner: Option<String>,
    runs: BTreeMap<RunId, StoredRun>,
    receipts: BTreeMap<(String, String), Receipt>,
    // Exact serialized content is the key; no hash collision can merge revisions.
    // Weak entries are pruned with run GC, so the pool cannot retain dead packages.
    packages: BTreeMap<Vec<u8>, Weak<ResolvedPackage>>,
}
impl State {
    fn intern(&mut self, package: ResolvedPackage) -> Result<Arc<ResolvedPackage>, ForgeError> {
        let key = serde_json::to_vec(&package).map_err(|_| {
            ForgeError::new("store.failed", "Recovery package cannot be serialized")
        })?;
        if let Some(shared) = self.packages.get(&key).and_then(Weak::upgrade) {
            return Ok(shared);
        }
        let shared = Arc::new(package);
        self.packages.insert(key, Arc::downgrade(&shared));
        Ok(shared)
    }
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
            checkpoint_format: CHECKPOINT_FORMAT,
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
        mut run: RunSnapshot,
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
            run.validate_initial()?;
            if s.runs.contains_key(&run.id) {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Run identity already exists or has an invalid initial revision",
                ));
            }
            let package = s.intern(std::mem::take(&mut run.package))?;
            if let Some(reservation) = receipt {
                s.receipts.insert(
                    (run.scope.clone(), reservation.key.clone()),
                    Receipt {
                        run_id: run.id.clone(),
                        reservation,
                    },
                );
            }
            s.runs.insert(run.id.clone(), StoredRun::new(run, package));
            Ok(CreateOutcome::Created)
        })
    }
    fn get<'a>(&'a self, id: &'a RunId) -> PortFuture<'a, Option<RunSnapshot>> {
        Box::pin(async move {
            Ok(locked(&self.state)?
                .runs
                .get(id)
                .map(StoredRun::materialize))
        })
    }
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>> {
        Box::pin(async move {
            Ok(locked(&self.state)?
                .runs
                .values()
                .filter(|r| !r.state.is_terminal())
                .map(StoredRun::materialize)
                .collect())
        })
    }
    fn commit<'a>(
        &'a self,
        owner: &'a str,
        expected: u64,
        mut next: RunSnapshot,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut s = locked(&self.state)?;
            s.authorize(owner)?;
            let current = s
                .runs
                .get(&next.id)
                .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
            // Validate the shared immutable value before checking the remaining
            // header with the same public invariant function as durable stores.
            if current.package.as_ref() != &next.package {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Recovery package differs from the accepted snapshot",
                ));
            }
            next.package = ResolvedPackage::default();
            current.validate_successor(expected, &next)?;
            let next = StoredRun::after(current, next);
            s.runs.insert(next.id.clone(), next);
            Ok(())
        })
    }
    fn view<'a>(
        &'a self,
        id: &'a RunId,
        node: Option<&'a str>,
    ) -> PortFuture<'a, Option<ExecutionView>> {
        Box::pin(async move { Ok(locked(&self.state)?.runs.get(id).map(|r| r.view(node))) })
    }
    fn unfinished_heads(&self) -> PortFuture<'_, Vec<RunHead>> {
        Box::pin(async move {
            Ok(locked(&self.state)?
                .runs
                .values()
                .filter(|r| !r.snapshot.state.is_terminal())
                .map(StoredRun::head)
                .collect())
        })
    }
    fn commit_invocation<'a>(
        &'a self,
        owner: &'a str,
        id: &'a RunId,
        expected: u64,
        node: &'a str,
        record: InvocationRecord,
    ) -> PortFuture<'a, ExecutionView> {
        Box::pin(async move {
            let mut state = locked(&self.state)?;
            state.authorize(owner)?;
            let current = state
                .runs
                .get_mut(id)
                .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
            if current.revision != expected
                || record.is_wait()
                || current
                    .invocations
                    .get(node)
                    .is_some_and(InvocationRecord::is_wait)
                || current.snapshot.state.is_terminal()
                || current
                    .invocations
                    .get(node)
                    .is_some_and(|old| old.state == InvocationState::Succeeded && old != &record)
            {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Invocation replacement does not match the committed revision",
                ));
            }
            let revision = expected
                .checked_add(1)
                .ok_or_else(|| ForgeError::new("state.conflict", "Revision exhausted"))?;
            current.bytes = current
                .bytes
                .saturating_sub(
                    current
                        .invocations
                        .get(node)
                        .map_or(0, InvocationRecord::retained_data_bytes),
                )
                .saturating_add(record.retained_data_bytes());
            if record.is_unresolved() {
                current.unresolved.insert(node.into());
            } else {
                current.unresolved.remove(node);
            }
            current.snapshot.invocations.insert(node.into(), record);
            current.snapshot.revision = revision;
            Ok(current.view(Some(node)))
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
            s.packages.retain(|_, package| package.strong_count() != 0);
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
