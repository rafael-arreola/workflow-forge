use crate::{
    binding,
    builder::{Composition, EngineAssembly},
    compiler::{self, PreparedWorkflow},
};
use futures::FutureExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, Notify, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use workflow_forge_protocol::*;

const READY: u8 = 0;
const DRAINING: u8 = 1;
const STOPPED: u8 = 2;
const FAILED: u8 = 3;
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn unavailable() -> ForgeError {
    ForgeError::new(
        "runtime.unavailable",
        "Runtime is not available for this command",
    )
}

#[derive(Default)]
pub struct BootOptions {
    pub definitions: Vec<WorkflowDefinition>,
    pub recovery: RecoveryPolicy,
}

/// Loading a library must not implicitly resume business work from an earlier host.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecoveryPolicy {
    #[default]
    RejectUnfinished,
    /// Explicitly resume recoverable work under its original deadlines and revisions.
    Resume,
}
pub struct ShutdownOptions {
    pub timeout: Duration,
}
impl Default for ShutdownOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }
}
#[derive(Debug)]
pub struct ShutdownReport {
    pub forced: bool,
    pub pending: Vec<RunId>,
}
pub struct StartRunRequest {
    pub plan: PreparedWorkflow,
    pub input: Value,
    pub options: StartOptions,
}
impl StartRunRequest {
    pub fn new(plan: PreparedWorkflow, input: Value) -> Self {
        Self {
            plan,
            input,
            options: StartOptions::default(),
        }
    }
}

struct Shared {
    calls: std::sync::Mutex<JoinSet<()>>,
    composition: Arc<Composition>,
    phase: AtomicU8,
    admission: Mutex<()>,
    cancel: CancellationToken,
    wake: Notify,
    changed: Notify,
    plans: Mutex<BTreeMap<RunId, PreparedWorkflow>>,
    versions: Mutex<BTreeMap<(String, String), Value>>,
    cancellations: Mutex<BTreeMap<RunId, CancellationToken>>,
    attempts: Arc<Semaphore>,
    scope_slots: Arc<Semaphore>,
    events: mpsc::Sender<ExecutionEvent>,
    late: mpsc::Sender<invocation::LateAttempt>,
}

#[derive(Clone)]
pub struct WorkflowApplication {
    shared: Arc<Shared>,
}
pub struct EngineRuntime {
    app: WorkflowApplication,
    supervisor: Option<JoinHandle<Result<(), ForgeError>>>,
    supervisor_abort: tokio::task::AbortHandle,
    observer: Option<JoinHandle<()>>,
    owns_store: bool,
}

mod application;
mod control;
mod coordinator;
mod execute;
mod groups;
mod invocation;
mod lifecycle;
mod planner;
mod reconcile;
mod recovery;
mod resources;
mod signals;
mod state;
mod steps;
mod waits;
use application::{prepare_recovered, prepare_registered};
use coordinator::{supervise, transition};
