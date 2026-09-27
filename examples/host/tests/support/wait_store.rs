use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;
use workflow_forge::prelude::*;

/// Hold one candidate before its actual CAS, so the competing commit is observed
/// rather than inferred from a sleep or a scheduler's preferred poll order.
pub struct GateStore {
    inner: Arc<dyn ExecutionStore>,
    matches: fn(&RunSnapshot) -> bool,
    held: AtomicBool,
    entered: Notify,
    release: Notify,
    owner: Mutex<String>,
}
impl GateStore {
    pub fn new(inner: Arc<dyn ExecutionStore>, matches: fn(&RunSnapshot) -> bool) -> Self {
        Self {
            inner,
            matches,
            held: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            owner: Mutex::new(String::new()),
        }
    }
    pub async fn entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
    pub fn release(&self) {
        self.release.notify_one();
    }
    pub fn owner(&self) -> String {
        self.owner.lock().unwrap().clone()
    }
}
impl ExecutionStore for GateStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn artifact_domain(&self) -> Option<&str> {
        self.inner.artifact_domain()
    }
    fn claim<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        Box::pin(async move {
            self.inner.claim(owner).await?;
            *self.owner.lock().unwrap() = owner.into();
            Ok(())
        })
    }
    fn release<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        self.inner.release(owner)
    }
    fn create<'a>(
        &'a self,
        owner: &'a str,
        run: RunSnapshot,
        receipt: Option<ReceiptReservation>,
        limits: &'a Limits,
    ) -> PortFuture<'a, CreateOutcome> {
        self.inner.create(owner, run, receipt, limits)
    }
    fn get<'a>(&'a self, id: &'a RunId) -> PortFuture<'a, Option<RunSnapshot>> {
        self.inner.get(id)
    }
    fn view<'a>(
        &'a self,
        id: &'a RunId,
        node: Option<&'a str>,
    ) -> PortFuture<'a, Option<ExecutionView>> {
        self.inner.view(id, node)
    }
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>> {
        self.inner.unfinished()
    }
    fn unfinished_heads(&self) -> PortFuture<'_, Vec<RunHead>> {
        self.inner.unfinished_heads()
    }
    fn commit<'a>(&'a self, owner: &'a str, expected: u64, run: RunSnapshot) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if (self.matches)(&run) && !self.held.swap(true, Ordering::AcqRel) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.commit(owner, expected, run).await
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
        self.inner
            .commit_invocation(owner, id, expected, node, record)
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        self.inner.collect(owner, now, limits)
    }
}
