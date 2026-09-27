use std::sync::atomic::{AtomicBool, Ordering};
use workflow_forge::v2::*;

#[derive(Default)]
pub struct FailingStore {
    pub inner: modules::MemoryExecutionStore,
    pub fail: AtomicBool,
}
impl ExecutionStore for FailingStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn claim<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        self.inner.claim(owner)
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
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>> {
        Box::pin(async {
            if self.fail.load(Ordering::SeqCst) {
                Err(ForgeError::new("store.failed", "injected read failure"))
            } else {
                self.inner.unfinished().await
            }
        })
    }
    fn commit<'a>(
        &'a self,
        owner: &'a str,
        expected: u64,
        next: RunSnapshot,
    ) -> PortFuture<'a, ()> {
        self.inner.commit(owner, expected, next)
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        self.inner.collect(owner, now, limits)
    }
}
