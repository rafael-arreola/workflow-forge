use std::{future::Future, sync::Mutex};
use tokio::task::JoinSet;

/// Only transfers needing asynchronous cleanup outlive a request. The service
/// owns their join set; closing it prevents a race with late request admission.
pub(crate) struct Jobs(Mutex<Option<JoinSet<()>>>);
impl Jobs {
    pub fn new() -> Self {
        Self(Mutex::new(Some(JoinSet::new())))
    }

    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let Some(jobs) = guard.as_mut() else {
            return false;
        };
        while jobs.try_join_next().is_some() {}
        jobs.spawn(future);
        true
    }

    pub fn close(&self) -> JoinSet<()> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or_default()
    }
}
