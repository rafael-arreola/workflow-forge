//! Local, single-owner SQLite adapter. SQL and blocking I/O never run on the
//! engine's async tasks. A dropped command future does not roll back a queued commit.
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};
use tokio::sync::{mpsc, oneshot};
use workflow_forge_protocol::*;

mod artifacts;
mod codec;
mod migration;
mod query;
mod schema;
mod store;
mod worker;
use worker::Worker;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SqliteOptions {
    pub queue_capacity: usize,
    pub max_database_bytes: u64,
    pub max_record_bytes: usize,
    pub busy_timeout_ms: u64,
    pub max_artifacts: usize,
    pub max_artifact_bytes: u64,
    pub artifact_grace_ms: u64,
}
impl Default for SqliteOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            max_database_bytes: 512 * 1024 * 1024,
            max_record_bytes: 32 * 1024 * 1024,
            busy_timeout_ms: 5000,
            max_artifacts: 1000,
            max_artifact_bytes: 64 * 1024 * 1024,
            artifact_grace_ms: 3_600_000,
        }
    }
}
type Job = Box<dyn FnOnce(&mut Worker) + Send>;

#[derive(Clone)]
pub struct SqliteExecutionStore {
    inner: Arc<Inner>,
}
struct Inner {
    path: PathBuf,
    options: SqliteOptions,
    sender: OnceLock<Result<mpsc::Sender<Job>, ForgeError>>,
    domain: String,
}
impl SqliteExecutionStore {
    /// Inactive configuration. The worker, file lock and connection start on claim.
    /// The containing local directory must already exist.
    pub fn open(path: impl AsRef<Path>, options: SqliteOptions) -> Result<Self, ForgeError> {
        if !(1..=4096).contains(&options.queue_capacity)
            || !(1024..=1024 * 1024 * 1024).contains(&options.max_record_bytes)
            || options.max_database_bytes < 65536
            || options.max_database_bytes > i64::MAX as u64
            || !(1..=60000).contains(&options.busy_timeout_ms)
            || options.max_artifacts == 0
            || options.max_artifacts > 1_000_000
            || options.max_artifact_bytes == 0
            || options.max_artifact_bytes > i64::MAX as u64
            || options.artifact_grace_ms > i64::MAX as u64
        {
            return Err(ForgeError::new(
                "definition.invalid",
                "Invalid SQLite limits",
            ));
        }
        let path = if path.as_ref().is_absolute() {
            path.as_ref().to_owned()
        } else {
            std::env::current_dir().map_err(|_| failed())?.join(path)
        };
        Ok(Self {
            inner: Arc::new(Inner {
                path,
                options,
                sender: OnceLock::new(),
                domain: uuid::Uuid::now_v7().to_string(),
            }),
        })
    }
    fn sender(&self) -> Result<&mpsc::Sender<Job>, ForgeError> {
        self.inner
            .sender
            .get_or_init(|| {
                let (sender, mut receiver) =
                    mpsc::channel::<Job>(self.inner.options.queue_capacity);
                let mut worker = Worker::new(self.inner.path.clone(), self.inner.options.clone());
                std::thread::Builder::new()
                    .name("workflow-forge-sqlite".into())
                    .spawn(move || {
                        while let Some(job) = receiver.blocking_recv() {
                            job(&mut worker);
                        }
                    })
                    .map_err(|_| failed())?;
                Ok(sender)
            })
            .as_ref()
            .map_err(Clone::clone)
    }
    async fn call<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Worker) -> Result<T, ForgeError> + Send + 'static,
    ) -> Result<T, ForgeError> {
        let (send, receive) = oneshot::channel();
        self.sender()?
            .send(Box::new(move |worker| {
                let _ = send.send(action(worker));
            }))
            .await
            .map_err(|_| failed())?;
        receive.await.map_err(|_| failed())?
    }
}

fn failed() -> ForgeError {
    ForgeError::new("store.failed", "SQLite provider is unavailable")
}
fn conflict() -> ForgeError {
    ForgeError::new("state.conflict", "SQLite owner or revision does not match")
}
fn corrupt() -> ForgeError {
    ForgeError::new(
        "store.corrupt",
        "Stored checkpoint is invalid or inconsistent",
    )
}
fn unsupported() -> ForgeError {
    ForgeError::new(
        "capability.unsupported",
        "SQLite schema or checkpoint version is unsupported",
    )
}
fn db_error(error: rusqlite::Error) -> ForgeError {
    // SQL text, paths and checkpoint values are deliberately absent from diagnostics.
    match error.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DiskFull | rusqlite::ErrorCode::TooBig) => {
            ForgeError::new("resource.limit", "SQLite storage budget is exhausted")
        }
        Some(rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase) => corrupt(),
        _ => failed(),
    }
}
