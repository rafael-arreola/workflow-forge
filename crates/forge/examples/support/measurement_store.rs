use std::{path::Path, sync::Arc};
use workflow_forge::v2::*;

/// Measurement-only composition. Durable measurements require a fresh database;
/// production hosts use SqliteExecutionStore directly to reopen accepted work.
pub struct MeasurementStore {
    pub execution: Arc<dyn ExecutionStore>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub profile: &'static str,
    pub provider_options: serde_json::Value,
}
impl MeasurementStore {
    pub fn new(path: Option<&Path>) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_sqlite_limit(path, None)
    }
    pub fn with_sqlite_limit(
        path: Option<&Path>,
        max_database_bytes: Option<u64>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if path.is_none() && max_database_bytes.is_some() {
            return Err("A SQLite byte budget requires --sqlite PATH".into());
        }
        if let Some(path) = path {
            if path.exists() {
                return Err("A measurement requires a new SQLite database path".into());
            }
            #[cfg(feature = "sqlite")]
            {
                let mut options = modules::SqliteOptions::default();
                if let Some(bytes) = max_database_bytes {
                    options.max_database_bytes = bytes;
                }
                let provider_options = serde_json::to_value(&options)?;
                let store = Arc::new(modules::SqliteExecutionStore::open(path, options)?);
                return Ok(Self {
                    execution: store.clone(),
                    artifacts: store,
                    profile: "sqlite-wal-full",
                    provider_options,
                });
            }
            #[cfg(not(feature = "sqlite"))]
            return Err("Build with --features sqlite to measure this profile".into());
        }
        Ok(Self {
            execution: Arc::new(modules::MemoryExecutionStore::default()),
            artifacts: Arc::new(modules::MemoryArtifacts::default()),
            profile: "default-memory",
            provider_options: serde_json::json!({}),
        })
    }
}

/// Keep numeric arguments compatible with the F-2 executables.
#[allow(
    dead_code,
    reason = "The shared module is also included by examples with their own option parser"
)]
pub fn sqlite_path(
    args: &[String],
    numeric: usize,
) -> Result<Option<&Path>, Box<dyn std::error::Error>> {
    match args.get(numeric..) {
        None | Some([]) => Ok(None),
        Some([flag, path]) if flag == "--sqlite" => Ok(Some(Path::new(path))),
        _ => Err("Optional arguments: --sqlite NEW_DATABASE_PATH (inventory: DIRECTORY)".into()),
    }
}
