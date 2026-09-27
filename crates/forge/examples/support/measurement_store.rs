use std::{path::Path, sync::Arc};
use workflow_forge::v2::*;

/// Measurement-only composition. Durable measurements require a fresh database;
/// production hosts use SqliteExecutionStore directly to reopen accepted work.
pub struct MeasurementStore {
    pub execution: Arc<dyn ExecutionStore>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub profile: &'static str,
}
impl MeasurementStore {
    pub fn new(path: Option<&Path>) -> Result<Self, Box<dyn std::error::Error>> {
        if let Some(path) = path {
            if path.exists() {
                return Err("A measurement requires a new SQLite database path".into());
            }
            #[cfg(feature = "sqlite")]
            {
                let store = Arc::new(modules::SqliteExecutionStore::open(
                    path,
                    modules::SqliteOptions::default(),
                )?);
                return Ok(Self {
                    execution: store.clone(),
                    artifacts: store,
                    profile: "sqlite-wal-full",
                });
            }
            #[cfg(not(feature = "sqlite"))]
            return Err("Build with --features sqlite to measure this profile".into());
        }
        Ok(Self {
            execution: Arc::new(modules::MemoryExecutionStore::default()),
            artifacts: Arc::new(modules::MemoryArtifacts::default()),
            profile: "default-memory",
        })
    }
}

/// Keep numeric arguments compatible with the F-2 executables.
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
