//! workflow-forge: engine for declarative workflows defined with
//! JSON Schema + JSONPath.
//!
//! This is the project facade: re-exports the core and registers the
//! official extensions according to the enabled feature flags
//! (`util`, `data`, `http`; all active by default).
//!
//! ```no_run
//! use workflow_forge::prelude::*;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let workflow: WorkflowDefinition = serde_json::from_str(r#"{
//!     "name": "demo", "version": "0.1.0",
//!     "nodes": [
//!         { "id": "start", "kind": "start" },
//!         { "id": "wait", "kind": "task", "task": "util.delay",
//!           "input": { "ms": 100, "value": "$.trigger" } },
//!         { "id": "end", "kind": "end" }
//!     ],
//!     "edges": [
//!         { "from": "start", "to": "wait" },
//!         { "from": "wait", "to": "end" }
//!     ]
//! }"#)?;
//!
//! let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry())
//!     .map_err(|errors| format!("{errors:?}"))?;
//! let result = executor.run(WorkflowData(serde_json::json!({ "hello": 1 }))).await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

pub use workflow_forge_core as core;
pub use workflow_forge_core::idempotency;

use workflow_forge_core::task::TaskRegistry;

/// Everyday-use types, ready to import with a single `use`
pub mod prelude {
    pub use workflow_forge_core::error::WorkflowError;
    pub use workflow_forge_core::io::secret::{EnvSecrets, SecretProvider};
    pub use workflow_forge_core::observe::{
        EventKind, ExecutionEvent, ExecutionObserver, ExecutionReport, InMemoryHistory,
        JsonlObserver, TracingObserver,
    };
    pub use workflow_forge_core::runtime::{
        CancellationToken, RunOptions, WorkflowContext, WorkflowExecutor, WorkflowRegistry,
    };
    pub use workflow_forge_core::spec::{TaskProfile, WorkflowDefinition};
    pub use workflow_forge_core::task::{
        FnTask, Task, TaskCtx, TaskId, TaskManifest, TaskRegistry, TypedTask,
    };
    pub use workflow_forge_core::task::{WorkflowData, WorkflowResult};
}

/// Mock tasks + dry-run helpers (enabled by the `testing` feature).
#[cfg(feature = "testing")]
pub use workflow_forge_core::testing;

/// Registers all feature-enabled extensions in the registry
pub fn register_extensions(registry: &TaskRegistry) {
    #[cfg(feature = "util")]
    workflow_forge_ext_util::register(registry);
    #[cfg(feature = "data")]
    workflow_forge_ext_data::register(registry);
    #[cfg(feature = "http")]
    workflow_forge_ext_http::register(registry);
    #[cfg(feature = "tabular")]
    workflow_forge_ext_tabular::register(registry);
    #[cfg(feature = "sftp")]
    workflow_forge_ext_sftp::register(registry);
    #[cfg(feature = "compress")]
    workflow_forge_ext_compress::register(registry);
}

/// A new registry with all enabled extensions already registered
pub fn default_registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    register_extensions(&registry);
    registry
}
