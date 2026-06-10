//! workflow-forge: motor de workflows declarativos definidos con
//! JSON Schema + JSONPath.
//!
//! Esta es la fachada del proyecto: re-exporta el core y registra las
//! extensiones oficiales según los feature flags habilitados
//! (`util`, `data`, `http`; todos activos por default).
//!
//! ```no_run
//! use workflow_forge::prelude::*;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let workflow: WorkflowDefinition = serde_json::from_str(r#"{
//!     "name": "demo", "version": "0.1.0",
//!     "nodes": [
//!         { "id": "start", "kind": "start" },
//!         { "id": "espera", "kind": "task", "task": "util.delay",
//!           "input": { "ms": 100, "value": "$.trigger" } },
//!         { "id": "end", "kind": "end" }
//!     ],
//!     "edges": [
//!         { "from": "start", "to": "espera" },
//!         { "from": "espera", "to": "end" }
//!     ]
//! }"#)?;
//!
//! let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry())
//!     .map_err(|errors| format!("{errors:?}"))?;
//! let result = executor.run(WorkflowData(serde_json::json!({ "hola": 1 }))).await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

pub use workflow_forge_core as core;

use workflow_forge_core::registry::TaskRegistry;

/// Tipos de uso cotidiano, listos para importar con un solo `use`
pub mod prelude {
    pub use workflow_forge_core::context::WorkflowContext;
    pub use workflow_forge_core::error::WorkflowError;
    pub use workflow_forge_core::executor::WorkflowExecutor;
    pub use workflow_forge_core::registry::TaskRegistry;
    pub use workflow_forge_core::task::{Task, TaskId, TaskManifest};
    pub use workflow_forge_core::types::{WorkflowData, WorkflowResult};
    pub use workflow_forge_core::workflow::WorkflowDefinition;
}

/// Registra en el registry todas las extensiones habilitadas por features
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
}

/// Registry nuevo con todas las extensiones habilitadas ya registradas
pub fn default_registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    register_extensions(&registry);
    registry
}
