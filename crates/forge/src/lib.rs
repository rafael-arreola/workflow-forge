//! Declarative integration engine with JSON Schema contracts and explicit bindings.
//!
//! Compose once when the host starts, retain the runtime, and share its application
//! handle. `standard()` selects memory providers; the host can replace them before
//! building. Features expose optional modules without silently configuring them.
//!
//! ```no_run
//! use workflow_forge::prelude::*;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let definition: WorkflowDefinition = serde_json::from_value(serde_json::json!({
//!     "format": WORKFLOW_FORMAT, "schema_dialect": SCHEMA_DIALECT,
//!     "id": "echo", "revision": "r1", "input_schema": true, "output_schema": true,
//!     "entry": "echo", "nodes": [{"id": "echo", "kind": "operation",
//!         "operation": {"id": "forge.data.identity", "contract": "1", "implementation": "r1"},
//!         "config": {}, "input": {"select": {"source": "input", "pointer": ""}}
//!     }], "edges": [],
//!     "output": {"select": {"source": "node", "node": "echo", "pointer": ""}}
//! }))?;
//! let runtime = EngineRuntime::boot(WorkflowBuilder::standard().build()?, BootOptions::default()).await?;
//! let app = runtime.application();
//! let access = AccessContext::trusted("default"); // The host supplies authorization.
//! let completed = async {
//!     let plan = app.prepare(access.clone(), definition).await?;
//!     let receipt = app.start(access.clone(), StartRunRequest::new(plan, serde_json::json!({"hello": 1}))).await?;
//!     app.wait(access, receipt.run_id).await
//! }.await;
//! runtime.shutdown(ShutdownOptions::default()).await?;
//! assert_eq!(completed?.output, Some(serde_json::json!({"hello": 1})));
//! # Ok(())
//! # }
//! ```

pub mod v2;
pub use v2::*;

/// Public contracts, engine handles and standard composition for host applications.
pub mod prelude {
    pub use crate::v2::*;
}
