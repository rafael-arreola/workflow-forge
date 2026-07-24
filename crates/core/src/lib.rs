//! # workflow-forge-core
//!
//! The workflow execution engine per spec 1.0, organized in
//! families with closed responsibilities:
//!
//! ```text
//! spec      the language: serde types of the document (nodes, edges,
//!           conditions, profiles). Pure data, no logic.
//!   ↓
//! validate  static rules over the definition (accumulates errors)
//!   ↓
//! runtime   execution: executor, state context, sub-workflow
//!           registration, precompiled indices
//!
//! expr      expression resolution ($.mapping, @.shape)
//! task      the extension contract: Task trait, manifests, registry,
//!           profiles
//! io        host resources: blobs ($blob) and secrets ($secret)
//! observe   execution events, in-memory history, reports
//! error     WorkflowError + error code catalog (error::codes)
//! ```
//!
//! ## Where do I add…?
//!
//! | I want to add | Where |
//! |---|---|
//! | a task (shortcut) | an async closure: [`register_typed`](task::TaskRegistry::register_typed) (typed, derived schemas) or [`register_fn`](task::TaskRegistry::register_fn) (raw JSON) |
//! | a task (full control) | implement [`task::Task`] (struct with state/dependencies) and register it in the [`task::TaskRegistry`] |
//! | a task profile | [`spec::TaskProfile`] + `register_profile` (or inline `tasks` section of the document) |
//! | a condition operator | implement [`expr::operators::ConditionOperator`] and register it in [`expr::operators::global`] |
//! | a validation rule | implement [`validate::ValidationRule`]; built-in → [`validate::rules`], host → `builder().rule(...)` |
//! | another blob storage | implement [`io::BlobStore`] + [`io::BlobStoreFactory`] → `builder().blobs(...)` |
//! | another secret provider | implement [`io::SecretProvider`] → `builder().secrets(...)` |
//! | a node kind (spec change) | variant in [`spec::node::NodeKind`] + handler in `runtime::handlers` + rules in [`validate::rules`] |
//! | an error code | documented constant in [`error::codes`] |
//! | an observability event | variant in [`observe::EventKind`] |

#![warn(missing_docs)]

pub mod error;
pub mod expr;
pub mod idempotency;
pub mod io;
pub mod observe;
pub mod runtime;
pub mod spec;
pub mod task;
#[cfg(feature = "testing")]
pub mod testing;
pub mod validate;

// Re-export so that extensions can build schemas without depending
// directly on schemars
pub use schemars;

/// Everything needed to write and execute workflows: the task contract,
/// the executor, and the types that cross the engine boundary.
pub mod prelude {
    pub use crate::error::WorkflowError;
    pub use crate::io::Secure;
    pub use crate::observe::{ExecutionObserver, InMemoryHistory};
    pub use crate::runtime::{
        CancellationToken, RunOptions, WorkflowContext, WorkflowExecutor, WorkflowRegistry,
    };
    pub use crate::spec::{TaskProfile, WorkflowDefinition};
    pub use crate::task::{
        FnTask, Task, TaskCtx, TaskId, TaskManifest, TaskRegistry, TypedTask, WorkflowData,
        WorkflowResult,
    };
}

/// Version of the workflow-forge-core crate
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
