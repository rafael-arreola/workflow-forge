//! The **runtime** family: workflow execution.
//!
//! - [`executor`]: the orchestrator. Builds with validation + compilation
//!   (schemas, graph index, sub-workflows) and traverses the graph
//!   run-to-completion. The per-node-kind semantics lives in `handlers`
//!   (one module per family) and the retry/timeout/panic policy in
//!   `policy`.
//! - [`context`]: the state document of an execution (`$.trigger`,
//!   `$.nodes.<id>.output`, `$.workflow`) against which mappings and
//!   conditions are resolved.
//! - [`registry`]: reusable workflows as sub-workflows, by name.
//! - `graph` / `schemas` (private): precompiled indices and validators
//!   built when constructing the executor.

pub mod context;
pub mod executor;
pub(crate) mod graph;
pub(crate) mod handlers;
pub(crate) mod policy;
pub mod registry;
pub(crate) mod schemas;

pub use context::WorkflowContext;
pub use executor::{CancellationToken, RunOptions, WorkflowExecutor, WorkflowExecutorBuilder};
pub use registry::WorkflowRegistry;
