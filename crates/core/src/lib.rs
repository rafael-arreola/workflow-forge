pub mod condition;
pub mod context;
pub mod error;
pub mod executor;
pub mod mapping;
pub mod node;
pub mod registry;
pub mod task;
pub mod types;
pub mod validation;
pub mod workflow;

/// Versión del crate workflow-forge-core
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
