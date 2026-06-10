pub mod blob;
pub mod condition;
pub mod context;
pub mod error;
pub mod executor;
pub mod mapping;
pub mod node;
pub mod observe;
pub mod profile;
pub mod registry;
pub mod secret;
pub mod shape;
pub mod task;
pub mod validation;
pub mod workflow;

// Re-export para que las extensiones construyan schemas sin depender
// directamente de schemars
pub use schemars;

/// Versión del crate workflow-forge-core
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
