//! Contracts shared by the engine, official modules and external extensions.
//! This crate does not contain a scheduler, network client or database adapter.
mod definition;
mod error;
mod execution;
mod operation;
mod ports;
mod reconcile;
mod retry;

pub use definition::*;
pub use error::*;
pub use execution::*;
pub use operation::*;
pub use ports::*;
pub use reconcile::*;
pub use retry::*;

pub const PROTOCOL_VERSION: u32 = 1;
pub const WORKFLOW_FORMAT: &str = "forge.workflow/2";
pub const SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

pub type WorkflowValue = serde_json::Value;
pub type PortFuture<'a, T, E = ForgeError> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'a>>;
