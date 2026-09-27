//! HTTP is an adapter over the public workflow facade, not another coordinator.
mod auth;
pub mod dto;
mod error;
pub mod host;
mod jobs;
mod listener;
mod pagination;
mod routes;
mod runtime;
mod transport;

pub use auth::{BearerIdentity, RequestAuthenticator, StaticBearerAuth};
pub use axum::http::HeaderMap;
pub use runtime::{ServiceOptions, ServiceRuntime, ServiceShutdownReport};
