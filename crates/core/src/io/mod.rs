//! The **io** family: host resources that cross the engine boundary.
//!
//! - [`blob`]: large binaries that travel by reference (`$blob`) instead
//!   of inline in the JSON context. [`BlobStore`] is the injectable trait;
//!   [`TempDirBlobStore`] the v1 implementation (temporary directory per
//!   execution).
//! - [`secret`]: credentials that are never baked into definitions
//!   (`{"$secret": "X"}`). [`SecretProvider`] is the injectable trait;
//!   [`EnvSecrets`] the default implementation (environment variables).

pub mod blob;
pub mod secret;
pub mod secure;

pub use blob::{BlobRef, BlobStore, BlobStoreFactory, TempDirBlobFactory, TempDirBlobStore};
pub use secret::{EnvSecrets, SecretProvider, resolve_secrets};
pub use secure::Secure;

