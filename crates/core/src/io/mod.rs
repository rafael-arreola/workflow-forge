//! La familia **io**: recursos del host que cruzan la frontera del engine.
//!
//! - [`blob`]: binarios grandes que viajan por referencia (`$blob`) en vez
//!   de inline en el contexto JSON. [`BlobStore`] es el trait inyectable;
//!   [`TempDirBlobStore`] la implementación v1 (directorio temporal por
//!   ejecución).
//! - [`secret`]: credenciales que nunca se hornean en definiciones
//!   (`{"$secret": "X"}`). [`SecretProvider`] es el trait inyectable;
//!   [`EnvSecrets`] la implementación por defecto (variables de entorno).

pub mod blob;
pub mod secret;

pub use blob::{BlobRef, BlobStore, BlobStoreFactory, TempDirBlobFactory, TempDirBlobStore};
pub use secret::{EnvSecrets, SecretProvider, resolve_secrets};
