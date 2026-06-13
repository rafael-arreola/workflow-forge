//! The **expr** family: expression resolution of the language.
//!
//! Workflows declare data with three string conventions, all
//! resolved with the same JSONPath engine ([`path`]):
//!
//! | Convention | Where used | Source document |
//! |---|---|---|
//! | `$.path` ([`mapping`]) | node `input`, foreach `items`, end `output` | execution context |
//! | `@.path` ([`shape`]) | profile `bind`/`output`, `data.transform` | a local value (input/output) |
//! | `{"$secret": "X"}` | profile `bind` | secret provider ([`crate::io::secret`]) |
//!
//! Key semantic difference: a `$.` path that does not resolve is an **error**
//! (`MAPPING_PATH_NOT_FOUND`, almost always a definition bug); an `@.` path
//! that does not resolve produces **`null`** (shapes fill structures).
//!
//! Spec conditions are evaluated in [`operators`], which is also the
//! vocabulary extension point: a host registers custom operators
//! ([`operators::ConditionOperator`]) and validation recognizes them.

pub mod mapping;
pub mod operators;
pub mod path;
pub mod shape;

pub use mapping::resolve;
pub use shape::apply_shape;
