//! Declarative definition of task profiles (the language syntax).
//! The derived task that executes them is
//! [`crate::task::profile::ProfileTask`].

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::task::TaskId;

/// Declarative definition (JSON) of a task profile: a named,
/// reusable instance of a base task, with baked config and own
/// schemas.
///
/// ```json
/// {
///   "id": "acme.crear_orden",
///   "extends": "http.request",
///   "input_schema": { "type": "object", "required": ["sku", "qty"] },
///   "output_schema": { "type": "object", "required": ["order_id"] },
///   "bind": {
///     "url": "https://api.acme.com/orders",
///     "method": "POST",
///     "auth": { "type": "bearer", "token": { "$secret": "ACME_TOKEN" } },
///     "body": "@"
///   },
///   "output": "@.body"
/// }
/// ```
///
/// Semantics:
/// - `bind` is a shape ([`crate::expr::shape`]) resolved against the profile's
///   input: `@` is the full input, `@.path` a subpath, the rest literals.
///   Without `bind`, the input passes through to the base as-is.
/// - `output` is an optional shape over the base's output (e.g., `"@.body"`).
/// - `{"$secret": "X"}` objects within `bind` are resolved when registering
///   the profile via [`crate::io::secret::SecretProvider`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProfile {
    /// Namespaced unique id under which the profile is registered
    pub id: TaskId,
    /// Id of the already-registered base task that this profile specializes
    pub extends: TaskId,
    /// Human-readable description for the catalog
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema of the profile-specific input
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<schemars::Schema>,
    /// JSON Schema of the profile-specific output
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<schemars::Schema>,
    /// Shape that builds the base's input from the profile's input
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind: Option<Value>,
    /// Optional shape applied to the base's output
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
}
