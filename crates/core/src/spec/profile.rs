//! Definición declarativa de los perfiles de tarea (la sintaxis del
//! lenguaje). La tarea derivada que los ejecuta es
//! [`crate::task::profile::ProfileTask`].

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::task::TaskId;

/// Definición declarativa (JSON) de un perfil de tarea: una instancia
/// nombrada y reusable de una tarea base, con config horneada y schemas
/// propios.
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
/// Semántica:
/// - `bind` es un shape ([`crate::expr::shape`]) resuelto contra el input del
///   perfil: `@` es el input completo, `@.path` un subpath, el resto literales.
///   Sin `bind`, el input pasa tal cual a la base.
/// - `output` es un shape opcional sobre el output de la base (ej. `"@.body"`).
/// - Los objetos `{"$secret": "X"}` dentro de `bind` se resuelven al registrar
///   el perfil vía [`crate::io::secret::SecretProvider`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProfile {
    /// Id namespaced único bajo el que se registra el perfil
    pub id: TaskId,
    /// Id de la tarea base ya registrada que este perfil especializa
    pub extends: TaskId,
    /// Descripción legible para el catálogo
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema del input específico del perfil
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<schemars::Schema>,
    /// JSON Schema del output específico del perfil
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<schemars::Schema>,
    /// Shape que construye el input de la base a partir del input del perfil
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind: Option<Value>,
    /// Shape opcional aplicado al output de la base
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
}
