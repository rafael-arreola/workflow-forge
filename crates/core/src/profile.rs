//! Perfiles de tarea: instancias nombradas y reusables de una tarea base.
//!
//! Un perfil especializa una tarea registrada (`extends`) horneando su
//! configuración (`bind`) y publicando schemas de input/output propios.
//! Una vez registrado ([`crate::registry::TaskRegistry::register_profile`])
//! es una tarea más: aparece en el catálogo y los workflows la invocan por id.
//!
//! ```json
//! {
//!   "id": "acme.crear_orden",
//!   "extends": "http.request",
//!   "input_schema": { "type": "object", "required": ["sku", "qty"] },
//!   "output_schema": { "type": "object", "required": ["order_id"] },
//!   "bind": {
//!     "url": "https://api.acme.com/orders",
//!     "method": "POST",
//!     "auth": { "type": "bearer", "token": { "$secret": "ACME_TOKEN" } },
//!     "fail_on_error_status": true,
//!     "body": "@"
//!   },
//!   "output": "@.body"
//! }
//! ```
//!
//! Semántica:
//! - `bind` es un shape ([`crate::shape`]) resuelto contra el input del
//!   perfil: `@` es el input completo, `@.path` un subpath, el resto literales.
//!   Sin `bind`, el input pasa tal cual a la base.
//! - `output` es un shape opcional sobre el output de la base (ej. `"@.body"`).
//! - Los objetos `{"$secret": "X"}` dentro de `bind` se resuelven al registrar
//!   el perfil vía [`crate::secret::SecretProvider`].

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::WorkflowContext;
use crate::error::WorkflowError;
use crate::secret::{SecretProvider, resolve_secrets};
use crate::shape::apply_shape;
use crate::task::{Task, TaskId, TaskManifest, WorkflowData, WorkflowResult};

/// Definición declarativa (JSON) de un perfil de tarea.
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

/// Tarea derivada de un perfil: envuelve a la base con el bind/output del
/// perfil y expone el manifiesto del perfil (no el de la base).
pub struct ProfileTask {
    manifest: TaskManifest,
    base: Arc<dyn Task>,
    bind: Option<Value>,
    output_shape: Option<Value>,
    /// Validador del input-schema de la BASE, precompilado: detecta binds que
    /// producen un input que la base rechazaría (fail-fast con mejor error)
    base_input_validator: Option<jsonschema::Validator>,
}

impl ProfileTask {
    /// Construye la tarea derivada resolviendo los secretos del `bind`.
    pub fn new(
        profile: TaskProfile,
        base: Arc<dyn Task>,
        secrets: &dyn SecretProvider,
    ) -> Result<Self, WorkflowError> {
        let mut bind = profile.bind;
        if let Some(bind) = bind.as_mut() {
            resolve_secrets(bind, secrets)
                .map_err(|e| e.with_source_task(profile.id.to_string()))?;
        }

        let base_input_validator = base
            .manifest()
            .input_schema
            .as_ref()
            .and_then(|schema| serde_json::to_value(schema).ok())
            .and_then(|json| jsonschema::validator_for(&json).ok());

        let manifest = TaskManifest {
            id: profile.id,
            description: profile.description,
            input_schema: profile.input_schema,
            output_schema: profile.output_schema,
        };

        Ok(Self {
            manifest,
            base,
            bind,
            output_shape: profile.output,
            base_input_validator,
        })
    }
}

#[async_trait]
impl Task for ProfileTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let base_input = match &self.bind {
            Some(bind) => apply_shape(bind, &input.0)
                .map_err(|e| e.with_source_task(self.manifest.id.to_string()))?,
            None => input.0,
        };

        if let Some(validator) = &self.base_input_validator
            && !validator.is_valid(&base_input)
        {
            let errors: Vec<String> = validator
                .iter_errors(&base_input)
                .map(|e| e.to_string())
                .collect();
            return Err(WorkflowError::new(
                "PROFILE_BIND_INVALID",
                format!(
                    "El bind del perfil '{}' produce un input que la tarea base '{}' rechaza: {}",
                    self.manifest.id,
                    self.base.task_id(),
                    errors.join("; ")
                ),
            ));
        }

        let base_output = self.base.execute(ctx, WorkflowData(base_input)).await?;

        match &self.output_shape {
            Some(shape) => apply_shape(shape, &base_output.0)
                .map(WorkflowData)
                .map_err(|e| e.with_source_task(self.manifest.id.to_string())),
            None => Ok(base_output),
        }
    }
}
