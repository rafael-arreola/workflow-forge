//! The profile-derived task ([`ProfileTask`]): wraps the base task with the
//! profile's `bind`/`output`. The profile's declarative definition (the JSON
//! syntax) is [`crate::spec::profile::TaskProfile`].

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::{WorkflowError, codes};
use crate::expr::shape::apply_shape;
use crate::io::secret::{SecretProvider, resolve_secrets};
use crate::runtime::context::WorkflowContext;
use crate::spec::profile::TaskProfile;
use crate::task::{Task, TaskManifest, WorkflowData, WorkflowResult};

/// Task derived from a profile: wraps the base with the profile's bind/output
/// and exposes the profile's manifest (not the base's).
pub struct ProfileTask {
    manifest: TaskManifest,
    base: Arc<dyn Task>,
    bind: Option<Value>,
    output_shape: Option<Value>,
    /// Precompiled validator for the BASE's input-schema: detects binds that
    /// produce an input the base would reject (fail-fast with a better error)
    base_input_validator: Option<jsonschema::Validator>,
}

impl ProfileTask {
    /// Builds the derived task by resolving secrets from the `bind`.
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
                codes::PROFILE_BIND_INVALID,
                format!(
                    "Profile '{}' bind produces an input that base task '{}' rejects: {}",
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
