//! Extensión `util` de workflow-forge: tareas de soporte para pruebas,
//! ejemplos y debugging de workflows.
//!
//! | Tarea | Contrato |
//! |-------|----------|
//! | `util.noop` | Devuelve su input tal cual |
//! | `util.log` | Loggea `message` con `level` y devuelve `value` (o null) |
//! | `util.delay` | Espera `ms` milisegundos y devuelve `value` (o null) |
//! | `util.idempotency_key` | Clave estable derivada de `value` → `{ key }` |

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::idempotency;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Registra todas las tareas de la extensión en el registry
pub fn register(registry: &TaskRegistry) {
    registry.register(NoopTask::default());
    registry.register(LogTask::default());
    registry.register(DelayTask::default());
    registry.register(IdempotencyKeyTask::default());
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("schema estático válido")
}

// ---------------------------------------------------------------------------
// util.noop
// ---------------------------------------------------------------------------

pub struct NoopTask {
    manifest: TaskManifest,
}

impl Default for NoopTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("util.noop");
        manifest.description = Some("Devuelve su input sin modificarlo".into());
        Self { manifest }
    }
}

#[async_trait]
impl Task for NoopTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        Ok(input)
    }
}

// ---------------------------------------------------------------------------
// util.log
// ---------------------------------------------------------------------------

pub struct LogTask {
    manifest: TaskManifest,
}

impl Default for LogTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("util.log");
        manifest.description =
            Some("Loggea `message` con el nivel indicado y devuelve `value`".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["message"],
            "properties": {
                "level": { "enum": ["debug", "info", "warn", "error"], "default": "info" },
                "message": { "description": "Valor a loggear (cualquier JSON)" },
                "value": { "description": "Token que la tarea devuelve como output" }
            }
        })));
        Self { manifest }
    }
}

#[async_trait]
impl Task for LogTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let message = input.get("message").cloned().unwrap_or(Value::Null);
        let rendered = match &message {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let execution_id = ctx.execution_id();
        match input.get("level").and_then(Value::as_str).unwrap_or("info") {
            "debug" => tracing::debug!(execution_id, "{rendered}"),
            "warn" => tracing::warn!(execution_id, "{rendered}"),
            "error" => tracing::error!(execution_id, "{rendered}"),
            _ => tracing::info!(execution_id, "{rendered}"),
        }
        Ok(WorkflowData(
            input.get("value").cloned().unwrap_or(Value::Null),
        ))
    }
}

// ---------------------------------------------------------------------------
// util.delay
// ---------------------------------------------------------------------------

pub struct DelayTask {
    manifest: TaskManifest,
}

impl Default for DelayTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("util.delay");
        manifest.description =
            Some("Espera `ms` milisegundos y devuelve `value` como output".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["ms"],
            "properties": {
                "ms": { "type": "integer", "minimum": 0 },
                "value": { "description": "Token que la tarea devuelve como output" }
            }
        })));
        Self { manifest }
    }
}

#[async_trait]
impl Task for DelayTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let ms = input.get("ms").and_then(Value::as_u64).unwrap_or(0);
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        Ok(WorkflowData(
            input.get("value").cloned().unwrap_or(Value::Null),
        ))
    }
}

// ---------------------------------------------------------------------------
// util.idempotency_key
// ---------------------------------------------------------------------------

pub struct IdempotencyKeyTask {
    manifest: TaskManifest,
}

impl Default for IdempotencyKeyTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("util.idempotency_key");
        manifest.description = Some(
            "Deriva una clave de idempotencia estable de `value` y la devuelve como `{ key }`"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["value"],
            "properties": {
                "value": { "description": "Payload del que derivar la clave (cualquier JSON)" }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["key"],
            "properties": { "key": { "type": "string" } }
        })));
        Self { manifest }
    }
}

#[async_trait]
impl Task for IdempotencyKeyTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let value = input.get("value").cloned().unwrap_or(Value::Null);
        Ok(WorkflowData(json!({ "key": idempotency::key_for(&value) })))
    }
}
