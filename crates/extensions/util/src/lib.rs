//! workflow-forge `util` extension: support tasks for testing,
//! examples, and workflow debugging.
//!
//! | Task | Contract |
//! |-------|----------|
//! | `util.noop` | Returns its input unchanged |
//! | `util.log` | Logs `message` with `level` and returns `value` (or null) |
//! | `util.delay` | Waits `ms` milliseconds and returns `value` (or null) |
//! | `util.idempotency_key` | Stable key derived from `value` → `{ key }` |

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::idempotency;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Registers all extension tasks in the registry
pub fn register(registry: &TaskRegistry) {
    registry.register(NoopTask::default());
    registry.register(LogTask::default());
    registry.register(DelayTask::default());
    registry.register(IdempotencyKeyTask::default());
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("valid static schema")
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
        manifest.description = Some("Returns its input unchanged".into());
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
            Some("Logs `message` at the given level and returns `value`".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["message"],
            "properties": {
                "level": { "enum": ["debug", "info", "warn", "error"], "default": "info" },
                "message": { "description": "Value to log (any JSON)" },
                "value": { "description": "Token the task returns as output" }
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
            Some("Waits `ms` milliseconds and returns `value` as output".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["ms"],
            "properties": {
                "ms": { "type": "integer", "minimum": 0 },
                "value": { "description": "Token the task returns as output" }
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
            "Derives a stable idempotency key from `value` and returns it as `{ key }`"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["value"],
            "properties": {
                "value": { "description": "Payload to derive the key from (any JSON)" }
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
