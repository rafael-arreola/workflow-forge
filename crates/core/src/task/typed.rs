//! Typed and closure-based tasks: the low-friction path for extending.
//!
//! The engine's extension unit is the task ([`Task`]). Implementing the trait
//! by hand (struct + `manifest` + `execute`) is the full path and remains
//! available for cases with state or full context access; this module offers
//! two shortcuts for the common case, where a task is essentially an
//! `input → output` function:
//!
//! - [`FnTask`] / [`register_fn`](crate::task::TaskRegistry::register_fn): an
//!   async closure over raw JSON ([`WorkflowData`]), without schemas. For
//!   trivial transformations.
//! - [`TypedTask`] / [`register_typed`](crate::task::TaskRegistry::register_typed):
//!   an async closure over your own types. The input/output JSON Schemas are
//!   **derived** from the types via `schemars`, so the engine validates them
//!   like any other task and they appear in the catalog without writing them
//!   by hand.
//!
//! Both closures receive a [`TaskCtx`]: a view of per-execution resources
//! (blobs, execution ids) that avoids borrowing the full [`WorkflowContext`]
//! and keeps type inference clean. If a task needs to read the entire state
//! document, implement [`Task`] directly.
//!
//! ```no_run
//! # use workflow_forge_core::task::TaskRegistry;
//! # use serde::{Deserialize, Serialize};
//! # use schemars::JsonSchema;
//! #[derive(Deserialize, JsonSchema)]
//! struct CreateShipmentIn { sku: String, qty: u32 }
//!
//! #[derive(Serialize, JsonSchema)]
//! struct CreateShipmentOut { tracking: String }
//!
//! let registry = TaskRegistry::new();
//! registry.register_typed("acme.create_shipment", |_ctx, input: CreateShipmentIn| async move {
//!     Ok(CreateShipmentOut { tracking: format!("{}-{}", input.sku, input.qty) })
//! });
//! ```

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{WorkflowError, codes};
use crate::io::blob::BlobStore;
use crate::runtime::context::WorkflowContext;
use crate::task::{Task, TaskId, TaskManifest, WorkflowData, WorkflowResult};

/// View of per-execution resources available to a closure-created task
/// ([`FnTask`] / [`TypedTask`]).
///
/// It is a handful of cheap-to-clone handles (blobs are an `Arc`, ids are
/// strings) taken from the [`WorkflowContext`]. It is delivered by value so
/// the closure's future is `'static` and closure inference does not hit
/// lifetime issues. For full access to the execution state document,
/// implement [`Task`] directly and use the `&WorkflowContext` that `execute`
/// receives.
#[derive(Clone)]
pub struct TaskCtx {
    execution_id: String,
    parent_execution_id: Option<String>,
    blobs: Arc<dyn BlobStore>,
}

impl TaskCtx {
    /// Unique identifier of the current execution (UUID v7).
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Parent execution id, if this runs inside a sub-workflow.
    pub fn parent_execution_id(&self) -> Option<&str> {
        self.parent_execution_id.as_deref()
    }

    /// Blob storage for the execution (`$blob` convention).
    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    /// Stable idempotency key derived from `value` (see
    /// [`crate::idempotency`]). The same payload always produces the same
    /// key; pass it to the target system so it deduplicates under retry.
    pub fn idempotency_key(&self, value: &serde_json::Value) -> String {
        crate::idempotency::key_for(value)
    }
}

impl From<&WorkflowContext> for TaskCtx {
    fn from(ctx: &WorkflowContext) -> Self {
        Self {
            execution_id: ctx.execution_id().to_string(),
            parent_execution_id: ctx.parent_execution_id().map(str::to_string),
            blobs: Arc::clone(ctx.blobs()),
        }
    }
}

// ---------------------------------------------------------------------------
// TypedTask: async closure over custom types, derived schemas
// ---------------------------------------------------------------------------

/// Task built from an async closure over custom types.
///
/// The input is deserialized to type `In` before invoking the closure (an
/// input that doesn't fit produces `TASK_INPUT_INVALID`) and the output is
/// serialized from type `Out` (`TASK_OUTPUT_INVALID` if it doesn't serialize).
/// The manifest's JSON Schemas are derived from `In`/`Out` via `schemars`.
/// Normally created via [`register_typed`](crate::task::TaskRegistry::register_typed);
/// use [`TypedTask::new`] directly only if you want to add a description
/// before registering.
pub struct TypedTask<In, Out, F> {
    manifest: TaskManifest,
    f: F,
    _pd: PhantomData<fn(In) -> Out>,
}

impl<In, Out, F, Fut> TypedTask<In, Out, F>
where
    In: DeserializeOwned + JsonSchema + Send + 'static,
    Out: Serialize + JsonSchema + Send + 'static,
    F: Fn(TaskCtx, In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Out, WorkflowError>> + Send + 'static,
{
    /// Creates the task by deriving input/output schemas from the types.
    #[must_use]
    pub fn new(id: impl Into<TaskId>, f: F) -> Self {
        let mut manifest = TaskManifest::new(id);
        manifest.input_schema = Some(schemars::schema_for!(In));
        manifest.output_schema = Some(schemars::schema_for!(Out));
        Self {
            manifest,
            f,
            _pd: PhantomData,
        }
    }

    /// Adds a human-readable description to the manifest.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.manifest.description = Some(description.into());
        self
    }
}

#[async_trait]
impl<In, Out, F, Fut> Task for TypedTask<In, Out, F>
where
    In: DeserializeOwned + JsonSchema + Send + 'static,
    Out: Serialize + JsonSchema + Send + 'static,
    F: Fn(TaskCtx, In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Out, WorkflowError>> + Send + 'static,
{
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let typed: In = serde_json::from_value(input.0).map_err(|e| {
            WorkflowError::new(
                codes::TASK_INPUT_INVALID,
                format!("input does not match the expected type: {e}"),
            )
            .with_source_task(self.manifest.id.0.clone())
        })?;
        let out = (self.f)(TaskCtx::from(ctx), typed).await?;
        let value = serde_json::to_value(out).map_err(|e| {
            WorkflowError::new(
                codes::TASK_OUTPUT_INVALID,
                format!("output is not serializable to JSON: {e}"),
            )
            .with_source_task(self.manifest.id.0.clone())
        })?;
        Ok(WorkflowData(value))
    }
}

// ---------------------------------------------------------------------------
// FnTask: async closure over raw JSON, no schemas
// ---------------------------------------------------------------------------

/// Task built from an async closure over raw JSON ([`WorkflowData`]), without
/// declared schemas.
///
/// The minimal shortcut when a task only manipulates `Value` and doesn't
/// warrant types or a struct. Normally created via
/// [`register_fn`](crate::task::TaskRegistry::register_fn).
pub struct FnTask<F> {
    manifest: TaskManifest,
    f: F,
}

impl<F, Fut> FnTask<F>
where
    F: Fn(TaskCtx, WorkflowData) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = WorkflowResult> + Send + 'static,
{
    /// Creates the task with a minimal manifest (only id, no schemas).
    #[must_use]
    pub fn new(id: impl Into<TaskId>, f: F) -> Self {
        Self {
            manifest: TaskManifest::new(id),
            f,
        }
    }

    /// Adds a human-readable description to the manifest.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.manifest.description = Some(description.into());
        self
    }
}

#[async_trait]
impl<F, Fut> Task for FnTask<F>
where
    F: Fn(TaskCtx, WorkflowData) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = WorkflowResult> + Send + 'static,
{
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        (self.f)(TaskCtx::from(ctx), input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::workflow::WorkflowDefinition;
    use crate::task::TaskRegistry;
    use serde::Deserialize;
    use serde_json::json;

    fn ctx() -> WorkflowContext {
        let workflow: WorkflowDefinition = serde_json::from_value(json!({
            "name": "t",
            "version": "0.1.0",
            "nodes": [],
            "edges": []
        }))
        .unwrap();
        WorkflowContext::new(&workflow, json!({}))
    }

    #[derive(Deserialize, JsonSchema)]
    struct SumIn {
        a: i64,
        b: i64,
    }

    #[derive(Serialize, JsonSchema)]
    struct SumOut {
        total: i64,
    }

    #[tokio::test]
    async fn typed_task_derives_schemas_and_executes() {
        let registry = TaskRegistry::new();
        registry.register_typed("math.sum", |_ctx, input: SumIn| async move {
            Ok(SumOut {
                total: input.a + input.b,
            })
        });

        let task = registry.get(&"math.sum".into()).expect("registered");
        // The manifest carries the schemas derived from the types.
        assert!(task.manifest().input_schema.is_some());
        assert!(task.manifest().output_schema.is_some());

        let out = task
            .execute(&ctx(), WorkflowData(json!({ "a": 2, "b": 3 })))
            .await
            .expect("ok");
        assert_eq!(out.0, json!({ "total": 5 }));
    }

    #[tokio::test]
    async fn typed_task_invalid_input_gives_code() {
        let registry = TaskRegistry::new();
        registry.register_typed("math.sum", |_ctx, input: SumIn| async move {
            Ok(SumOut {
                total: input.a + input.b,
            })
        });
        let task = registry.get(&"math.sum".into()).unwrap();

        let err = task
            .execute(&ctx(), WorkflowData(json!({ "a": "not-a-number" })))
            .await
            .expect_err("must fail");
        assert_eq!(err.code, codes::TASK_INPUT_INVALID);
        assert_eq!(err.source_task.as_deref(), Some("math.sum"));
    }

    #[tokio::test]
    async fn fn_task_over_raw_json() {
        let registry = TaskRegistry::new();
        registry.register_fn("util.echo", |_ctx, input| async move { Ok(input) });
        let task = registry.get(&"util.echo".into()).unwrap();
        // No declared schemas.
        assert!(task.manifest().input_schema.is_none());

        let out = task
            .execute(&ctx(), WorkflowData(json!({ "hello": "world" })))
            .await
            .unwrap();
        assert_eq!(out.0, json!({ "hello": "world" }));
    }
}
