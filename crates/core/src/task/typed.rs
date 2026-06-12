//! Tareas tipadas y por closure: el camino de baja fricción para extender.
//!
//! La unidad de extensión del engine es la tarea ([`Task`]). Implementar el
//! trait a mano (struct + `manifest` + `execute`) es el camino completo y
//! sigue disponible para casos con estado o acceso total al contexto; este
//! módulo ofrece dos atajos para el caso común, donde una tarea es
//! esencialmente una función `entrada → salida`:
//!
//! - [`FnTask`] / [`TaskRegistry::register_fn`]: una closure async sobre JSON
//!   crudo ([`WorkflowData`]), sin schemas. Para transformaciones triviales.
//! - [`TypedTask`] / [`TaskRegistry::register_typed`]: una closure async sobre
//!   tus propios tipos. Los JSON Schema de input/output se **derivan** de los
//!   tipos vía `schemars`, de modo que el engine los valida como con cualquier
//!   otra tarea y aparecen en el catálogo sin escribirlos a mano.
//!
//! Ambas closures reciben un [`TaskCtx`]: una vista de los recursos por
//! ejecución (blobs, ids de ejecución) que evita tomar prestado el
//! [`WorkflowContext`] completo y mantiene la inferencia de tipos limpia. Si
//! una tarea necesita leer el documento de estado entero, implementa [`Task`]
//! directamente.
//!
//! ```no_run
//! # use workflow_forge_core::task::TaskRegistry;
//! # use serde::{Deserialize, Serialize};
//! # use schemars::JsonSchema;
//! #[derive(Deserialize, JsonSchema)]
//! struct CrearEnvioIn { sku: String, qty: u32 }
//!
//! #[derive(Serialize, JsonSchema)]
//! struct CrearEnvioOut { tracking: String }
//!
//! let registry = TaskRegistry::new();
//! registry.register_typed("acme.crear_envio", |_ctx, input: CrearEnvioIn| async move {
//!     Ok(CrearEnvioOut { tracking: format!("{}-{}", input.sku, input.qty) })
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

/// Vista de los recursos por ejecución disponibles para una tarea creada por
/// closure ([`FnTask`] / [`TypedTask`]).
///
/// Es un puñado de handles baratos de clonar (los blobs son un `Arc`, los ids
/// son cadenas) tomados del [`WorkflowContext`]. Se entrega por valor para que
/// el futuro de la closure sea `'static` y la inferencia de la closure no tope
/// con lifetimes. Para acceso completo al documento de estado de la ejecución,
/// implementa [`Task`] directamente y usa el `&WorkflowContext` que recibe
/// `execute`.
#[derive(Clone)]
pub struct TaskCtx {
    execution_id: String,
    parent_execution_id: Option<String>,
    blobs: Arc<dyn BlobStore>,
}

impl TaskCtx {
    /// Identificador único de la ejecución actual (UUID v7).
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Id de la ejecución padre, si esta corre dentro de un sub-workflow.
    pub fn parent_execution_id(&self) -> Option<&str> {
        self.parent_execution_id.as_deref()
    }

    /// Almacenamiento de blobs de la ejecución (convención `$blob`).
    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    /// Clave de idempotencia estable derivada de `value` (ver
    /// [`crate::idempotency`]). El mismo payload siempre produce la misma
    /// clave; pásala al sistema destino para que deduplique bajo reintento.
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
// TypedTask: closure async sobre tipos propios, schemas derivados
// ---------------------------------------------------------------------------

/// Tarea construida a partir de una closure async sobre tipos propios.
///
/// El input se deserializa al tipo `In` antes de invocar la closure (un input
/// que no encaja produce `TASK_INPUT_INVALID`) y el output se serializa desde
/// el tipo `Out` (`TASK_OUTPUT_INVALID` si no serializa). Los JSON Schema del
/// manifiesto se derivan de `In`/`Out` con `schemars`. Normalmente se crea vía
/// [`TaskRegistry::register_typed`]; usa [`TypedTask::new`] directamente solo
/// si quieres añadir una descripción antes de registrar.
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
    /// Crea la tarea derivando los schemas de input/output de los tipos.
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

    /// Añade la descripción legible del manifiesto.
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
                format!("el input no coincide con el tipo esperado: {e}"),
            )
            .with_source_task(self.manifest.id.0.clone())
        })?;
        let out = (self.f)(TaskCtx::from(ctx), typed).await?;
        let value = serde_json::to_value(out).map_err(|e| {
            WorkflowError::new(
                codes::TASK_OUTPUT_INVALID,
                format!("el output no es serializable a JSON: {e}"),
            )
            .with_source_task(self.manifest.id.0.clone())
        })?;
        Ok(WorkflowData(value))
    }
}

// ---------------------------------------------------------------------------
// FnTask: closure async sobre JSON crudo, sin schemas
// ---------------------------------------------------------------------------

/// Tarea construida a partir de una closure async sobre JSON crudo
/// ([`WorkflowData`]), sin schemas declarados.
///
/// El atajo mínimo cuando una tarea solo manipula `Value` y no amerita tipos
/// ni un struct. Normalmente se crea vía [`TaskRegistry::register_fn`].
pub struct FnTask<F> {
    manifest: TaskManifest,
    f: F,
}

impl<F, Fut> FnTask<F>
where
    F: Fn(TaskCtx, WorkflowData) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = WorkflowResult> + Send + 'static,
{
    /// Crea la tarea con un manifiesto mínimo (solo id, sin schemas).
    pub fn new(id: impl Into<TaskId>, f: F) -> Self {
        Self {
            manifest: TaskManifest::new(id),
            f,
        }
    }

    /// Añade la descripción legible del manifiesto.
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
    async fn typed_task_deriva_schemas_y_ejecuta() {
        let registry = TaskRegistry::new();
        registry.register_typed("math.sum", |_ctx, input: SumIn| async move {
            Ok(SumOut {
                total: input.a + input.b,
            })
        });

        let task = registry.get(&"math.sum".into()).expect("registrada");
        // El manifiesto trae los schemas derivados de los tipos.
        assert!(task.manifest().input_schema.is_some());
        assert!(task.manifest().output_schema.is_some());

        let out = task
            .execute(&ctx(), WorkflowData(json!({ "a": 2, "b": 3 })))
            .await
            .expect("ok");
        assert_eq!(out.0, json!({ "total": 5 }));
    }

    #[tokio::test]
    async fn typed_task_input_invalido_da_codigo() {
        let registry = TaskRegistry::new();
        registry.register_typed("math.sum", |_ctx, input: SumIn| async move {
            Ok(SumOut {
                total: input.a + input.b,
            })
        });
        let task = registry.get(&"math.sum".into()).unwrap();

        let err = task
            .execute(&ctx(), WorkflowData(json!({ "a": "no-es-numero" })))
            .await
            .expect_err("debe fallar");
        assert_eq!(err.code, codes::TASK_INPUT_INVALID);
        assert_eq!(err.source_task.as_deref(), Some("math.sum"));
    }

    #[tokio::test]
    async fn fn_task_sobre_json_crudo() {
        let registry = TaskRegistry::new();
        registry.register_fn("util.echo", |_ctx, input| async move { Ok(input) });
        let task = registry.get(&"util.echo".into()).unwrap();
        // Sin schemas declarados.
        assert!(task.manifest().input_schema.is_none());

        let out = task
            .execute(&ctx(), WorkflowData(json!({ "hola": "mundo" })))
            .await
            .unwrap();
        assert_eq!(out.0, json!({ "hola": "mundo" }));
    }
}
