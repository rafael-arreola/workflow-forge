//! Helpers de prueba: tareas mock para ejercitar workflows sin dependencias
//! vivas.
//!
//! Se habilita con el feature `testing`. La idea es un *dry run*: reemplazar
//! una tarea real (`http.request`, una transferencia SFTP, un perfil de
//! cliente) por una [`MockTask`] en el [`TaskRegistry`](crate::task::TaskRegistry),
//! correr el workflow con el executor real y asertar tanto el output
//! producido como **lo que el workflow habría llamado** — sin red, sin
//! filesystem, sin secretos.
//!
//! Como la tarea es la unidad de trabajo, mockear una basta para recorrer
//! cualquier camino del grafo de forma determinista.
//!
//! ```
//! use std::sync::Arc;
//! use serde_json::json;
//! use workflow_forge_core::runtime::WorkflowExecutor;
//! use workflow_forge_core::task::{TaskRegistry, WorkflowData};
//! use workflow_forge_core::testing::MockTask;
//!
//! # let rt = tokio::runtime::Runtime::new().unwrap();
//! # rt.block_on(async {
//! let workflow = serde_json::from_value(json!({
//!     "name": "dry-run", "version": "0.1.0",
//!     "nodes": [
//!         { "id": "start", "kind": "start" },
//!         { "id": "call",  "kind": "task", "task": "acme.create_order" },
//!         { "id": "end",   "kind": "end" }
//!     ],
//!     "edges": [
//!         { "from": "start", "to": "call" },
//!         { "from": "call",  "to": "end" }
//!     ]
//! })).unwrap();
//!
//! let registry = Arc::new(TaskRegistry::new());
//! let mock = MockTask::returning("acme.create_order", json!({ "order_id": "o-1" }));
//! let calls = mock.call_log();              // toma el handle ANTES de registrar
//! registry.register(mock);
//!
//! let executor = WorkflowExecutor::new(workflow, registry).unwrap();
//! let out = executor.run(WorkflowData(json!({ "sku": "A", "qty": 2 }))).await.unwrap();
//!
//! assert_eq!(out.0, json!({ "order_id": "o-1" }));
//! assert_eq!(calls.count(), 1);
//! assert_eq!(calls.nth(0), Some(json!({ "sku": "A", "qty": 2 })));
//! # });
//! ```

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use crate::error::WorkflowError;
use crate::runtime::context::WorkflowContext;
use crate::task::{Task, TaskId, TaskManifest, WorkflowData, WorkflowResult};

type MockFn = dyn Fn(Value) -> WorkflowResult + Send + Sync;

enum Behavior {
    Returns(Value),
    Fails(WorkflowError),
    Calls(Box<MockFn>),
}

/// Una [`Task`] suplente que registra cada input que recibe y produce un
/// resultado preprogramado. Regístrala bajo el id de la tarea que quieres
/// reemplazar; el executor la trata exactamente igual que a la real.
pub struct MockTask {
    manifest: TaskManifest,
    behavior: Behavior,
    calls: Arc<Mutex<Vec<Value>>>,
}

impl MockTask {
    /// Un mock que siempre devuelve `value`.
    pub fn returning(id: impl Into<TaskId>, value: Value) -> Self {
        Self::with_behavior(id, Behavior::Returns(value))
    }

    /// Un mock que siempre falla con `error` (para ejercitar rutas
    /// `on: "error"`, reintentos, etc.).
    pub fn failing(id: impl Into<TaskId>, error: WorkflowError) -> Self {
        Self::with_behavior(id, Behavior::Fails(error))
    }

    /// Un mock que calcula su resultado a partir del input recibido.
    pub fn with_fn<F>(id: impl Into<TaskId>, f: F) -> Self
    where
        F: Fn(Value) -> WorkflowResult + Send + Sync + 'static,
    {
        Self::with_behavior(id, Behavior::Calls(Box::new(f)))
    }

    fn with_behavior(id: impl Into<TaskId>, behavior: Behavior) -> Self {
        Self {
            manifest: TaskManifest::new(id),
            behavior,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Handle barato y compartible de las llamadas registradas. Clónalo
    /// **antes** de registrar el mock — el registro mueve la tarea al
    /// registry.
    pub fn call_log(&self) -> CallLog {
        CallLog {
            calls: Arc::clone(&self.calls),
        }
    }
}

#[async_trait]
impl Task for MockTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        self.calls
            .lock()
            .expect("MockTask call log poisoned")
            .push(input.0.clone());
        match &self.behavior {
            Behavior::Returns(value) => Ok(WorkflowData(value.clone())),
            Behavior::Fails(error) => Err(error.clone()),
            Behavior::Calls(f) => f(input.0),
        }
    }
}

/// Vista compartible y de solo lectura de los inputs que una [`MockTask`]
/// recibió, en orden de llamada. Se obtiene de [`MockTask::call_log`].
#[derive(Clone)]
pub struct CallLog {
    calls: Arc<Mutex<Vec<Value>>>,
}

impl CallLog {
    /// Cuántas veces fue invocado el mock.
    pub fn count(&self) -> usize {
        self.calls.lock().expect("CallLog poisoned").len()
    }

    /// `true` si el mock fue invocado al menos una vez.
    pub fn called(&self) -> bool {
        self.count() > 0
    }

    /// Copia de todos los inputs recibidos, en orden de llamada.
    pub fn inputs(&self) -> Vec<Value> {
        self.calls.lock().expect("CallLog poisoned").clone()
    }

    /// El input de la n-ésima invocación, si ocurrió.
    pub fn nth(&self, index: usize) -> Option<Value> {
        self.calls
            .lock()
            .expect("CallLog poisoned")
            .get(index)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::WorkflowExecutor;
    use crate::task::TaskRegistry;
    use serde_json::json;

    fn workflow_calling(task: &str) -> crate::spec::WorkflowDefinition {
        serde_json::from_value(json!({
            "name": "t", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "call",  "kind": "task", "task": task },
                { "id": "ok",    "kind": "end" },
                { "id": "ko",    "kind": "end", "status": "error" }
            ],
            "edges": [
                { "from": "start", "to": "call" },
                { "from": "call",  "to": "ok" },
                { "from": "call",  "on": "error", "to": "ko" }
            ]
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn mock_returning_registra_input_y_output() {
        let registry = Arc::new(TaskRegistry::new());
        let mock = MockTask::returning("ext.call", json!({ "ok": true }));
        let calls = mock.call_log();
        registry.register(mock);

        let executor = WorkflowExecutor::new(workflow_calling("ext.call"), registry).unwrap();
        let out = executor.run(WorkflowData(json!({ "x": 1 }))).await.unwrap();

        assert_eq!(out.0, json!({ "ok": true }));
        assert!(calls.called());
        assert_eq!(calls.count(), 1);
        assert_eq!(calls.nth(0), Some(json!({ "x": 1 })));
    }

    #[tokio::test]
    async fn mock_failing_recorre_la_ruta_de_error() {
        let registry = Arc::new(TaskRegistry::new());
        registry.register(MockTask::failing(
            "ext.call",
            WorkflowError::new("BOOM", "fallo simulado"),
        ));

        let executor = WorkflowExecutor::new(workflow_calling("ext.call"), registry).unwrap();
        // El error rutea al nodo end "ko", así que la ejecución completa igual.
        let out = executor.run(WorkflowData(json!({}))).await.unwrap();
        assert_eq!(out.0["code"], json!("BOOM"));
    }

    #[tokio::test]
    async fn mock_with_fn_transforma_el_input() {
        let registry = Arc::new(TaskRegistry::new());
        registry.register(MockTask::with_fn("ext.call", |input| {
            let n = input["n"].as_i64().unwrap_or(0);
            Ok(WorkflowData(json!({ "doubled": n * 2 })))
        }));

        let executor = WorkflowExecutor::new(workflow_calling("ext.call"), registry).unwrap();
        let out = executor
            .run(WorkflowData(json!({ "n": 21 })))
            .await
            .unwrap();
        assert_eq!(out.0, json!({ "doubled": 42 }));
    }
}
