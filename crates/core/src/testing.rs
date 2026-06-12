//! Test helpers: mock tasks for exercising workflows without live dependencies.
//!
//! Enabled by the `testing` feature. The idea is a *dry run*: replace a real
//! task (`http.request`, an SFTP transfer, a client profile) with a
//! [`MockTask`] in the [`TaskRegistry`], run the workflow with the real
//! executor, and assert both the produced output and **what the workflow would
//! have called** — no network, no filesystem, no secrets.
//!
//! Because tasks are the unit of work, mocking one is enough to drive any path
//! through the graph deterministically.
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
//! let calls = mock.call_log();              // grab the handle BEFORE registering
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

/// A stand-in [`Task`] that records every input it receives and produces a
/// preprogrammed result. Register it under the id of the task you want to
/// replace; the executor treats it exactly like the real one.
pub struct MockTask {
    manifest: TaskManifest,
    behavior: Behavior,
    calls: Arc<Mutex<Vec<Value>>>,
}

impl MockTask {
    /// A mock that always returns `value`.
    pub fn returning(id: impl Into<TaskId>, value: Value) -> Self {
        Self::with_behavior(id, Behavior::Returns(value))
    }

    /// A mock that always fails with `error` (drives `on: "error"` routes,
    /// retries, etc.).
    pub fn failing(id: impl Into<TaskId>, error: WorkflowError) -> Self {
        Self::with_behavior(id, Behavior::Fails(error))
    }

    /// A mock that computes its result from the received input.
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

    /// A cheap, shareable handle to the recorded calls. Clone it **before**
    /// registering the mock — registration moves the task into the registry.
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

/// A shareable, read-only view of the inputs a [`MockTask`] has received,
/// in call order. Obtained from [`MockTask::call_log`].
#[derive(Clone)]
pub struct CallLog {
    calls: Arc<Mutex<Vec<Value>>>,
}

impl CallLog {
    /// How many times the mock was invoked.
    pub fn count(&self) -> usize {
        self.calls.lock().expect("CallLog poisoned").len()
    }

    /// Whether the mock was invoked at least once.
    pub fn called(&self) -> bool {
        self.count() > 0
    }

    /// A snapshot of every input received, in call order.
    pub fn inputs(&self) -> Vec<Value> {
        self.calls.lock().expect("CallLog poisoned").clone()
    }

    /// The input of the n-th invocation, if it happened.
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
    use crate::error::codes;
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
    async fn returning_mock_records_input_and_output() {
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
    async fn failing_mock_drives_error_route() {
        let registry = Arc::new(TaskRegistry::new());
        registry.register(MockTask::failing(
            "ext.call",
            WorkflowError::new("BOOM", "simulated failure"),
        ));

        let executor = WorkflowExecutor::new(workflow_calling("ext.call"), registry).unwrap();
        // The error routes to the "ko" end node, so the run still completes.
        let out = executor.run(WorkflowData(json!({}))).await.unwrap();
        assert_eq!(out.0["code"], json!("BOOM"));
    }

    #[tokio::test]
    async fn with_fn_mock_transforms_input() {
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
        // sanity: code constant import is used so the module compiles cleanly
        let _ = codes::TASK_PANIC;
    }
}
