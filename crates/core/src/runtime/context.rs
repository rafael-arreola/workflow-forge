//! The execution context: the shared state document against which mappings
//! and conditions are resolved, plus per-execution resources (blobs, event
//! counter).

use std::sync::Arc;

use parking_lot::RwLock;
use std::time::Instant;

use serde_json::{Value, json};
use uuid::Uuid;

use crate::io::blob::{BlobStore, BlobStoreFactory, TempDirBlobFactory};
use crate::spec::node::NodeId;
use crate::spec::workflow::WorkflowDefinition;

/// Execution context. Holds the state document against which mappings and
/// conditions are resolved:
///
/// ```text
/// $.trigger              → initial workflow input
/// $.nodes.<id>.output    → result of each executed node
/// $.workflow             → metadata (id, name, version, execution_id)
/// ```
///
/// Thread-safe: parallel branches read and write concurrently.
pub struct WorkflowContext {
    /// Unique identifier of the current execution (UUID v7)
    execution_id: String,
    /// Parent execution id, if this execution is a sub-workflow
    parent_execution_id: Option<String>,
    /// Instant when the execution started
    started_at: Instant,
    /// Execution state document
    state: RwLock<Value>,
    /// Blob storage (`$blob`) with execution lifecycle
    /// (sub-workflows share the root execution's store)
    blobs: Arc<dyn BlobStore>,
    /// Observability event counter. Shared between an execution and its
    /// sub-workflows: the total order covers the full execution tree
    event_seq: Arc<std::sync::atomic::AtomicU64>,
}

impl WorkflowContext {
    /// Creates the context for a new execution with its initial state document
    /// and the default [`TempDirBlobFactory`].
    pub fn new(workflow: &WorkflowDefinition, trigger: Value) -> Self {
        Self::with_blob_factory(workflow, trigger, &TempDirBlobFactory)
    }

    /// Like [`WorkflowContext::new`], with an explicit blob factory
    /// (injected via `WorkflowExecutorBuilder::blobs`).
    pub fn with_blob_factory(
        workflow: &WorkflowDefinition,
        trigger: Value,
        factory: &dyn BlobStoreFactory,
    ) -> Self {
        let execution_id = Uuid::now_v7().to_string();
        let state = json!({
            "trigger": trigger,
            "nodes": {},
            "workflow": {
                "id": workflow.id,
                "name": workflow.name,
                "version": workflow.version,
                "execution_id": execution_id,
            }
        });
        let blobs = factory.create(&execution_id);
        Self {
            execution_id,
            parent_execution_id: None,
            started_at: Instant::now(),
            state: RwLock::new(state),
            blobs,
            event_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Context for a child execution (sub-workflow): its own execution_id
    /// (UUID v7) and its own state document, but shares the parent's BlobStore
    /// and event counter. `$blob` references cross the boundary; the parent
    /// cleans up blobs when the root execution finishes.
    pub fn child_of(workflow: &WorkflowDefinition, trigger: Value, parent: &Self) -> Self {
        let execution_id = Uuid::now_v7().to_string();
        let state = json!({
            "trigger": trigger,
            "nodes": {},
            "workflow": {
                "id": workflow.id,
                "name": workflow.name,
                "version": workflow.version,
                "execution_id": execution_id,
                "parent_execution_id": parent.execution_id,
            }
        });
        Self {
            execution_id,
            parent_execution_id: Some(parent.execution_id.clone()),
            started_at: Instant::now(),
            state: RwLock::new(state),
            blobs: Arc::clone(&parent.blobs),
            event_seq: Arc::clone(&parent.event_seq),
        }
    }

    /// Parent execution id, if this execution is a sub-workflow
    pub fn parent_execution_id(&self) -> Option<&str> {
        self.parent_execution_id.as_deref()
    }

    /// Next event sequence number (total order per execution)
    pub fn next_event_seq(&self) -> u64 {
        self.event_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Unique identifier of the execution
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Blob storage for this execution (`$blob` convention)
    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    /// Time elapsed since the execution started
    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }

    /// Reads the state document under the lock, without cloning
    pub fn with_state<R>(&self, f: impl FnOnce(&Value) -> R) -> R {
        let state = self.state.read();
        f(&state)
    }

    /// Full copy of the state document (for debugging/inspection)
    pub fn snapshot(&self) -> Value {
        self.with_state(Clone::clone)
    }

    /// Publishes a node's output at `$.nodes.<id>.output`
    pub fn set_node_output(&self, node_id: &NodeId, output: Value) {
        let mut state = self.state.write();
        state["nodes"][node_id.0.as_str()] = json!({ "output": output });
    }

    /// Output of a previously executed node, if any
    pub fn node_output(&self, node_id: &NodeId) -> Option<Value> {
        self.with_state(|state| state["nodes"][node_id.0.as_str()].get("output").cloned())
    }

    /// Publishes a node's error at `$.nodes.<id>.error` (on_error paths)
    pub fn set_node_error(&self, node_id: &NodeId, error: Value) {
        let mut state = self.state.write();
        state["nodes"][node_id.0.as_str()]["error"] = error;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workflow() -> WorkflowDefinition {
        serde_json::from_value(json!({
            "name": "test",
            "version": "0.1.0",
            "nodes": [],
            "edges": []
        }))
        .unwrap()
    }

    #[test]
    fn initial_document_and_outputs() {
        let ctx = WorkflowContext::new(&workflow(), json!({ "user_id": 7 }));

        ctx.with_state(|state| {
            assert_eq!(state["trigger"]["user_id"], 7);
            assert_eq!(state["workflow"]["name"], "test");
            assert_eq!(
                state["workflow"]["execution_id"],
                ctx.execution_id().to_string().as_str()
            );
        });

        let node = NodeId::from("fetch");
        assert_eq!(ctx.node_output(&node), None);
        ctx.set_node_output(&node, json!({ "status": 200 }));
        assert_eq!(ctx.node_output(&node), Some(json!({ "status": 200 })));
        ctx.with_state(|state| {
            assert_eq!(state["nodes"]["fetch"]["output"]["status"], 200);
        });
    }
}
