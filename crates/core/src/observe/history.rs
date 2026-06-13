//! Built-in in-memory observer and the report it produces.

use parking_lot::Mutex;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::WorkflowError;
use crate::observe::{EventKind, ExecutionEvent, ExecutionObserver};

/// Built-in observer: accumulates execution events in memory.
#[derive(Default)]
pub struct InMemoryHistory {
    events: Mutex<Vec<ExecutionEvent>>,
}

impl ExecutionObserver for InMemoryHistory {
    fn on_event(&self, event: &ExecutionEvent) {
        self.events.lock().push(event.clone());
    }
}

impl InMemoryHistory {
    /// Creates an empty history
    pub fn new() -> Self {
        Self::default()
    }

    /// Copy of accumulated events, sorted by `seq`
    pub fn events(&self) -> Vec<ExecutionEvent> {
        let mut events = self.events.lock().clone();
        events.sort_by_key(|e| e.seq);
        events
    }

    /// Summary of the root execution per node, built from events.
    /// Sub-workflow events are not folded here: the parent's subworkflow node
    /// summarizes them; use [`InMemoryHistory::report_for`] for the detail of
    /// a child execution.
    pub fn report(&self) -> ExecutionReport {
        let events = self.events();
        match events.first().map(|e| e.execution_id.clone()) {
            Some(root) => ExecutionReport::from_events(
                &events
                    .into_iter()
                    .filter(|e| e.execution_id == root)
                    .collect::<Vec<_>>(),
            ),
            None => ExecutionReport::from_events(&events),
        }
    }

    /// Summary of a specific execution (root or sub-workflow)
    pub fn report_for(&self, execution_id: &str) -> ExecutionReport {
        ExecutionReport::from_events(
            &self
                .events()
                .into_iter()
                .filter(|e| e.execution_id == execution_id)
                .collect::<Vec<_>>(),
        )
    }

    /// Ids of the executions present in the history, in appearance order
    /// (the root first, then each sub-workflow as it started)
    pub fn executions(&self) -> Vec<String> {
        let mut seen = Vec::new();
        for event in self.events() {
            if !seen.contains(&event.execution_id) {
                seen.push(event.execution_id.clone());
            }
        }
        seen
    }
}

/// Terminal (or not) state of an execution based on its events.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    /// No terminal event yet
    Running,
    /// Completed successfully
    Completed,
    /// Completed with error
    Failed,
}

/// State of a node based on its events.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Started and has no terminal event
    Running,
    /// Completed successfully
    Completed,
    /// Failed definitively
    Failed,
    /// Failed but the flow continued through an `on: error` edge
    ErrorRouted,
}

/// Serializable summary of an execution: global status + per-node timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    /// Id of the summarized execution
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    /// Workflow metadata (id, name, version)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<Value>,
    /// Global execution status
    pub status: ExecutionStatus,
    /// Total duration in milliseconds, if finished
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Final output, if completed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Final error, if failed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<WorkflowError>,
    /// Nodes in first-start order
    pub nodes: Vec<NodeReport>,
}

/// Summary of a node within the report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReport {
    /// Node id
    pub node_id: String,
    /// Node kind as it appears in the spec (`task`, `gateway`, ...)
    pub kind: String,
    /// Node state based on its events
    pub status: NodeStatus,
    /// Recorded task attempts (0 for non-task/foreach nodes)
    pub attempts: u32,
    /// Duration in milliseconds, if finished
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Node output, if completed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Node error, if failed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<WorkflowError>,
    /// Successful elements/iterations (foreach and loop only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_ok: Option<usize>,
    /// Failed elements/iterations (foreach and loop only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_failed: Option<usize>,
}

impl ExecutionReport {
    /// Builds the report by folding events (they must be ordered by seq)
    pub fn from_events(events: &[ExecutionEvent]) -> Self {
        let mut report = ExecutionReport {
            execution_id: None,
            workflow: None,
            status: ExecutionStatus::Running,
            duration_ms: None,
            output: None,
            error: None,
            nodes: Vec::new(),
        };
        let mut index: HashMap<String, usize> = HashMap::new();

        for event in events {
            report
                .execution_id
                .get_or_insert_with(|| event.execution_id.clone());

            if let Some(node_id) = event.kind.node_id() {
                let i = *index.entry(node_id.to_string()).or_insert_with(|| {
                    report.nodes.push(NodeReport {
                        node_id: node_id.to_string(),
                        kind: String::new(),
                        status: NodeStatus::Running,
                        attempts: 0,
                        duration_ms: None,
                        output: None,
                        error: None,
                        items_ok: None,
                        items_failed: None,
                    });
                    report.nodes.len() - 1
                });
                let node = &mut report.nodes[i];
                match &event.kind {
                    EventKind::NodeStarted { kind, .. } => {
                        node.kind = kind.clone();
                    }
                    EventKind::TaskAttemptStarted { attempt, .. } => {
                        node.attempts = node.attempts.max(*attempt);
                    }
                    EventKind::NodeCompleted {
                        output,
                        duration_ms,
                        ..
                    } => {
                        node.status = NodeStatus::Completed;
                        node.output = Some(output.clone());
                        node.duration_ms = Some(*duration_ms);
                    }
                    EventKind::NodeFailed {
                        error,
                        error_routed,
                        ..
                    } => {
                        node.status = if *error_routed {
                            NodeStatus::ErrorRouted
                        } else {
                            NodeStatus::Failed
                        };
                        node.error = Some(error.clone());
                    }
                    EventKind::ForeachItemCompleted { .. }
                    | EventKind::LoopIterationCompleted { .. } => {
                        *node.items_ok.get_or_insert(0) += 1;
                        node.items_failed.get_or_insert(0);
                    }
                    EventKind::ForeachItemFailed { .. } | EventKind::LoopIterationFailed { .. } => {
                        *node.items_failed.get_or_insert(0) += 1;
                        node.items_ok.get_or_insert(0);
                    }
                    _ => {}
                }
            } else {
                match &event.kind {
                    EventKind::WorkflowStarted { workflow, .. } => {
                        report.workflow = Some(workflow.clone());
                    }
                    EventKind::WorkflowCompleted {
                        output,
                        duration_ms,
                    } => {
                        report.status = ExecutionStatus::Completed;
                        report.output = Some(output.clone());
                        report.duration_ms = Some(*duration_ms);
                    }
                    EventKind::WorkflowFailed { error, duration_ms } => {
                        report.status = ExecutionStatus::Failed;
                        report.error = Some(error.clone());
                        report.duration_ms = Some(*duration_ms);
                    }
                    _ => {}
                }
            }
        }
        report
    }
}
