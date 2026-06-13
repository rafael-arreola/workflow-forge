//! The root document of the spec: workflow definition and its edges.

use crate::spec::node::{Node, NodeId};
use crate::spec::profile::TaskProfile;
use serde::{Deserialize, Serialize};

/// Complete definition of a workflow ready to be serialized/deserialized.
/// Contains nodes and edges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    /// Version of the spec this definition complies with (e.g., "1.0")
    #[serde(default = "default_spec")]
    pub spec: String,
    /// Unique identifier of the workflow (assigned if not provided)
    #[serde(default)]
    pub id: Option<String>,
    /// Descriptive name of the workflow
    pub name: String,
    /// Semantic version
    pub version: String,
    /// Local task profiles for the workflow: preconfigured instances of
    /// registered tasks, visible only to this definition
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tasks: Vec<TaskProfile>,
    /// Local sub-workflows in the document, referenceable by name from
    /// `kind: "subworkflow"` nodes. They take precedence over the
    /// shared `WorkflowRegistry` and are visible only to this definition
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflows: Vec<WorkflowDefinition>,
    /// Nodes that make up the graph
    pub nodes: Vec<Node>,
    /// Directed edges that define the control flow
    #[serde(default)]
    pub edges: Vec<FlowEdge>,
}

fn default_spec() -> String {
    "1.0".to_string()
}

/// Directed connection between two nodes of the workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowEdge {
    /// Source node ID
    pub from: NodeId,
    /// Target node ID
    pub to: NodeId,
    /// Edge label; connects a gateway branch (`branches[].edge`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Alternative trigger: `error` routes the flow when the source node
    /// exhausts its retries. Without `on`, the edge is from the normal flow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<EdgeTrigger>,
}

/// Alternative edge triggers.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EdgeTrigger {
    /// The edge is followed when the source node definitively fails
    Error,
    /// The edge is followed when the source node's task panics
    /// (bug in the extension). A panic does not retry nor fall into `on: error`.
    Panic,
}
