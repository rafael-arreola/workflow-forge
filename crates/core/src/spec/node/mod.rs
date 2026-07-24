//! Graph nodes: the spec 1.0 vocabulary.
//!
//! [`NodeKind`] is a **deliberately closed** enum: the node vocabulary
//! is defined by the spec, not the host. The engine's extensibility goes through
//! tasks ([`crate::task::Task`]), not through new kinds. Adding a kind is
//! a spec change: variant here + handler in `runtime/handlers/` +
//! rules in `validate/rules/` (full checklist in the `lib.rs` doc).

use serde::{Deserialize, Serialize};

pub mod event;
pub mod foreach;
pub mod gateway;
pub mod loop_node;
pub mod task;

/// Unique identifier of a node within the workflow graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        NodeId(s)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        NodeId(s.to_string())
    }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Censorship settings for a node's observability events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SecureConfig {
    /// Full censorship of input, attempt, and output events (`"secure": true`)
    Full(bool),
    /// Censorship restricted to specific JSON key names (`"secure": ["password", "token"]`)
    Fields(Vec<String>),
}

/// Workflow graph node. Contains an identifier, optional security settings,
/// and a behavior variant that is resolved at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier of the node within the workflow
    pub id: NodeId,
    /// Optional censorship configuration for execution events
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secure: Option<SecureConfig>,
    /// Node type: executable task, conditional, loop, parallel, etc.
    #[serde(flatten)]
    pub kind: NodeKind,
}

/// Node classification. The `"kind"` field acts as the discriminator in JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeKind {
    /// Workflow start node
    Start(event::StartNode),
    /// Workflow end node
    End(event::EndNode),
    /// Executable task node
    Task(task::TaskNode),
    /// Node that iterates over an array invoking a task per element
    Foreach(foreach::ForeachNode),
    /// Node that invokes a task repeatedly with a continuation condition
    /// and hard iteration cap
    Loop(loop_node::LoopNode),
    /// Control flow node (exclusive/parallel/join)
    Gateway(gateway::GatewayNode),
    /// Node that executes another workflow as if it were a task
    Subworkflow(SubworkflowNode),
}

impl NodeKind {
    /// Kind name as it appears in the spec JSON (the discriminator).
    pub fn name(&self) -> &'static str {
        match self {
            NodeKind::Start(_) => "start",
            NodeKind::End(_) => "end",
            NodeKind::Task(_) => "task",
            NodeKind::Foreach(_) => "foreach",
            NodeKind::Loop(_) => "loop",
            NodeKind::Gateway(_) => "gateway",
            NodeKind::Subworkflow(_) => "subworkflow",
        }
    }
}

/// Node that executes another workflow: the resolved `input` (or the
/// predecessor's token) becomes the child's trigger and the child's final output
/// is the node's output. All three routable outputs apply: a child
/// failure routes via `on: error` and a panic within the child via `on: panic`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubworkflowNode {
    /// Child workflow name. Resolved first against the document's
    /// `workflows` section and then against the executor's shared
    /// `WorkflowRegistry`.
    pub workflow: String,
    /// Mapping of the child's trigger (`$.` rules from the inputs);
    /// without `input`, the child receives the predecessor's output
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_node_deserialization() {
        let json_full = serde_json::json!({
            "id": "node1",
            "secure": true,
            "kind": "start"
        });
        let node: Node = serde_json::from_value(json_full).unwrap();
        assert_eq!(node.secure, Some(SecureConfig::Full(true)));

        let json_fields = serde_json::json!({
            "id": "node2",
            "secure": ["password", "token"],
            "kind": "start"
        });
        let node2: Node = serde_json::from_value(json_fields).unwrap();
        assert_eq!(
            node2.secure,
            Some(SecureConfig::Fields(vec![
                "password".to_string(),
                "token".to_string()
            ]))
        );
    }
}

