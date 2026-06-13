use std::collections::HashMap;

use crate::spec::node::{Node, NodeId, NodeKind};
use crate::spec::workflow::{EdgeTrigger, FlowEdge, WorkflowDefinition};

/// Precomputed graph indices for fast lookups.
pub(crate) struct GraphIndex {
    nodes: HashMap<NodeId, Node>,
    /// Outgoing edges in the normal flow (without `on: error`)
    outgoing: HashMap<NodeId, Vec<FlowEdge>>,
    /// Error outgoing edges (`on: error`)
    outgoing_error: HashMap<NodeId, Vec<FlowEdge>>,
    /// Panic outgoing edges (`on: panic`)
    outgoing_panic: HashMap<NodeId, Vec<FlowEdge>>,
    /// Number of incoming edges in the normal flow per node
    pub(crate) incoming_count: HashMap<NodeId, usize>,
    pub(crate) start: NodeId,
}

impl GraphIndex {
    pub(crate) fn build(workflow: &WorkflowDefinition) -> Self {
        let nodes: HashMap<NodeId, Node> = workflow
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.clone()))
            .collect();

        let mut outgoing: HashMap<NodeId, Vec<FlowEdge>> = HashMap::new();
        let mut outgoing_error: HashMap<NodeId, Vec<FlowEdge>> = HashMap::new();
        let mut outgoing_panic: HashMap<NodeId, Vec<FlowEdge>> = HashMap::new();
        let mut incoming_count: HashMap<NodeId, usize> = HashMap::new();

        for edge in &workflow.edges {
            match edge.on {
                Some(EdgeTrigger::Error) => {
                    outgoing_error
                        .entry(edge.from.clone())
                        .or_default()
                        .push(edge.clone());
                }
                Some(EdgeTrigger::Panic) => {
                    outgoing_panic
                        .entry(edge.from.clone())
                        .or_default()
                        .push(edge.clone());
                }
                None => {
                    outgoing
                        .entry(edge.from.clone())
                        .or_default()
                        .push(edge.clone());
                    *incoming_count.entry(edge.to.clone()).or_default() += 1;
                }
            }
        }

        let start = workflow
            .nodes
            .iter()
            .find(|n| matches!(n.kind, NodeKind::Start(_)))
            .map(|n| n.id.clone())
            .expect("validation guarantees a single start node");

        Self {
            nodes,
            outgoing,
            outgoing_error,
            outgoing_panic,
            incoming_count,
            start,
        }
    }

    pub(crate) fn node(&self, id: &NodeId) -> &Node {
        self.nodes
            .get(id)
            .expect("validation guarantees references")
    }

    pub(crate) fn outgoing_edges(&self, id: &NodeId) -> &[FlowEdge] {
        self.outgoing.get(id).map(Vec::as_slice).unwrap_or(&[])
    }

    pub(crate) fn error_edges(&self, id: &NodeId) -> &[FlowEdge] {
        self.outgoing_error
            .get(id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn panic_edges(&self, id: &NodeId) -> &[FlowEdge] {
        self.outgoing_panic
            .get(id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}
