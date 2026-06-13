use std::collections::HashMap;

use crate::spec::node::{Node, NodeId, NodeKind};
use crate::spec::workflow::{FlowEdge, WorkflowDefinition};

/// Precomputed indices on the definition, shared by all validation rules:
/// avoids each rule rebuilding the adjacency maps.
///
/// The indices are built from the document as-is, without assuming it is
/// valid: an edge may reference nonexistent nodes and still appear in
/// `outgoing`/`incoming` (the reference rule reports it).
pub struct ValidationCtx<'w> {
    nodes: HashMap<&'w NodeId, &'w Node>,
    outgoing: HashMap<&'w NodeId, Vec<&'w FlowEdge>>,
    incoming: HashMap<&'w NodeId, Vec<&'w FlowEdge>>,
    starts: Vec<&'w Node>,
}

impl<'w> ValidationCtx<'w> {
    /// Builds the indices for a document.
    pub fn build(workflow: &'w WorkflowDefinition) -> Self {
        let nodes: HashMap<&NodeId, &Node> = workflow.nodes.iter().map(|n| (&n.id, n)).collect();

        let mut outgoing: HashMap<&NodeId, Vec<&FlowEdge>> = HashMap::new();
        let mut incoming: HashMap<&NodeId, Vec<&FlowEdge>> = HashMap::new();
        for edge in &workflow.edges {
            outgoing.entry(&edge.from).or_default().push(edge);
            incoming.entry(&edge.to).or_default().push(edge);
        }

        let starts: Vec<&Node> = workflow
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::Start(_)))
            .collect();

        Self {
            nodes,
            outgoing,
            incoming,
            starts,
        }
    }

    /// `true` if a node with that id exists
    pub fn has_node(&self, id: &NodeId) -> bool {
        self.nodes.contains_key(id)
    }

    /// Number of nodes in the document
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Ids of all nodes
    pub fn node_ids(&self) -> impl Iterator<Item = &'w NodeId> + '_ {
        self.nodes.keys().copied()
    }

    /// Outgoing edges of a node (normal and error flow, unfiltered)
    pub fn outgoing(&self, id: &NodeId) -> &[&'w FlowEdge] {
        self.outgoing.get(id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Incoming edges of a node (normal and error flow, unfiltered)
    pub fn incoming(&self, id: &NodeId) -> &[&'w FlowEdge] {
        self.incoming.get(id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Declared `start` nodes (the spec requires exactly one)
    pub fn starts(&self) -> &[&'w Node] {
        &self.starts
    }
}
