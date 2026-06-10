use std::collections::HashMap;

use crate::node::{Node, NodeId, NodeKind};
use crate::workflow::{EdgeTrigger, FlowEdge, WorkflowDefinition};

/// Índices precalculados del grafo para búsquedas rápidas.
pub(crate) struct GraphIndex {
    nodes: HashMap<NodeId, Node>,
    /// Aristas salientes del flujo normal (sin `on: error`)
    outgoing: HashMap<NodeId, Vec<FlowEdge>>,
    /// Aristas salientes de error (`on: error`)
    outgoing_error: HashMap<NodeId, Vec<FlowEdge>>,
    /// Cantidad de aristas entrantes del flujo normal por nodo
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
        let mut incoming_count: HashMap<NodeId, usize> = HashMap::new();

        for edge in &workflow.edges {
            match edge.on {
                Some(EdgeTrigger::Error) => {
                    outgoing_error
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
            .expect("validación garantiza un start");

        Self {
            nodes,
            outgoing,
            outgoing_error,
            incoming_count,
            start,
        }
    }

    pub(crate) fn node(&self, id: &NodeId) -> &Node {
        self.nodes
            .get(id)
            .expect("validación garantiza referencias")
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
}
