use std::collections::HashMap;

use crate::spec::node::{Node, NodeId, NodeKind};
use crate::spec::workflow::{FlowEdge, WorkflowDefinition};

/// Índices precalculados sobre la definición, compartidos por todas las
/// reglas de validación: evita que cada regla reconstruya los mapas de
/// adyacencia.
///
/// Los índices se construyen tal cual está el documento, sin asumir que es
/// válido: una arista puede referenciar nodos inexistentes y aun así
/// aparece en `outgoing`/`incoming` (la regla de referencias la reporta).
pub struct ValidationCtx<'w> {
    nodes: HashMap<&'w NodeId, &'w Node>,
    outgoing: HashMap<&'w NodeId, Vec<&'w FlowEdge>>,
    incoming: HashMap<&'w NodeId, Vec<&'w FlowEdge>>,
    starts: Vec<&'w Node>,
}

impl<'w> ValidationCtx<'w> {
    /// Construye los índices de un documento.
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

    /// `true` si existe un nodo con ese id
    pub fn has_node(&self, id: &NodeId) -> bool {
        self.nodes.contains_key(id)
    }

    /// Cantidad de nodos del documento
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Ids de todos los nodos
    pub fn node_ids(&self) -> impl Iterator<Item = &'w NodeId> + '_ {
        self.nodes.keys().copied()
    }

    /// Aristas salientes de un nodo (flujo normal y de error, sin filtrar)
    pub fn outgoing(&self, id: &NodeId) -> &[&'w FlowEdge] {
        self.outgoing.get(id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Aristas entrantes de un nodo (flujo normal y de error, sin filtrar)
    pub fn incoming(&self, id: &NodeId) -> &[&'w FlowEdge] {
        self.incoming.get(id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Nodos `start` declarados (la spec exige exactamente uno)
    pub fn starts(&self) -> &[&'w Node] {
        &self.starts
    }
}
