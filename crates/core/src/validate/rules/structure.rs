//! Reglas de estructura básica: versión de spec, unicidad de ids,
//! referencias de aristas y presencia/grado de start/end.

use std::collections::HashSet;

use crate::error::{WorkflowError, codes};
use crate::spec::node::{NodeId, NodeKind};
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{SUPPORTED_SPECS, ValidationCtx, ValidationRule};

/// La versión de spec del documento debe estar soportada por este core.
pub struct SpecSupported;

impl ValidationRule for SpecSupported {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::UNSUPPORTED_SPEC]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        _ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        if !SUPPORTED_SPECS.contains(&workflow.spec.as_str()) {
            errors.push(WorkflowError::new(
                codes::UNSUPPORTED_SPEC,
                format!(
                    "Spec '{}' no soportada; este core soporta: {}",
                    workflow.spec,
                    SUPPORTED_SPECS.join(", ")
                ),
            ));
        }
    }
}

/// Ningún id de nodo puede repetirse.
pub struct UniqueNodeIds;

impl ValidationRule for UniqueNodeIds {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::DUPLICATE_NODE_ID]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        _ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        let mut seen: HashSet<&NodeId> = HashSet::new();
        for node in &workflow.nodes {
            if !seen.insert(&node.id) {
                errors.push(
                    WorkflowError::new(
                        codes::DUPLICATE_NODE_ID,
                        format!("El id de nodo '{}' está duplicado", node.id),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// Toda arista debe referenciar nodos que existen.
pub struct EdgeReferences;

impl ValidationRule for EdgeReferences {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::UNKNOWN_NODE_REF]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for edge in &workflow.edges {
            for (end, id) in [("origen", &edge.from), ("destino", &edge.to)] {
                if !ctx.has_node(id) {
                    errors.push(WorkflowError::new(
                        codes::UNKNOWN_NODE_REF,
                        format!(
                            "La arista {}→{} referencia un {} inexistente",
                            edge.from, edge.to, end
                        ),
                    ));
                }
            }
        }
    }
}

/// El workflow debe tener exactamente un `start` y al menos un `end`.
pub struct StartEndPresence;

impl ValidationRule for StartEndPresence {
    fn codes(&self) -> &'static [&'static str] {
        &[
            codes::NO_START_NODE,
            codes::MULTIPLE_START_NODES,
            codes::NO_END_NODE,
        ]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        match ctx.starts().len() {
            0 => errors.push(WorkflowError::new(
                codes::NO_START_NODE,
                "El workflow no tiene nodo start",
            )),
            1 => {}
            _ => errors.push(WorkflowError::new(
                codes::MULTIPLE_START_NODES,
                "El workflow tiene más de un nodo start; la spec 1.0 exige exactamente uno",
            )),
        }
        if !workflow
            .nodes
            .iter()
            .any(|n| matches!(n.kind, NodeKind::End(_)))
        {
            errors.push(WorkflowError::new(
                codes::NO_END_NODE,
                "El workflow no tiene ningún nodo end",
            ));
        }
    }
}

/// Un `start` no admite aristas entrantes; un `end` no admite salientes.
pub struct StartEndEdges;

impl ValidationRule for StartEndEdges {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::START_HAS_INCOMING, codes::END_HAS_OUTGOING]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for node in &workflow.nodes {
            match &node.kind {
                NodeKind::Start(_) if !ctx.incoming(&node.id).is_empty() => {
                    errors.push(
                        WorkflowError::new(
                            codes::START_HAS_INCOMING,
                            format!(
                                "El nodo start '{}' no puede tener aristas entrantes",
                                node.id
                            ),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
                NodeKind::End(_) if !ctx.outgoing(&node.id).is_empty() => {
                    errors.push(
                        WorkflowError::new(
                            codes::END_HAS_OUTGOING,
                            format!("El nodo end '{}' no puede tener aristas salientes", node.id),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
                _ => {}
            }
        }
    }
}
