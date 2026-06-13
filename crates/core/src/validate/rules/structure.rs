//! Basic structure rules: spec version, id uniqueness, edge references,
//! and start/end presence and degree.

use std::collections::HashSet;

use crate::error::{WorkflowError, codes};
use crate::spec::node::{NodeId, NodeKind};
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{SUPPORTED_SPECS, ValidationCtx, ValidationRule};

/// The document's spec version must be supported by this core.
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
                    "Spec '{}' not supported; this core supports: {}",
                    workflow.spec,
                    SUPPORTED_SPECS.join(", ")
                ),
            ));
        }
    }
}

/// No node id may be repeated.
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
                        format!("Node id '{}' is duplicated", node.id),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// Every edge must reference nodes that exist.
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
            for (end, id) in [("source", &edge.from), ("target", &edge.to)] {
                if !ctx.has_node(id) {
                    errors.push(WorkflowError::new(
                        codes::UNKNOWN_NODE_REF,
                        format!(
                            "Edge {}→{} references a nonexistent {}",
                            edge.from, edge.to, end
                        ),
                    ));
                }
            }
        }
    }
}

/// The workflow must have exactly one `start` and at least one `end`.
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
                "The workflow has no start node",
            )),
            1 => {}
            _ => errors.push(WorkflowError::new(
                codes::MULTIPLE_START_NODES,
                "The workflow has more than one start node; spec 1.0 requires exactly one",
            )),
        }
        if !workflow
            .nodes
            .iter()
            .any(|n| matches!(n.kind, NodeKind::End(_)))
        {
            errors.push(WorkflowError::new(
                codes::NO_END_NODE,
                "The workflow has no end node",
            ));
        }
    }
}

/// A `start` does not accept incoming edges; an `end` does not accept
/// outgoing ones.
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
                                "Start node '{}' cannot have incoming edges",
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
                            format!("End node '{}' cannot have outgoing edges", node.id),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
                _ => {}
            }
        }
    }
}
