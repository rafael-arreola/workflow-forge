//! Reglas de validación integradas, una unidad por familia de invariante.
//!
//! Cada regla declara los códigos que puede emitir ([`super::ValidationRule::codes`]):
//! el conjunto de reglas es, a la vez, el catálogo de qué se valida.
//! El orden de [`BUILTIN`] importa: las reglas de grafo (`graph`) se
//! auto-desactivan si una regla previa reportó referencias rotas.

pub mod conditions;
pub mod edges;
pub mod gateway;
pub mod graph;
pub mod nodes;
pub mod structure;

use super::ValidationRule;

/// Reglas integradas, en el orden en que se ejecutan.
pub const BUILTIN: &[&dyn ValidationRule] = &[
    &structure::SpecSupported,
    &structure::UniqueNodeIds,
    &structure::EdgeReferences,
    &structure::StartEndPresence,
    &structure::StartEndEdges,
    &nodes::ForeachConcurrency,
    &nodes::LoopMaxIterations,
    &nodes::SubworkflowName,
    &nodes::InlineWorkflowNames,
    &gateway::GatewayCoherence,
    &conditions::KnownConditionOperators,
    &edges::EdgeTriggers,
    &graph::Reachability,
    &graph::Acyclicity,
];
