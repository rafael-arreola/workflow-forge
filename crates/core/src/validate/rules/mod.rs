//! Built-in validation rules, one unit per invariant family.
//!
//! Each rule declares the codes it can emit ([`super::ValidationRule::codes`]):
//! the rule set is, at the same time, the catalog of what is validated.
//! The [`BUILTIN`] order matters: graph rules (`graph`) disable themselves
//! if a previous rule reported broken references.

pub mod conditions;
pub mod edges;
pub mod gateway;
pub mod graph;
pub mod nodes;
pub mod structure;

use super::ValidationRule;

/// Built-in rules, in the order they are executed.
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
    &conditions::KnownConditionOperators { registry: None },
    &edges::EdgeTriggers,
    &graph::Reachability,
    &graph::Acyclicity,
];
