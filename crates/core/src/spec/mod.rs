//! The **spec** family: the workflow definition language.
//!
//! Contains exclusively the data types that serialize/deserialize
//! a workflow document (spec 1.0): the graph ([`WorkflowDefinition`],
//! [`FlowEdge`]), the nodes ([`Node`], [`NodeKind`] and its variants), the
//! declarative conditions ([`Condition`]), and the task profiles
//! ([`TaskProfile`]).
//!
//! Family rule: **no execution or validation logic lives here**,
//! only the shape of the language. The semantics live in `validate` (static
//! rules), `expr` (expression resolution), and `runtime` (execution).

pub mod condition;
pub mod node;
pub mod profile;
pub mod workflow;

pub use condition::{CompareOp, Comparison, Condition};
pub use node::{Node, NodeId, NodeKind, SubworkflowNode};
pub use profile::TaskProfile;
pub use workflow::{EdgeTrigger, FlowEdge, WorkflowDefinition};
