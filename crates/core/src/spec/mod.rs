//! La familia **spec**: el lenguaje de definición de workflows.
//!
//! Contiene exclusivamente los tipos de datos que serializan/deserializan
//! un documento de workflow (spec 1.0): el grafo ([`WorkflowDefinition`],
//! [`FlowEdge`]), los nodos ([`Node`], [`NodeKind`] y sus variantes), las
//! condiciones declarativas ([`Condition`]) y los perfiles de tarea
//! ([`TaskProfile`]).
//!
//! Regla de la familia: **aquí no hay lógica de ejecución ni de validación**,
//! solo la forma del lenguaje. La semántica vive en `validate` (reglas
//! estáticas), `expr` (resolución de expresiones) y `runtime` (ejecución).

pub mod condition;
pub mod node;
pub mod profile;
pub mod workflow;

pub use condition::{CompareOp, Comparison, Condition};
pub use node::{Node, NodeId, NodeKind, SubworkflowNode};
pub use profile::TaskProfile;
pub use workflow::{EdgeTrigger, FlowEdge, WorkflowDefinition};
