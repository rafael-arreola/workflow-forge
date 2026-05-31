pub mod context;
pub mod executor;
pub mod node;
pub mod registry;
pub mod task;
pub mod types;
pub mod workflow;

pub use context::WorkflowContext;
pub use executor::{ExecutionState, WorkflowExecutor};
pub use node::{
    ConditionalBranch, ConditionalNode, JoinNode, LoopNode, Node, NodeKind, ParallelNode,
    SwitchCase, SwitchNode, TaskNode,
};
pub use registry::TaskRegistry;
pub use task::Task;
pub use types::{PortDef, WorkflowData, WorkflowError, WorkflowResult};
pub use workflow::{
    Condition, ConditionOp, DataMapping, EdgeEndpoint, ErrorPolicy, FlowEdge, OutputStatus,
    WorkflowConfig, WorkflowDef,
};

/// Versión del crate workflow-forge-core
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
