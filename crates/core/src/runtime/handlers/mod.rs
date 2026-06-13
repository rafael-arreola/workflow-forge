//! Execution semantics for each node family, one module per kind.
//!
//! [`crate::spec::node::NodeKind`] is a closed enum (the vocabulary is
//! defined by the spec), so dispatch lives in `executor::execute_from` and
//! each family implements its semantics in a separate `impl WorkflowExecutor`:
//!
//! | Module | Kinds | Responsibility |
//! |---|---|---|
//! | [`event`] | `start`, `end` | trigger schema, defaults, final output |
//! | [`task`] | `task` | input resolution and execution with policy |
//! | [`foreach`] | `foreach` | iteration with per-element concurrency/throttle |
//! | [`loop_node`] | `loop` | bounded iteration with continuation condition |
//! | [`gateway`] | `gateway` | exclusive (branches), parallel (fan-out), join (fan-in) |
//! | [`subworkflow`] | `subworkflow` | execution of the child workflow as a task |
//!
//! The shared retry/timeout/panic policy is in
//! [`crate::runtime::policy`]. To add a new kind (spec change):
//! variant in `NodeKind`, module here, arm in `execute_from`, rules in
//! `validate/rules` and, if it invokes tasks, reuse of `execute_with_policy`.

pub(crate) mod event;
pub(crate) mod foreach;
pub(crate) mod gateway;
pub(crate) mod loop_node;
pub(crate) mod subworkflow;
pub(crate) mod task;
