//! The orchestrator: validated executor construction and graph traversal.
//!
//! The semantics for each node kind lives in `runtime::handlers`; the
//! retry/timeout/panic policy in `runtime::policy` (crate-private modules).
//! This module only knows about traversal: dispatch by kind, continuation
//! through edges (normal or error), and execution finalization.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, try_join_all};
use serde_json::{Value, json};
pub use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::error::{WorkflowError, codes};
use crate::expr::operators::OperatorRegistry;
use crate::io::blob::{BlobStoreFactory, TempDirBlobFactory};
use crate::io::secret::SecretProvider;
use crate::observe::{EventKind, ExecutionEvent, ExecutionObserver};
use crate::runtime::context::WorkflowContext;
use crate::runtime::graph::GraphIndex;
use crate::runtime::registry::WorkflowRegistry;
use crate::runtime::schemas::CompiledSchemas;
use crate::spec::node::gateway::GatewayKind;
use crate::spec::node::{NodeId, NodeKind, SubworkflowNode};
use crate::spec::workflow::{FlowEdge, WorkflowDefinition};
use crate::task::{TaskRegistry, WorkflowData, WorkflowResult};
use crate::validate::ValidationRule;

/// Tokens that arrived at a join, identified by their origin node.
pub(crate) type JoinArrivals = Vec<(NodeId, Arc<Value>)>;

/// Optional limits on an execution. **Unlimited by default**: no
/// `deadline` and no cancellation. The host decides when and how to bound.
///
/// ```
/// # use std::time::Duration;
/// # use workflow_forge_core::runtime::{RunOptions, CancellationToken};
/// let token = CancellationToken::new();
/// let options = RunOptions::default()
///     .deadline(Duration::from_secs(30)) // total execution limit
///     .cancel(token.clone());            // cooperative external cancellation
/// // token.cancel(); // from another task to abort the execution
/// # let _ = options;
/// ```
#[must_use]
#[derive(Default, Clone)]
pub struct RunOptions {
    /// Maximum total execution time. `None` = unlimited (default).
    pub deadline: Option<Duration>,
    /// Cooperative external cancellation token. `None` = no cancellation.
    pub cancel: Option<CancellationToken>,
}

impl RunOptions {
    /// Sets the maximum total execution time.
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Associates a cooperative cancellation token.
    pub fn cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }
}

fn timeout_error(deadline: Duration) -> WorkflowError {
    WorkflowError::new(
        codes::EXECUTION_TIMEOUT,
        format!(
            "Execution exceeded the {} ms deadline",
            deadline.as_millis()
        ),
    )
}

fn cancelled_error() -> WorkflowError {
    WorkflowError::new(codes::EXECUTION_CANCELLED, "Execution was cancelled")
}

/// Maximum nesting depth for sub-workflows (the root is level 1)
const MAX_SUBWORKFLOW_DEPTH: usize = 8;

/// Mutable state of an in-progress execution.
pub(crate) struct RunState {
    /// Accumulated arrivals per still-incomplete gateway join
    pub(crate) joins: Mutex<HashMap<NodeId, JoinArrivals>>,
    /// Results of executed end nodes
    pub(crate) ends: Mutex<Vec<(NodeId, Value)>>,
}

/// Workflow execution engine per spec 1.0: global context, JSONPath
/// mappings, gateways, retry/timeout, and error paths.
pub struct WorkflowExecutor {
    pub(crate) workflow: WorkflowDefinition,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) index: GraphIndex,
    pub(crate) schemas: CompiledSchemas,
    pub(crate) observer: Option<Arc<dyn ExecutionObserver>>,
    /// Blob storage factory for each execution
    /// (default: [`TempDirBlobFactory`])
    pub(crate) blobs: Arc<dyn BlobStoreFactory>,
    /// Child executors, one per `kind: "subworkflow"` node, resolved and
    /// validated at build time (construction fails if a name does not exist,
    /// there is a cycle, or the maximum depth is exceeded)
    pub(crate) subworkflows: HashMap<NodeId, WorkflowExecutor>,
    /// Optional injected operator registry for condition evaluation.
    #[allow(dead_code)]
    pub(crate) operator_registry: Option<Arc<OperatorRegistry>>,
}

/// Construction of a [`WorkflowExecutor`]: the single injection point for
/// runtime dependencies.
///
/// | Method | Dependency | Default |
/// |---|---|---|
/// | [`secrets`](Self::secrets) | [`SecretProvider`] for `{"$secret": "X"}` | env vars |
/// | [`workflows`](Self::workflows) | [`WorkflowRegistry`] of sub-workflows | none |
/// | [`blobs`](Self::blobs) | [`BlobStoreFactory`] for `$blob` storage | temp directory |
/// | [`observer`](Self::observer) | [`ExecutionObserver`] for events | none |
/// | [`rule`](Self::rule) | Host [`ValidationRule`] extensions | only built-in |
#[must_use]
pub struct WorkflowExecutorBuilder<'s> {
    workflow: WorkflowDefinition,
    registry: Arc<TaskRegistry>,
    secrets: &'s dyn SecretProvider,
    workflows: Option<Arc<WorkflowRegistry>>,
    blobs: Option<Arc<dyn BlobStoreFactory>>,
    observer: Option<Arc<dyn ExecutionObserver>>,
    rules: Vec<Box<dyn ValidationRule>>,
    operator_registry: Option<Arc<OperatorRegistry>>,
}

impl<'s> WorkflowExecutorBuilder<'s> {
    /// Secret provider for `{"$secret": "X"}` in inline profiles
    /// (default: env vars)
    pub fn secrets<'n>(self, secrets: &'n dyn SecretProvider) -> WorkflowExecutorBuilder<'n> {
        WorkflowExecutorBuilder {
            workflow: self.workflow,
            registry: self.registry,
            secrets,
            workflows: self.workflows,
            blobs: self.blobs,
            observer: self.observer,
            rules: self.rules,
            operator_registry: self.operator_registry,
        }
    }

    /// Shared sub-workflow registry. The document's inline `workflows`
    /// section takes precedence over this registry.
    pub fn workflows(mut self, workflows: Arc<WorkflowRegistry>) -> Self {
        self.workflows = Some(workflows);
        self
    }

    /// Blob storage factory per execution
    /// (default: [`TempDirBlobFactory`])
    pub fn blobs(mut self, blobs: Arc<dyn BlobStoreFactory>) -> Self {
        self.blobs = Some(blobs);
        self
    }

    /// Observer that will receive events from every execution, including
    /// sub-workflow executions (equivalent to [`WorkflowExecutor::with_observer`])
    pub fn observer(mut self, observer: Arc<dyn ExecutionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Adds a host validation rule. Runs after the built-in ones, also on
    /// every sub-workflow (inline or from the registry).
    pub fn rule<R: ValidationRule + 'static>(mut self, rule: R) -> Self {
        self.rules.push(Box::new(rule));
        self
    }

    /// Injects an operator registry for custom condition operators.
    /// When set, all condition evaluations (gateway branches, loop conditions)
    /// resolve custom operators against this registry instead of the global one.
    pub fn operator_registry(mut self, registry: Arc<OperatorRegistry>) -> Self {
        self.operator_registry = Some(registry);
        self
    }

    /// Builds the executor (structural validation, registered tasks,
    /// precompiled schemas, and recursively resolved sub-workflows)
    pub fn build(self) -> Result<WorkflowExecutor, Vec<WorkflowError>> {
        let mut executor = WorkflowExecutor::build_internal(
            self.workflow,
            self.registry,
            self.secrets,
            self.workflows.as_deref(),
            &self.rules,
            self.operator_registry,
            &mut Vec::new(),
        )?;
        if let Some(blobs) = self.blobs {
            executor.blobs = blobs;
        }
        if let Some(observer) = self.observer {
            executor.set_observer(observer);
        }
        Ok(executor)
    }
}

impl WorkflowExecutor {
    /// Builds an executor by validating the graph structure, that every
    /// referenced task is registered, and precompiling JSON Schemas.
    /// Secrets in inline profiles are resolved from env vars
    /// ([`crate::io::secret::EnvSecrets`]); for another provider use
    /// [`WorkflowExecutor::new_with_secrets`].
    pub fn new(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
    ) -> Result<Self, Vec<WorkflowError>> {
        Self::new_with_secrets(workflow, registry, &crate::io::secret::EnvSecrets)
    }

    /// Like [`WorkflowExecutor::new`], with an explicit [`SecretProvider`]
    /// for `{"$secret": "X"}` in the workflow's inline profiles.
    pub fn new_with_secrets(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
        secrets: &dyn SecretProvider,
    ) -> Result<Self, Vec<WorkflowError>> {
        Self::build_internal(
            workflow,
            registry,
            secrets,
            None,
            &[],
            None,
            &mut Vec::new(),
        )
    }

    /// Constructor with dependency injection (secrets, sub-workflows,
    /// blobs, observer, rules). See [`WorkflowExecutorBuilder`].
    pub fn builder(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
    ) -> WorkflowExecutorBuilder<'static> {
        WorkflowExecutorBuilder {
            workflow,
            registry,
            secrets: &crate::io::secret::EnvSecrets,
            workflows: None,
            blobs: None,
            observer: None,
            rules: Vec::new(),
            operator_registry: None,
        }
    }

    fn build_internal(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
        secrets: &dyn SecretProvider,
        shared: Option<&WorkflowRegistry>,
        rules: &[Box<dyn ValidationRule>],
        operator_registry: Option<Arc<OperatorRegistry>>,
        ancestry: &mut Vec<String>,
    ) -> Result<Self, Vec<WorkflowError>> {
        // Children are built with the original registry: inline profiles
        // from a document are local to that document
        let original_registry = Arc::clone(&registry);
        let registry = Self::registry_with_inline_profiles(&workflow, registry, secrets)?;

        let rule_refs: Vec<&dyn ValidationRule> = rules.iter().map(AsRef::as_ref).collect();
        crate::validate::validate_with(&workflow, &rule_refs)?;
        crate::validate::validate_tasks(&workflow, &registry)?;
        let schemas = CompiledSchemas::build(&workflow, &registry)?;
        let index = GraphIndex::build(&workflow);
        let subworkflows = Self::build_subworkflows(
            &workflow,
            &original_registry,
            secrets,
            shared,
            rules,
            operator_registry.as_ref(),
            ancestry,
        )?;

        Ok(Self {
            workflow,
            registry,
            index,
            schemas,
            observer: None,
            blobs: Arc::new(TempDirBlobFactory),
            subworkflows,
            operator_registry,
        })
    }

    /// If the document declares inline profiles (`tasks` section), registers
    /// them in a scoped copy of the registry: the shared registry is not
    /// polluted with the workflow's local definitions.
    fn registry_with_inline_profiles(
        workflow: &WorkflowDefinition,
        registry: Arc<TaskRegistry>,
        secrets: &dyn SecretProvider,
    ) -> Result<Arc<TaskRegistry>, Vec<WorkflowError>> {
        if workflow.tasks.is_empty() {
            return Ok(registry);
        }
        let scoped = registry.scoped();
        let errors: Vec<WorkflowError> = workflow
            .tasks
            .iter()
            .filter_map(|profile| scoped.register_profile(profile.clone(), secrets).err())
            .collect();
        if errors.is_empty() {
            Ok(Arc::new(scoped))
        } else {
            Err(errors)
        }
    }

    /// Resolves `kind: "subworkflow"` nodes and recursively builds (and
    /// validates) a child executor for each one: inline first, shared registry
    /// second. A nonexistent name, a cycle, excessive depth, or an invalid
    /// child all fail here — at parent build time — never at runtime.
    fn build_subworkflows(
        workflow: &WorkflowDefinition,
        registry: &Arc<TaskRegistry>,
        secrets: &dyn SecretProvider,
        shared: Option<&WorkflowRegistry>,
        rules: &[Box<dyn ValidationRule>],
        operator_registry: Option<&Arc<OperatorRegistry>>,
        ancestry: &mut Vec<String>,
    ) -> Result<HashMap<NodeId, WorkflowExecutor>, Vec<WorkflowError>> {
        let sub_nodes: Vec<(&NodeId, &SubworkflowNode)> = workflow
            .nodes
            .iter()
            .filter_map(|node| match &node.kind {
                NodeKind::Subworkflow(sub) => Some((&node.id, sub)),
                _ => None,
            })
            .collect();
        if sub_nodes.is_empty() {
            return Ok(HashMap::new());
        }
        if ancestry.len() + 1 >= MAX_SUBWORKFLOW_DEPTH {
            return Err(vec![WorkflowError::new(
                codes::SUBWORKFLOW_DEPTH_EXCEEDED,
                format!(
                    "Workflow '{}' nests sub-workflows beyond the \
                     maximum depth ({MAX_SUBWORKFLOW_DEPTH} levels)",
                    workflow.name
                ),
            )]);
        }

        let mut subworkflows = HashMap::new();
        let mut errors: Vec<WorkflowError> = Vec::new();
        ancestry.push(workflow.name.clone());
        for (node_id, sub) in sub_nodes {
            if ancestry.contains(&sub.workflow) {
                errors.push(
                    WorkflowError::new(
                        codes::SUBWORKFLOW_CYCLE,
                        format!(
                            "Node '{}' references workflow '{}', which is already \
                             in the execution chain ({})",
                            node_id,
                            sub.workflow,
                            ancestry.join(" → ")
                        ),
                    )
                    .with_source_task(node_id.to_string()),
                );
                continue;
            }
            let definition = workflow
                .workflows
                .iter()
                .find(|w| w.name == sub.workflow)
                .cloned()
                .or_else(|| {
                    shared
                        .and_then(|s| s.get(&sub.workflow))
                        .map(|w| (*w).clone())
                });
            let Some(definition) = definition else {
                errors.push(
                    WorkflowError::new(
                        codes::SUBWORKFLOW_NOT_FOUND,
                        format!(
                            "Node '{}' references workflow '{}', which is not in the \
                             document's `workflows` section nor in the registry",
                            node_id, sub.workflow
                        ),
                    )
                    .with_source_task(node_id.to_string()),
                );
                continue;
            };
            match Self::build_internal(
                definition,
                Arc::clone(registry),
                secrets,
                shared,
                rules,
                operator_registry.cloned(),
                ancestry,
            ) {
                Ok(child) => {
                    subworkflows.insert(node_id.clone(), child);
                }
                Err(child_errors) => {
                    errors.extend(child_errors.into_iter().map(|mut e| {
                        e.message = format!("in sub-workflow '{}': {}", sub.workflow, e.message);
                        e
                    }));
                }
            }
        }
        ancestry.pop();

        if errors.is_empty() {
            Ok(subworkflows)
        } else {
            Err(errors)
        }
    }

    /// Registers an observer that will receive events from every execution
    /// ([`crate::observe::ExecutionEvent`]), including sub-workflow
    /// executions.
    pub fn with_observer(mut self, observer: Arc<dyn ExecutionObserver>) -> Self {
        self.set_observer(observer);
        self
    }

    fn set_observer(&mut self, observer: Arc<dyn ExecutionObserver>) {
        for child in self.subworkflows.values_mut() {
            child.set_observer(Arc::clone(&observer));
        }
        self.observer = Some(observer);
    }

    /// Emits an event to the observer, if one is registered
    pub(crate) fn emit(&self, ctx: &WorkflowContext, kind: EventKind) {
        if let Some(observer) = &self.observer {
            observer.on_event(&ExecutionEvent {
                execution_id: ctx.execution_id().to_string(),
                parent_execution_id: ctx.parent_execution_id().map(str::to_string),
                seq: ctx.next_event_seq(),
                elapsed_ms: ctx.elapsed().as_millis() as u64,
                kind,
            });
        }
    }

    /// Executes the workflow until it finishes or fails (run-to-completion),
    /// **without** time limit or cancellation. To bound it, use
    /// [`run_with`](Self::run_with).
    pub async fn run(&self, trigger: WorkflowData) -> WorkflowResult {
        self.run_with(trigger, RunOptions::default()).await
    }

    /// Like [`run`](Self::run), but with [`RunOptions`]: a total `deadline`
    /// and/or a `CancellationToken`. By default executions are unlimited;
    /// these limits are opt-in from the host.
    ///
    /// When the deadline expires or cancellation is requested, the in-progress
    /// execution is abandoned (its futures are discarded: cooperative
    /// cancellation at the `await` points) and `EXECUTION_TIMEOUT` /
    /// `EXECUTION_CANCELLED` is returned. The execution's blobs are cleaned up
    /// regardless.
    pub async fn run_with(&self, trigger: WorkflowData, options: RunOptions) -> WorkflowResult {
        let ctx = WorkflowContext::with_blob_factory(
            &self.workflow,
            trigger.0.clone(),
            self.blobs.as_ref(),
        );
        let result = self.run_bounded(trigger, &ctx, &options).await;
        // Only the root execution cleans up blobs: sub-workflows share this
        // store and their references can cross the boundary
        if let Err(e) = ctx.blobs().cleanup().await {
            warn!(code = %e.code, message = %e.message, "Could not clean up blobs");
        }
        result
    }

    /// Runs `run_with_ctx` under the limits of `options`. Without limits, a
    /// simple `await`; with them, a race against the deadline and the token.
    async fn run_bounded(
        &self,
        trigger: WorkflowData,
        ctx: &WorkflowContext,
        options: &RunOptions,
    ) -> WorkflowResult {
        let work = self.run_with_ctx(trigger, ctx);

        match (options.deadline, options.cancel.clone()) {
            (None, None) => work.await,
            (Some(deadline), None) => {
                tokio::select! {
                    result = work => result,
                    _ = tokio::time::sleep(deadline) => Err(timeout_error(deadline)),
                }
            }
            (None, Some(token)) => {
                tokio::select! {
                    result = work => result,
                    _ = token.cancelled() => Err(cancelled_error()),
                }
            }
            (Some(deadline), Some(token)) => {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => Err(cancelled_error()),
                    result = work => result,
                    _ = tokio::time::sleep(deadline) => Err(timeout_error(deadline)),
                }
            }
        }
    }

    /// Common body of an execution (root or sub-workflow): start/end events
    /// around `run_inner`, without blob cleanup.
    #[tracing::instrument(skip(self, trigger, ctx))]
    pub(crate) async fn run_with_ctx(
        &self,
        trigger: WorkflowData,
        ctx: &WorkflowContext,
    ) -> WorkflowResult {
        info!(
            execution_id = %ctx.execution_id(),
            name = %self.workflow.name,
            "Starting workflow execution"
        );
        self.emit(
            ctx,
            EventKind::WorkflowStarted {
                workflow: json!({
                    "id": self.workflow.id,
                    "name": self.workflow.name,
                    "version": self.workflow.version,
                }),
                trigger: trigger.0.clone(),
            },
        );

        let result = self.run_inner(trigger, ctx).await;
        let duration_ms = ctx.elapsed().as_millis() as u64;
        match &result {
            Ok(output) => self.emit(
                ctx,
                EventKind::WorkflowCompleted {
                    output: output.0.clone(),
                    duration_ms,
                },
            ),
            Err(error) => self.emit(
                ctx,
                EventKind::WorkflowFailed {
                    error: error.clone(),
                    duration_ms,
                },
            ),
        }
        result
    }

    #[tracing::instrument(skip(self, trigger, ctx), fields(workflow = %self.workflow.name))]
    async fn run_inner(&self, trigger: WorkflowData, ctx: &WorkflowContext) -> WorkflowResult {
        let state = RunState {
            joins: Mutex::new(HashMap::new()),
            ends: Mutex::new(Vec::new()),
        };

        self.execute_from(&self.index.start, Arc::new(trigger.0), None, ctx, &state)
            .await?;

        // Joins that never received all their branches (e.g. an exclusive
        // gateway diverted the flow): explicit diagnosis instead of a silent
        // failure
        let starved: Vec<String> = {
            let joins = state.joins.lock();
            joins
                .iter()
                .map(|(id, arrivals)| {
                    let expected = self.index.incoming_count.get(id).copied().unwrap_or(0);
                    let arrived: Vec<&str> =
                        arrivals.iter().map(|(from, _)| from.0.as_str()).collect();
                    format!(
                        "'{}' received {}/{} branches (arrived: [{}])",
                        id,
                        arrivals.len(),
                        expected,
                        arrived.join(", ")
                    )
                })
                .collect()
        };

        let mut ends = state.ends.into_inner();
        if ends.is_empty() && !starved.is_empty() {
            return Err(WorkflowError::new(
                codes::JOIN_INCOMPLETE,
                format!(
                    "Execution finished with joins waiting for branches that never arrived: {}. \
                     Check that no exclusive gateway diverts the flow away from a join.",
                    starved.join("; ")
                ),
            ));
        }
        if !starved.is_empty() {
            warn!(joins = %starved.join("; "), "Incomplete joins at workflow end");
        }

        match ends.len() {
            0 => Err(WorkflowError::new(
                codes::NO_OUTPUT,
                "Workflow finished without reaching any end node",
            )),
            1 => Ok(WorkflowData(ends.pop().expect("length already checked").1)),
            // Multiple ends reached (parallel branches): object keyed by end id
            _ => Ok(WorkflowData(Value::Object(
                ends.into_iter().map(|(id, v)| (id.0, v)).collect(),
            ))),
        }
    }

    /// Executes a node and continues traversal through its outgoing edges.
    /// `carried` is the predecessor's output (the "token" arriving at the node)
    /// and `origin` is that predecessor's id (None only for start).
    /// The token travels as `Arc` so fan-out does not clone payloads.
    #[tracing::instrument(skip(self, carried, ctx, state), fields(node_id = %node_id))]
    pub(crate) fn execute_from<'a>(
        &'a self,
        node_id: &'a NodeId,
        carried: Arc<Value>,
        origin: Option<NodeId>,
        ctx: &'a WorkflowContext,
        state: &'a RunState,
    ) -> BoxFuture<'a, Result<(), WorkflowError>> {
        Box::pin(async move {
            let node = self.index.node(node_id);
            debug!(node_id = %node_id, "Executing node");

            // A join "starts" multiple times (once per arrival); its start
            // event is emitted when it completes, alongside the end event
            let is_join =
                matches!(&node.kind, NodeKind::Gateway(g) if g.gateway == GatewayKind::Join);
            if !is_join {
                self.emit(
                    ctx,
                    EventKind::NodeStarted {
                        node_id: node_id.0.clone(),
                        kind: node.kind.name().to_string(),
                    },
                );
            }
            let node_started = Instant::now();

            match &node.kind {
                NodeKind::Start(start_node) => {
                    self.run_start(node_id, start_node, carried, node_started, ctx, state)
                        .await
                }

                NodeKind::End(end_node) => {
                    self.run_end(node_id, end_node, carried, node_started, ctx, state)
                        .await
                }

                NodeKind::Task(task_node) => {
                    let result = self.run_task(node, task_node, carried, ctx).await;
                    self.after_task_result(node_id, result, node_started, ctx, state)
                        .await
                }

                NodeKind::Foreach(foreach) => {
                    let result = self.run_foreach(node, foreach, ctx).await;
                    self.after_task_result(node_id, result, node_started, ctx, state)
                        .await
                }

                NodeKind::Loop(lp) => {
                    let result = self.run_loop(node, lp, carried, ctx).await;
                    self.after_task_result(node_id, result, node_started, ctx, state)
                        .await
                }

                NodeKind::Gateway(gateway) => {
                    self.run_gateway(node, gateway, carried, origin, node_started, ctx, state)
                        .await
                }

                NodeKind::Subworkflow(sub) => {
                    let result = self.run_subworkflow(node, sub, carried, ctx).await;
                    self.after_task_result(node_id, result, node_started, ctx, state)
                        .await
                }
            }
        })
    }

    /// Continues traversal through a set of edges. Multiple edges are
    /// traversed concurrently; the first error cancels sibling branches.
    pub(crate) async fn continue_through(
        &self,
        edges: &[FlowEdge],
        carried: Arc<Value>,
        ctx: &WorkflowContext,
        state: &RunState,
    ) -> Result<(), WorkflowError> {
        match edges {
            [] => Ok(()),
            [edge] => {
                self.execute_from(&edge.to, carried, Some(edge.from.clone()), ctx, state)
                    .await
            }
            many => try_join_all(many.iter().map(|edge| {
                self.execute_from(
                    &edge.to,
                    Arc::clone(&carried),
                    Some(edge.from.clone()),
                    ctx,
                    state,
                )
            }))
            .await
            .map(|_| ()),
        }
    }

    /// Finalizes the execution of a task-invoking node (task, foreach, or
    /// subworkflow): publishes output and continues, or follows the error
    /// path (`on: error` / `on: panic`) if one exists.
    async fn after_task_result(
        &self,
        node_id: &NodeId,
        result: Result<Value, WorkflowError>,
        node_started: Instant,
        ctx: &WorkflowContext,
        state: &RunState,
    ) -> Result<(), WorkflowError> {
        match result {
            Ok(output) => {
                self.emit(
                    ctx,
                    EventKind::NodeCompleted {
                        node_id: node_id.0.clone(),
                        output: output.clone(),
                        duration_ms: node_started.elapsed().as_millis() as u64,
                    },
                );
                ctx.set_node_output(node_id, output.clone());
                self.continue_through(
                    self.index.outgoing_edges(node_id),
                    Arc::new(output),
                    ctx,
                    state,
                )
                .await
            }
            Err(err) => {
                // A panic only routes through `on: panic` (no fallback to
                // error: it may have left partial effects); everything else
                // routes through `on: error`
                let route_edges = if err.code == codes::TASK_PANIC {
                    self.index.panic_edges(node_id)
                } else {
                    self.index.error_edges(node_id)
                };
                let error_routed = !route_edges.is_empty();
                self.emit(
                    ctx,
                    EventKind::NodeFailed {
                        node_id: node_id.0.clone(),
                        error: err.clone(),
                        error_routed,
                    },
                );
                if !error_routed {
                    return Err(err);
                }
                // Alternate path: the serialized error travels as the token
                warn!(node_id = %node_id, code = %err.code, "Node failed; following alternate path");
                let error_value = serde_json::to_value(&err).unwrap_or(Value::Null);
                ctx.set_node_error(node_id, error_value.clone());
                self.continue_through(route_edges, Arc::new(error_value), ctx, state)
                    .await
            }
        }
    }
}
