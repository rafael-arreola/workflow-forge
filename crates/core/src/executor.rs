mod graph;
mod schemas;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, try_join_all};
use futures::stream::{self, StreamExt};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::context::WorkflowContext;
use crate::error::WorkflowError;
use crate::mapping;
use crate::node::foreach::{ForeachNode, OnItemError};
use crate::node::gateway::{GatewayKind, GatewayNode};
use crate::node::task::{Backoff, RetryPolicy, TaskNode};
use crate::node::{Node, NodeId, NodeKind};
use crate::observe::{EventKind, ExecutionEvent, ExecutionObserver};
use crate::registry::TaskRegistry;
use crate::task::{Task, WorkflowData, WorkflowResult};
use crate::workflow::{FlowEdge, WorkflowDefinition};
use graph::GraphIndex;
use schemas::{CompiledSchemas, validate_compiled};

/// Tokens que llegaron a un join, identificados por su nodo de origen.
type JoinArrivals = Vec<(NodeId, Arc<Value>)>;

/// Estado mutable de una ejecución en curso.
struct RunState {
    /// Llegadas acumuladas por cada gateway join aún incompleto
    joins: Mutex<HashMap<NodeId, JoinArrivals>>,
    /// Resultados de los nodos end ejecutados
    ends: Mutex<Vec<(NodeId, Value)>>,
}

/// Motor de ejecución de workflows según la spec 1.0: contexto global,
/// mappings JSONPath, gateways, retry/timeout y rutas de error.
pub struct WorkflowExecutor {
    workflow: WorkflowDefinition,
    registry: Arc<TaskRegistry>,
    index: GraphIndex,
    schemas: CompiledSchemas,
    observer: Option<Arc<dyn ExecutionObserver>>,
}

/// Política de ejecución de una invocación de tarea (nodo task o elemento
/// de foreach): reintentos, timeout y si se emiten eventos de attempt.
struct ExecPolicy<'a> {
    retry: Option<&'a RetryPolicy>,
    timeout_ms: Option<u64>,
    emit_attempts: bool,
}

impl WorkflowExecutor {
    /// Construye un executor validando la estructura del grafo, que toda
    /// tarea referenciada esté registrada y precompilando los JSON Schemas.
    /// Los secretos de perfiles inline se resuelven con variables de entorno
    /// ([`crate::secret::EnvSecrets`]); para otro provider usar
    /// [`WorkflowExecutor::new_with_secrets`].
    pub fn new(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
    ) -> Result<Self, Vec<WorkflowError>> {
        Self::new_with_secrets(workflow, registry, &crate::secret::EnvSecrets)
    }

    /// Como [`WorkflowExecutor::new`], con un [`SecretProvider`] explícito
    /// para los `{"$secret": "X"}` de los perfiles inline del workflow.
    pub fn new_with_secrets(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
        secrets: &dyn crate::secret::SecretProvider,
    ) -> Result<Self, Vec<WorkflowError>> {
        // Los perfiles inline viven en una copia scoped: el registry
        // compartido no se contamina con definiciones locales del workflow
        let registry = if workflow.tasks.is_empty() {
            registry
        } else {
            let scoped = registry.scoped();
            let errors: Vec<WorkflowError> = workflow
                .tasks
                .iter()
                .filter_map(|profile| scoped.register_profile(profile.clone(), secrets).err())
                .collect();
            if !errors.is_empty() {
                return Err(errors);
            }
            Arc::new(scoped)
        };

        crate::validation::validate(&workflow)?;
        crate::validation::validate_tasks(&workflow, &registry)?;
        let schemas = CompiledSchemas::build(&workflow, &registry)?;
        let index = GraphIndex::build(&workflow);
        Ok(Self {
            workflow,
            registry,
            index,
            schemas,
            observer: None,
        })
    }

    /// Registra un observer que recibirá los eventos de cada ejecución
    /// ([`crate::observe::ExecutionEvent`]).
    pub fn with_observer(mut self, observer: Arc<dyn ExecutionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Emite un evento al observer, si hay uno registrado
    fn emit(&self, ctx: &WorkflowContext, kind: EventKind) {
        if let Some(observer) = &self.observer {
            observer.on_event(&ExecutionEvent {
                execution_id: ctx.execution_id().to_string(),
                seq: ctx.next_event_seq(),
                elapsed_ms: ctx.elapsed().as_millis() as u64,
                kind,
            });
        }
    }

    /// Ejecuta el workflow hasta terminar o fallar (run-to-completion).
    pub async fn run(&self, trigger: WorkflowData) -> WorkflowResult {
        let ctx = WorkflowContext::new(&self.workflow, trigger.0.clone());
        info!(
            execution_id = %ctx.execution_id(),
            name = %self.workflow.name,
            "Iniciando ejecución de workflow"
        );
        self.emit(
            &ctx,
            EventKind::WorkflowStarted {
                workflow: json!({
                    "id": self.workflow.id,
                    "name": self.workflow.name,
                    "version": self.workflow.version,
                }),
                trigger: trigger.0.clone(),
            },
        );

        let result = self.run_inner(trigger, &ctx).await;
        if let Err(e) = ctx.blobs().cleanup().await {
            warn!(code = %e.code, message = %e.message, "No se pudieron limpiar los blobs");
        }
        let duration_ms = ctx.elapsed().as_millis() as u64;
        match &result {
            Ok(output) => self.emit(
                &ctx,
                EventKind::WorkflowCompleted {
                    output: output.0.clone(),
                    duration_ms,
                },
            ),
            Err(error) => self.emit(
                &ctx,
                EventKind::WorkflowFailed {
                    error: error.clone(),
                    duration_ms,
                },
            ),
        }
        result
    }

    async fn run_inner(&self, trigger: WorkflowData, ctx: &WorkflowContext) -> WorkflowResult {
        let state = RunState {
            joins: Mutex::new(HashMap::new()),
            ends: Mutex::new(Vec::new()),
        };

        self.execute_from(&self.index.start, Arc::new(trigger.0), None, ctx, &state)
            .await?;

        // Joins que nunca recibieron todas sus ramas (p.ej. un exclusive
        // desvió el flujo): diagnóstico explícito en vez de un fallo mudo
        let starved: Vec<String> = {
            let joins = state.joins.lock().expect("RunState lock poisoned");
            joins
                .iter()
                .map(|(id, arrivals)| {
                    let expected = self.index.incoming_count.get(id).copied().unwrap_or(0);
                    let arrived: Vec<&str> =
                        arrivals.iter().map(|(from, _)| from.0.as_str()).collect();
                    format!(
                        "'{}' recibió {}/{} ramas (llegaron: [{}])",
                        id,
                        arrivals.len(),
                        expected,
                        arrived.join(", ")
                    )
                })
                .collect()
        };

        let mut ends = state.ends.into_inner().expect("RunState lock poisoned");
        if ends.is_empty() && !starved.is_empty() {
            return Err(WorkflowError::new(
                "JOIN_INCOMPLETE",
                format!(
                    "La ejecución terminó con joins esperando ramas que nunca llegaron: {}. \
                     Verifica que ningún gateway exclusive desvíe el flujo lejos de un join.",
                    starved.join("; ")
                ),
            ));
        }
        if !starved.is_empty() {
            warn!(joins = %starved.join("; "), "Joins incompletos al finalizar el workflow");
        }

        match ends.len() {
            0 => Err(WorkflowError::new(
                "NO_OUTPUT",
                "El workflow finalizó sin alcanzar ningún nodo end",
            )),
            1 => Ok(WorkflowData(ends.pop().expect("len comprobado").1)),
            // Varios ends alcanzados (ramas paralelas): objeto por id de end
            _ => Ok(WorkflowData(Value::Object(
                ends.into_iter().map(|(id, v)| (id.0, v)).collect(),
            ))),
        }
    }

    /// Ejecuta un nodo y continúa el recorrido por sus aristas salientes.
    /// `carried` es el output del predecesor (el "token" que llega al nodo)
    /// y `origin` el id de ese predecesor (None solo para el start).
    /// El token viaja como `Arc` para que el fan-out no clone payloads.
    fn execute_from<'a>(
        &'a self,
        node_id: &'a NodeId,
        carried: Arc<Value>,
        origin: Option<NodeId>,
        ctx: &'a WorkflowContext,
        state: &'a RunState,
    ) -> BoxFuture<'a, Result<(), WorkflowError>> {
        Box::pin(async move {
            let node = self.index.node(node_id);
            debug!(node_id = %node_id, "Ejecutando nodo");

            // Un join "arranca" varias veces (una por llegada); su evento de
            // inicio se emite cuando completa, junto al de término
            let is_join =
                matches!(&node.kind, NodeKind::Gateway(g) if g.gateway == GatewayKind::Join);
            if !is_join {
                self.emit(
                    ctx,
                    EventKind::NodeStarted {
                        node_id: node_id.0.clone(),
                        kind: kind_name(&node.kind).to_string(),
                    },
                );
            }
            let node_started = Instant::now();

            match &node.kind {
                NodeKind::Start(start_node) => {
                    if let Some(validator) = self.schemas.nodes.get(node_id) {
                        validate_compiled(validator, &carried).map_err(|e| {
                            WorkflowError::new(
                                "SCHEMA_VALIDATION_FAILED",
                                format!("El trigger no cumple el schema del start: {e}"),
                            )
                            .with_source_task(node_id.to_string())
                        })?;
                    }
                    let mut output = Arc::unwrap_or_clone(carried);
                    if let Some(defaults) = &start_node.defaults {
                        let mut map = match output {
                            Value::Object(m) => m,
                            other => {
                                let mut m = serde_json::Map::new();
                                m.insert("_input".to_string(), other);
                                m
                            }
                        };
                        for (key, value) in defaults {
                            map.entry(key.clone()).or_insert_with(|| value.0.clone());
                        }
                        output = Value::Object(map);
                    }
                    ctx.set_node_output(node_id, output.clone());
                    self.emit(
                        ctx,
                        EventKind::NodeCompleted {
                            node_id: node_id.0.clone(),
                            output: output.clone(),
                            duration_ms: node_started.elapsed().as_millis() as u64,
                        },
                    );
                    self.continue_through(
                        self.index.outgoing_edges(node_id),
                        Arc::new(output),
                        ctx,
                        state,
                    )
                    .await
                }

                NodeKind::End(end_node) => {
                    let result = match &end_node.output {
                        Some(mapping_def) => {
                            ctx.with_state(|s| mapping::resolve(mapping_def, s))
                                .map_err(|e| e.with_source_task(node_id.to_string()))?
                        }
                        None => Arc::unwrap_or_clone(carried),
                    };
                    if let Some(validator) = self.schemas.nodes.get(node_id) {
                        validate_compiled(validator, &result).map_err(|e| {
                            WorkflowError::new(
                                "OUTPUT_SCHEMA_VALIDATION_FAILED",
                                format!("El resultado no cumple el schema del end: {e}"),
                            )
                            .with_source_task(node_id.to_string())
                        })?;
                    }
                    ctx.set_node_output(node_id, result.clone());
                    self.emit(
                        ctx,
                        EventKind::NodeCompleted {
                            node_id: node_id.0.clone(),
                            output: result.clone(),
                            duration_ms: node_started.elapsed().as_millis() as u64,
                        },
                    );
                    state
                        .ends
                        .lock()
                        .expect("RunState lock poisoned")
                        .push((node_id.clone(), result));
                    Ok(())
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

                NodeKind::Gateway(gateway) => {
                    self.run_gateway(node, gateway, carried, origin, node_started, ctx, state)
                        .await
                }

                NodeKind::Subworkflow(_) => unreachable!("la validación rechaza subworkflow"),
            }
        })
    }

    /// Continúa el recorrido por un conjunto de aristas. Varias aristas se
    /// recorren concurrentemente; el primer error cancela las ramas hermanas.
    async fn continue_through(
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

    /// Cierra la ejecución de un nodo que invoca tareas (task o foreach):
    /// publica output y continúa, o sigue la ruta de error si existe.
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
                let error_edges = self.index.error_edges(node_id);
                let error_routed = !error_edges.is_empty();
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
                // Ruta de error: el error serializado viaja como token
                warn!(node_id = %node_id, code = %err.code, "Nodo falló; siguiendo ruta de error");
                let error_value = serde_json::to_value(&err).unwrap_or(Value::Null);
                ctx.set_node_error(node_id, error_value.clone());
                self.continue_through(error_edges, Arc::new(error_value), ctx, state)
                    .await
            }
        }
    }

    /// Ejecuta un nodo task: resuelve su input, aplica timeout y reintentos.
    async fn run_task(
        &self,
        node: &Node,
        task_node: &TaskNode,
        carried: Arc<Value>,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let task = self
            .registry
            .get(&task_node.task)
            .expect("validate_tasks garantiza el registro");

        let input = match &task_node.input {
            Some(mapping_def) => ctx
                .with_state(|s| mapping::resolve(mapping_def, s))
                .map_err(|e| e.with_source_task(node.id.to_string()))?,
            None => Arc::unwrap_or_clone(carried),
        };

        self.execute_with_policy(
            &task,
            &node.id,
            input,
            ExecPolicy {
                retry: task_node.retry.as_ref(),
                timeout_ms: task_node.timeout_ms,
                emit_attempts: true,
            },
            ctx,
        )
        .await
    }

    /// Invoca una tarea con validación de schemas, timeout y reintentos.
    /// Es el camino compartido entre nodos task y elementos de foreach.
    async fn execute_with_policy(
        &self,
        task: &Arc<dyn Task>,
        node_id: &NodeId,
        input: Value,
        policy: ExecPolicy<'_>,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let task_id = task.task_id().clone();
        let (input_validator, output_validator) = self
            .schemas
            .tasks
            .get(&task_id)
            .map(|(i, o)| (i.as_ref(), o.as_ref()))
            .unwrap_or((None, None));

        if let Some(validator) = input_validator {
            validate_compiled(validator, &input).map_err(|e| {
                WorkflowError::new(
                    "TASK_INPUT_INVALID",
                    format!("El input de la tarea '{task_id}' no cumple su schema: {e}"),
                )
                .with_source_task(node_id.to_string())
            })?;
        }

        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            if policy.emit_attempts {
                self.emit(
                    ctx,
                    EventKind::TaskAttemptStarted {
                        node_id: node_id.0.clone(),
                        attempt,
                        input: (attempt == 1).then(|| input.clone()),
                    },
                );
            }
            let execution = task.execute(ctx, WorkflowData(input.clone()));
            let result = match policy.timeout_ms {
                Some(ms) => {
                    match tokio::time::timeout(Duration::from_millis(ms), execution).await {
                        Ok(result) => result,
                        Err(_) => Err(WorkflowError::new(
                            "TASK_TIMEOUT",
                            format!("La tarea '{task_id}' superó el timeout de {ms}ms"),
                        )),
                    }
                }
                None => execution.await,
            };

            match result {
                Ok(output) => {
                    if let Some(validator) = output_validator {
                        validate_compiled(validator, &output.0).map_err(|e| {
                            WorkflowError::new(
                                "TASK_OUTPUT_INVALID",
                                format!(
                                    "El output de la tarea '{task_id}' no cumple su schema: {e}"
                                ),
                            )
                            .with_source_task(node_id.to_string())
                        })?;
                    }
                    return Ok(output.0);
                }
                Err(mut err) => {
                    if err.source_task.is_none() {
                        err.source_task = Some(node_id.to_string());
                    }
                    let retries_left = policy.retry.is_some_and(|r| attempt <= r.max);
                    let delay = retries_left.then(|| {
                        backoff_delay(policy.retry.expect("retries_left lo implica"), attempt)
                    });
                    if policy.emit_attempts {
                        self.emit(
                            ctx,
                            EventKind::TaskAttemptFailed {
                                node_id: node_id.0.clone(),
                                attempt,
                                error: err.clone(),
                                will_retry: retries_left,
                                next_delay_ms: delay.map(|d| d.as_millis() as u64),
                            },
                        );
                    }
                    let Some(delay) = delay else {
                        return Err(err);
                    };
                    warn!(
                        node_id = %node_id,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        code = %err.code,
                        "Tarea falló; reintentando"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Ejecuta un nodo foreach: resuelve `items` y ejecuta la tarea por cada
    /// elemento con la concurrencia/throttle configurados.
    async fn run_foreach(
        &self,
        node: &Node,
        foreach: &ForeachNode,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let task = self
            .registry
            .get(&foreach.task)
            .expect("validate_tasks garantiza el registro");

        let items_value = ctx
            .with_state(|s| mapping::resolve(&foreach.items, s))
            .map_err(|e| e.with_source_task(node.id.to_string()))?;
        let Value::Array(items) = items_value else {
            return Err(WorkflowError::new(
                "FOREACH_ITEMS_NOT_ARRAY",
                format!(
                    "El mapping `items` del foreach '{}' no resolvió a un array",
                    node.id
                ),
            )
            .with_source_task(node.id.to_string()));
        };

        // Throttle: separa los ARRANQUES de elementos al menos `throttle_ms`
        // entre sí, también bajo concurrencia (rate limit hacia el destino)
        let gate = Arc::new(Mutex::new(Instant::now()));

        let futures: Vec<_> = items
            .into_iter()
            .enumerate()
            .map(|(index, item)| {
                self.foreach_item(
                    node,
                    foreach,
                    Arc::clone(&task),
                    Arc::clone(&gate),
                    index,
                    item,
                    ctx,
                )
            })
            .collect();
        let mut buffered = stream::iter(futures).buffered(foreach.concurrency);

        match foreach.on_item_error {
            // Fallo rápido: el primer error corta el stream y cancela los
            // elementos aún en vuelo (drop del buffered)
            OnItemError::Fail => {
                let mut outputs = Vec::new();
                while let Some((_, _, result)) = buffered.next().await {
                    outputs.push(result?);
                }
                Ok(Value::Array(outputs))
            }
            // Ejecuta todo y separa éxitos de fallos; el nodo no falla
            OnItemError::Collect => {
                let results: Vec<(usize, Value, Result<Value, WorkflowError>)> =
                    buffered.collect().await;
                let mut ok = Vec::new();
                let mut failed = Vec::new();
                for (index, item, result) in results {
                    match result {
                        Ok(output) => ok.push(output),
                        Err(error) => failed.push(json!({
                            "index": index,
                            "item": item,
                            "error": serde_json::to_value(&error).unwrap_or(Value::Null),
                        })),
                    }
                }
                Ok(json!({ "ok": ok, "failed": failed }))
            }
        }
    }

    /// Ejecuta UN elemento de un foreach: respeta el throttle, aplica la
    /// política de retry/timeout y emite los eventos del elemento.
    /// Devuelve un future boxeado con lifetimes explícitos (rustc no logra
    /// probar `Send` de closures async dentro del executor recursivo).
    #[allow(clippy::too_many_arguments)]
    fn foreach_item<'a>(
        &'a self,
        node: &'a Node,
        foreach: &'a ForeachNode,
        task: Arc<dyn Task>,
        gate: Arc<Mutex<Instant>>,
        index: usize,
        item: Value,
        ctx: &'a WorkflowContext,
    ) -> BoxFuture<'a, (usize, Value, Result<Value, WorkflowError>)> {
        Box::pin(self.foreach_item_inner(node, foreach, task, gate, index, item, ctx))
    }

    #[allow(clippy::too_many_arguments)]
    async fn foreach_item_inner(
        &self,
        node: &Node,
        foreach: &ForeachNode,
        task: Arc<dyn Task>,
        gate: Arc<Mutex<Instant>>,
        index: usize,
        item: Value,
        ctx: &WorkflowContext,
    ) -> (usize, Value, Result<Value, WorkflowError>) {
        let throttle = Duration::from_millis(foreach.throttle_ms);
        if !throttle.is_zero() {
            let wait = {
                let mut next = gate.lock().expect("foreach gate lock poisoned");
                let now = Instant::now();
                let start_at = (*next).max(now);
                *next = start_at + throttle;
                start_at - now
            };
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
        }

        let result = self
            .execute_with_policy(
                &task,
                &node.id,
                item.clone(),
                ExecPolicy {
                    retry: foreach.retry.as_ref(),
                    timeout_ms: foreach.timeout_ms,
                    emit_attempts: false,
                },
                ctx,
            )
            .await;
        match &result {
            Ok(output) => self.emit(
                ctx,
                EventKind::ForeachItemCompleted {
                    node_id: node.id.0.clone(),
                    index,
                    output: output.clone(),
                },
            ),
            Err(error) => self.emit(
                ctx,
                EventKind::ForeachItemFailed {
                    node_id: node.id.0.clone(),
                    index,
                    error: error.clone(),
                },
            ),
        }
        (index, item, result)
    }

    /// Ejecuta un gateway según su tipo.
    #[allow(clippy::too_many_arguments)]
    async fn run_gateway(
        &self,
        node: &Node,
        gateway: &GatewayNode,
        carried: Arc<Value>,
        origin: Option<NodeId>,
        node_started: Instant,
        ctx: &WorkflowContext,
        state: &RunState,
    ) -> Result<(), WorkflowError> {
        let node_id = &node.id;
        match gateway.gateway {
            GatewayKind::Exclusive => {
                let winner = self
                    .pick_branch(gateway, ctx)
                    .map_err(|e| e.with_source_task(node_id.to_string()))?
                    .ok_or_else(|| {
                        WorkflowError::new(
                            "NO_BRANCH_MATCHED",
                            format!(
                                "Ninguna rama del gateway '{node_id}' se cumplió y no hay rama else"
                            ),
                        )
                        .with_source_task(node_id.to_string())
                    })?;

                debug!(node_id = %node_id, branch = %winner, "Gateway exclusive resuelto");
                ctx.set_node_output(node_id, (*carried).clone());
                self.emit(
                    ctx,
                    EventKind::NodeCompleted {
                        node_id: node_id.0.clone(),
                        output: (*carried).clone(),
                        duration_ms: node_started.elapsed().as_millis() as u64,
                    },
                );
                let edges: Vec<FlowEdge> = self
                    .index
                    .outgoing_edges(node_id)
                    .iter()
                    .filter(|e| e.label.as_deref() == Some(winner.as_str()))
                    .cloned()
                    .collect();
                self.continue_through(&edges, carried, ctx, state).await
            }

            GatewayKind::Parallel => {
                ctx.set_node_output(node_id, (*carried).clone());
                self.emit(
                    ctx,
                    EventKind::NodeCompleted {
                        node_id: node_id.0.clone(),
                        output: (*carried).clone(),
                        duration_ms: node_started.elapsed().as_millis() as u64,
                    },
                );
                self.continue_through(self.index.outgoing_edges(node_id), carried, ctx, state)
                    .await
            }

            GatewayKind::Join => {
                let expected = *self
                    .index
                    .incoming_count
                    .get(node_id)
                    .expect("validación exige >=2 entradas");
                let from = origin.expect("un join siempre tiene predecesor");
                let complete = {
                    let mut joins = state.joins.lock().expect("RunState lock poisoned");
                    let arrivals = joins.entry(node_id.clone()).or_default();
                    arrivals.push((from, carried));
                    if arrivals.len() == expected {
                        // Se remueve la entrada: lo que quede en el mapa al
                        // final son joins que nunca completaron
                        joins.remove(node_id)
                    } else {
                        None
                    }
                };

                match complete {
                    None => Ok(()), // esta rama termina; la última llegada continúa
                    Some(arrivals) => {
                        // Output determinista: objeto {nodo_origen: output}
                        let output = Value::Object(
                            arrivals
                                .into_iter()
                                .map(|(id, v)| (id.0, Arc::unwrap_or_clone(v)))
                                .collect(),
                        );
                        ctx.set_node_output(node_id, output.clone());
                        self.emit(
                            ctx,
                            EventKind::NodeStarted {
                                node_id: node_id.0.clone(),
                                kind: "gateway".to_string(),
                            },
                        );
                        self.emit(
                            ctx,
                            EventKind::NodeCompleted {
                                node_id: node_id.0.clone(),
                                output: output.clone(),
                                duration_ms: node_started.elapsed().as_millis() as u64,
                            },
                        );
                        self.continue_through(
                            self.index.outgoing_edges(node_id),
                            Arc::new(output),
                            ctx,
                            state,
                        )
                        .await
                    }
                }
            }
        }
    }

    /// Evalúa las ramas de un gateway exclusive en orden y devuelve el label
    /// de la primera que se cumple, o la rama else.
    fn pick_branch(
        &self,
        gateway: &GatewayNode,
        ctx: &WorkflowContext,
    ) -> Result<Option<String>, WorkflowError> {
        ctx.with_state(|context| {
            for branch in &gateway.branches {
                if let Some(when) = &branch.when
                    && when.evaluate(context)?
                {
                    return Ok(Some(branch.edge.clone()));
                }
            }
            Ok(gateway
                .branches
                .iter()
                .find(|b| b.is_else)
                .map(|b| b.edge.clone()))
        })
    }
}

/// Nombre del kind de un nodo, como aparece en la spec JSON.
fn kind_name(kind: &NodeKind) -> &'static str {
    match kind {
        NodeKind::Start(_) => "start",
        NodeKind::End(_) => "end",
        NodeKind::Task(_) => "task",
        NodeKind::Foreach(_) => "foreach",
        NodeKind::Gateway(_) => "gateway",
        NodeKind::Subworkflow(_) => "subworkflow",
    }
}

/// Espera entre reintentos según la estrategia de backoff.
/// `attempt` es el intento que acaba de fallar (1-indexado).
fn backoff_delay(retry: &RetryPolicy, attempt: u32) -> Duration {
    let base = retry.initial_ms;
    let ms = match retry.backoff {
        Backoff::Exponential => base.saturating_mul(2u64.saturating_pow(attempt - 1)),
        Backoff::Linear => base.saturating_mul(attempt as u64),
        Backoff::Fixed => base,
    };
    Duration::from_millis(ms)
}
