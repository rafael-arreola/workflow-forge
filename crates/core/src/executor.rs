use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::{BoxFuture, try_join_all};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::context::WorkflowContext;
use crate::error::WorkflowError;
use crate::mapping;
use crate::node::gateway::{GatewayKind, GatewayNode};
use crate::node::task::{Backoff, RetryPolicy, TaskNode};
use crate::node::{Node, NodeId, NodeKind};
use crate::registry::TaskRegistry;
use crate::task::TaskId;
use crate::types::{WorkflowData, WorkflowResult};
use crate::workflow::{EdgeTrigger, FlowEdge, WorkflowDefinition};

/// Índices precalculados del grafo para búsquedas rápidas.
struct GraphIndex {
    nodes: HashMap<NodeId, Node>,
    /// Aristas salientes del flujo normal (sin `on: error`)
    outgoing: HashMap<NodeId, Vec<FlowEdge>>,
    /// Aristas salientes de error (`on: error`)
    outgoing_error: HashMap<NodeId, Vec<FlowEdge>>,
    /// Cantidad de aristas entrantes del flujo normal por nodo
    incoming_count: HashMap<NodeId, usize>,
    start: NodeId,
}

impl GraphIndex {
    fn build(workflow: &WorkflowDefinition) -> Self {
        let nodes: HashMap<NodeId, Node> = workflow
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.clone()))
            .collect();

        let mut outgoing: HashMap<NodeId, Vec<FlowEdge>> = HashMap::new();
        let mut outgoing_error: HashMap<NodeId, Vec<FlowEdge>> = HashMap::new();
        let mut incoming_count: HashMap<NodeId, usize> = HashMap::new();

        for edge in &workflow.edges {
            match edge.on {
                Some(EdgeTrigger::Error) => {
                    outgoing_error
                        .entry(edge.from.clone())
                        .or_default()
                        .push(edge.clone());
                }
                None => {
                    outgoing
                        .entry(edge.from.clone())
                        .or_default()
                        .push(edge.clone());
                    *incoming_count.entry(edge.to.clone()).or_default() += 1;
                }
            }
        }

        let start = workflow
            .nodes
            .iter()
            .find(|n| matches!(n.kind, NodeKind::Start(_)))
            .map(|n| n.id.clone())
            .expect("validación garantiza un start");

        Self {
            nodes,
            outgoing,
            outgoing_error,
            incoming_count,
            start,
        }
    }

    fn node(&self, id: &NodeId) -> &Node {
        self.nodes
            .get(id)
            .expect("validación garantiza referencias")
    }

    fn outgoing_edges(&self, id: &NodeId) -> &[FlowEdge] {
        self.outgoing.get(id).map(Vec::as_slice).unwrap_or(&[])
    }

    fn error_edges(&self, id: &NodeId) -> &[FlowEdge] {
        self.outgoing_error
            .get(id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

/// Validadores JSON Schema precompilados al construir el executor.
/// Los schemas son estáticos por definición: compilarlos en cada ejecución
/// de nodo (y en cada retry) es trabajo repetido.
#[derive(Default)]
struct CompiledSchemas {
    /// Validadores de nodos start (trigger) y end (resultado final)
    nodes: HashMap<NodeId, jsonschema::Validator>,
    /// Validadores (input, output) por tarea referenciada en el workflow
    tasks: HashMap<TaskId, (Option<jsonschema::Validator>, Option<jsonschema::Validator>)>,
}

impl CompiledSchemas {
    fn build(
        workflow: &WorkflowDefinition,
        registry: &TaskRegistry,
    ) -> Result<Self, Vec<WorkflowError>> {
        let mut compiled = Self::default();
        let mut errors: Vec<WorkflowError> = Vec::new();

        let mut compile =
            |schema: &schemars::Schema, where_: String| match serde_json::to_value(schema)
                .map_err(|e| e.to_string())
                .and_then(|json| jsonschema::validator_for(&json).map_err(|e| e.to_string()))
            {
                Ok(validator) => Some(validator),
                Err(e) => {
                    errors.push(WorkflowError::new(
                        "INVALID_SCHEMA",
                        format!("Schema inválido en {where_}: {e}"),
                    ));
                    None
                }
            };

        for node in &workflow.nodes {
            let schema = match &node.kind {
                NodeKind::Start(start) => start.schema.as_ref(),
                NodeKind::End(end) => end.schema.as_ref(),
                _ => None,
            };
            if let Some(schema) = schema
                && let Some(validator) = compile(schema, format!("el nodo '{}'", node.id))
            {
                compiled.nodes.insert(node.id.clone(), validator);
            }

            if let NodeKind::Task(task_node) = &node.kind
                && !compiled.tasks.contains_key(&task_node.task)
                && let Some(task) = registry.get(&task_node.task)
            {
                let manifest = task.manifest();
                let input = manifest
                    .input_schema
                    .as_ref()
                    .and_then(|s| compile(s, format!("el input de la tarea '{}'", manifest.id)));
                let output = manifest
                    .output_schema
                    .as_ref()
                    .and_then(|s| compile(s, format!("el output de la tarea '{}'", manifest.id)));
                compiled
                    .tasks
                    .insert(task_node.task.clone(), (input, output));
            }
        }

        if errors.is_empty() {
            Ok(compiled)
        } else {
            Err(errors)
        }
    }
}

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
}

impl WorkflowExecutor {
    /// Construye un executor validando la estructura del grafo, que toda
    /// tarea referenciada esté registrada y precompilando los JSON Schemas.
    pub fn new(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
    ) -> Result<Self, Vec<WorkflowError>> {
        crate::validation::validate(&workflow)?;
        crate::validation::validate_tasks(&workflow, &registry)?;
        let schemas = CompiledSchemas::build(&workflow, &registry)?;
        let index = GraphIndex::build(&workflow);
        Ok(Self {
            workflow,
            registry,
            index,
            schemas,
        })
    }

    /// Ejecuta el workflow hasta terminar o fallar (run-to-completion).
    pub async fn run(&self, trigger: WorkflowData) -> WorkflowResult {
        let ctx = WorkflowContext::new(&self.workflow, trigger.0.clone());
        info!(
            execution_id = %ctx.execution_id(),
            name = %self.workflow.name,
            "Iniciando ejecución de workflow"
        );

        let state = RunState {
            joins: Mutex::new(HashMap::new()),
            ends: Mutex::new(Vec::new()),
        };

        self.execute_from(&self.index.start, Arc::new(trigger.0), None, &ctx, &state)
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
                    state
                        .ends
                        .lock()
                        .expect("RunState lock poisoned")
                        .push((node_id.clone(), result));
                    Ok(())
                }

                NodeKind::Task(task_node) => {
                    match self.run_task(node, task_node, carried, ctx).await {
                        Ok(output) => {
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
                            if error_edges.is_empty() {
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

                NodeKind::Gateway(gateway) => {
                    self.run_gateway(node, gateway, carried, origin, ctx, state)
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

        let (input_validator, output_validator) = self
            .schemas
            .tasks
            .get(&task_node.task)
            .map(|(i, o)| (i.as_ref(), o.as_ref()))
            .unwrap_or((None, None));

        if let Some(validator) = input_validator {
            validate_compiled(validator, &input).map_err(|e| {
                WorkflowError::new(
                    "TASK_INPUT_INVALID",
                    format!(
                        "El input de la tarea '{}' no cumple su schema: {e}",
                        task_node.task
                    ),
                )
                .with_source_task(node.id.to_string())
            })?;
        }

        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let execution = task.execute(ctx, WorkflowData(input.clone()));
            let result = match task_node.timeout_ms {
                Some(ms) => {
                    match tokio::time::timeout(Duration::from_millis(ms), execution).await {
                        Ok(result) => result,
                        Err(_) => Err(WorkflowError::new(
                            "TASK_TIMEOUT",
                            format!("La tarea '{}' superó el timeout de {ms}ms", task_node.task),
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
                                    "El output de la tarea '{}' no cumple su schema: {e}",
                                    task_node.task
                                ),
                            )
                            .with_source_task(node.id.to_string())
                        })?;
                    }
                    return Ok(output.0);
                }
                Err(mut err) => {
                    if err.source_task.is_none() {
                        err.source_task = Some(node.id.to_string());
                    }
                    let retries_left = task_node.retry.as_ref().is_some_and(|r| attempt <= r.max);
                    if !retries_left {
                        return Err(err);
                    }
                    let retry = task_node.retry.as_ref().expect("retries_left lo implica");
                    let delay = backoff_delay(retry, attempt);
                    warn!(
                        node_id = %node.id,
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

    /// Ejecuta un gateway según su tipo.
    async fn run_gateway(
        &self,
        node: &Node,
        gateway: &GatewayNode,
        carried: Arc<Value>,
        origin: Option<NodeId>,
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

/// Valida un `Value` contra un validador precompilado.
fn validate_compiled(validator: &jsonschema::Validator, data: &Value) -> Result<(), String> {
    if validator.is_valid(data) {
        Ok(())
    } else {
        let errors: Vec<String> = validator.iter_errors(data).map(|e| e.to_string()).collect();
        Err(errors.join("; "))
    }
}
