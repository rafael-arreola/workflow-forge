//! El orquestador: construcción validada del executor y recorrido del grafo.
//!
//! La semántica de cada kind de nodo vive en [`crate::runtime::handlers`];
//! la política de retry/timeout/panic en [`crate::runtime::policy`]. Este
//! módulo solo conoce el recorrido: dispatch por kind, continuación por
//! aristas (normales o de error) y cierre de la ejecución.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures::future::{BoxFuture, try_join_all};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::error::{WorkflowError, codes};
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

/// Tokens que llegaron a un join, identificados por su nodo de origen.
pub(crate) type JoinArrivals = Vec<(NodeId, Arc<Value>)>;

/// Niveles máximos de anidamiento de sub-workflows (la raíz es el nivel 1)
const MAX_SUBWORKFLOW_DEPTH: usize = 8;

/// Estado mutable de una ejecución en curso.
pub(crate) struct RunState {
    /// Llegadas acumuladas por cada gateway join aún incompleto
    pub(crate) joins: Mutex<HashMap<NodeId, JoinArrivals>>,
    /// Resultados de los nodos end ejecutados
    pub(crate) ends: Mutex<Vec<(NodeId, Value)>>,
}

/// Motor de ejecución de workflows según la spec 1.0: contexto global,
/// mappings JSONPath, gateways, retry/timeout y rutas de error.
pub struct WorkflowExecutor {
    pub(crate) workflow: WorkflowDefinition,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) index: GraphIndex,
    pub(crate) schemas: CompiledSchemas,
    pub(crate) observer: Option<Arc<dyn ExecutionObserver>>,
    /// Fábrica del almacenamiento de blobs de cada ejecución
    /// (default: [`TempDirBlobFactory`])
    pub(crate) blobs: Arc<dyn BlobStoreFactory>,
    /// Executors hijos, uno por nodo `kind: "subworkflow"`, resueltos y
    /// validados al construir (la construcción falla si un nombre no existe,
    /// hay un ciclo o se supera la profundidad máxima)
    pub(crate) subworkflows: HashMap<NodeId, WorkflowExecutor>,
}

/// Construcción de un [`WorkflowExecutor`]: el punto único de inyección de
/// dependencias del runtime.
///
/// | Método | Dependencia | Default |
/// |---|---|---|
/// | [`secrets`](Self::secrets) | [`SecretProvider`] para `{"$secret": "X"}` | variables de entorno |
/// | [`workflows`](Self::workflows) | [`WorkflowRegistry`] de sub-workflows | ninguno |
/// | [`blobs`](Self::blobs) | [`BlobStoreFactory`] del almacenamiento `$blob` | directorio temporal |
/// | [`observer`](Self::observer) | [`ExecutionObserver`] de eventos | ninguno |
/// | [`rule`](Self::rule) | [`ValidationRule`] adicionales del host | solo las integradas |
pub struct WorkflowExecutorBuilder<'s> {
    workflow: WorkflowDefinition,
    registry: Arc<TaskRegistry>,
    secrets: &'s dyn SecretProvider,
    workflows: Option<Arc<WorkflowRegistry>>,
    blobs: Option<Arc<dyn BlobStoreFactory>>,
    observer: Option<Arc<dyn ExecutionObserver>>,
    rules: Vec<Box<dyn ValidationRule>>,
}

impl<'s> WorkflowExecutorBuilder<'s> {
    /// Provider de secretos para los `{"$secret": "X"}` de perfiles inline
    /// (default: variables de entorno)
    pub fn secrets<'n>(self, secrets: &'n dyn SecretProvider) -> WorkflowExecutorBuilder<'n> {
        WorkflowExecutorBuilder {
            workflow: self.workflow,
            registry: self.registry,
            secrets,
            workflows: self.workflows,
            blobs: self.blobs,
            observer: self.observer,
            rules: self.rules,
        }
    }

    /// Registro compartido de sub-workflows. La sección `workflows` inline
    /// del documento tiene precedencia sobre este registro.
    pub fn workflows(mut self, workflows: Arc<WorkflowRegistry>) -> Self {
        self.workflows = Some(workflows);
        self
    }

    /// Fábrica del almacenamiento de blobs por ejecución
    /// (default: [`TempDirBlobFactory`])
    pub fn blobs(mut self, blobs: Arc<dyn BlobStoreFactory>) -> Self {
        self.blobs = Some(blobs);
        self
    }

    /// Observer que recibirá los eventos de cada ejecución, incluidas las
    /// de sub-workflows (equivalente a [`WorkflowExecutor::with_observer`])
    pub fn observer(mut self, observer: Arc<dyn ExecutionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Agrega una regla de validación del host. Corre después de las
    /// integradas, también sobre cada sub-workflow (inline o del registro).
    pub fn rule<R: ValidationRule + 'static>(mut self, rule: R) -> Self {
        self.rules.push(Box::new(rule));
        self
    }

    /// Construye el executor (validación estructural, tareas registradas,
    /// schemas precompilados y sub-workflows resueltos recursivamente)
    pub fn build(self) -> Result<WorkflowExecutor, Vec<WorkflowError>> {
        let mut executor = WorkflowExecutor::build_internal(
            self.workflow,
            self.registry,
            self.secrets,
            self.workflows.as_deref(),
            &self.rules,
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
    /// Construye un executor validando la estructura del grafo, que toda
    /// tarea referenciada esté registrada y precompilando los JSON Schemas.
    /// Los secretos de perfiles inline se resuelven con variables de entorno
    /// ([`crate::io::secret::EnvSecrets`]); para otro provider usar
    /// [`WorkflowExecutor::new_with_secrets`].
    pub fn new(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
    ) -> Result<Self, Vec<WorkflowError>> {
        Self::new_with_secrets(workflow, registry, &crate::io::secret::EnvSecrets)
    }

    /// Como [`WorkflowExecutor::new`], con un [`SecretProvider`] explícito
    /// para los `{"$secret": "X"}` de los perfiles inline del workflow.
    pub fn new_with_secrets(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
        secrets: &dyn SecretProvider,
    ) -> Result<Self, Vec<WorkflowError>> {
        Self::build_internal(workflow, registry, secrets, None, &[], &mut Vec::new())
    }

    /// Constructor con inyección de dependencias (secretos, sub-workflows,
    /// blobs, observer, reglas). Ver [`WorkflowExecutorBuilder`].
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
        }
    }

    fn build_internal(
        workflow: WorkflowDefinition,
        registry: Arc<TaskRegistry>,
        secrets: &dyn SecretProvider,
        shared: Option<&WorkflowRegistry>,
        rules: &[Box<dyn ValidationRule>],
        ancestry: &mut Vec<String>,
    ) -> Result<Self, Vec<WorkflowError>> {
        // Los hijos se construyen con el registry original: los perfiles
        // inline de un documento son locales a ese documento
        let original_registry = Arc::clone(&registry);

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

        let rule_refs: Vec<&dyn ValidationRule> = rules.iter().map(AsRef::as_ref).collect();
        crate::validate::validate_with(&workflow, &rule_refs)?;
        crate::validate::validate_tasks(&workflow, &registry)?;
        let schemas = CompiledSchemas::build(&workflow, &registry)?;
        let index = GraphIndex::build(&workflow);

        // Resolver los nodos subworkflow: inline primero, registro después.
        // Cada hijo se construye (y valida) recursivamente aquí, de modo que
        // un nombre inexistente, un ciclo o un hijo inválido fallan en la
        // construcción del padre, nunca en runtime.
        let mut subworkflows = HashMap::new();
        let mut errors: Vec<WorkflowError> = Vec::new();
        let sub_nodes: Vec<(&NodeId, &SubworkflowNode)> = workflow
            .nodes
            .iter()
            .filter_map(|node| match &node.kind {
                NodeKind::Subworkflow(sub) => Some((&node.id, sub)),
                _ => None,
            })
            .collect();
        if !sub_nodes.is_empty() {
            if ancestry.len() + 1 >= MAX_SUBWORKFLOW_DEPTH {
                errors.push(WorkflowError::new(
                    codes::SUBWORKFLOW_DEPTH_EXCEEDED,
                    format!(
                        "El workflow '{}' anida sub-workflows más allá de la \
                         profundidad máxima ({MAX_SUBWORKFLOW_DEPTH} niveles)",
                        workflow.name
                    ),
                ));
            } else {
                ancestry.push(workflow.name.clone());
                for (node_id, sub) in sub_nodes {
                    if ancestry.contains(&sub.workflow) {
                        errors.push(
                            WorkflowError::new(
                                codes::SUBWORKFLOW_CYCLE,
                                format!(
                                    "El nodo '{}' referencia el workflow '{}', que ya está \
                                     en la cadena de ejecución ({})",
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
                                    "El nodo '{}' referencia el workflow '{}', que no está en \
                                     la sección `workflows` del documento ni en el registro",
                                    node_id, sub.workflow
                                ),
                            )
                            .with_source_task(node_id.to_string()),
                        );
                        continue;
                    };
                    match Self::build_internal(
                        definition,
                        Arc::clone(&original_registry),
                        secrets,
                        shared,
                        rules,
                        ancestry,
                    ) {
                        Ok(child) => {
                            subworkflows.insert(node_id.clone(), child);
                        }
                        Err(child_errors) => {
                            errors.extend(child_errors.into_iter().map(|mut e| {
                                e.message =
                                    format!("en el sub-workflow '{}': {}", sub.workflow, e.message);
                                e
                            }));
                        }
                    }
                }
                ancestry.pop();
            }
        }
        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(Self {
            workflow,
            registry,
            index,
            schemas,
            observer: None,
            blobs: Arc::new(TempDirBlobFactory),
            subworkflows,
        })
    }

    /// Registra un observer que recibirá los eventos de cada ejecución
    /// ([`crate::observe::ExecutionEvent`]), incluidas las de sub-workflows.
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

    /// Emite un evento al observer, si hay uno registrado
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

    /// Ejecuta el workflow hasta terminar o fallar (run-to-completion).
    pub async fn run(&self, trigger: WorkflowData) -> WorkflowResult {
        let ctx = WorkflowContext::with_blob_factory(
            &self.workflow,
            trigger.0.clone(),
            self.blobs.as_ref(),
        );
        let result = self.run_with_ctx(trigger, &ctx).await;
        // Solo la ejecución raíz limpia los blobs: los sub-workflows
        // comparten este store y sus referencias pueden cruzar la frontera
        if let Err(e) = ctx.blobs().cleanup().await {
            warn!(code = %e.code, message = %e.message, "No se pudieron limpiar los blobs");
        }
        result
    }

    /// Cuerpo común de una ejecución (raíz o sub-workflow): eventos de
    /// inicio/fin alrededor de `run_inner`, sin limpieza de blobs.
    pub(crate) async fn run_with_ctx(
        &self,
        trigger: WorkflowData,
        ctx: &WorkflowContext,
    ) -> WorkflowResult {
        info!(
            execution_id = %ctx.execution_id(),
            name = %self.workflow.name,
            "Iniciando ejecución de workflow"
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
                codes::JOIN_INCOMPLETE,
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
                codes::NO_OUTPUT,
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

    /// Continúa el recorrido por un conjunto de aristas. Varias aristas se
    /// recorren concurrentemente; el primer error cancela las ramas hermanas.
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

    /// Cierra la ejecución de un nodo que invoca tareas (task, foreach o
    /// subworkflow): publica output y continúa, o sigue la ruta de error
    /// (`on: error` / `on: panic`) si existe.
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
                // Un panic solo rutea por `on: panic` (sin fallback a error:
                // pudo dejar efectos a medias); el resto rutea por `on: error`
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
                // Ruta alternativa: el error serializado viaja como token
                warn!(node_id = %node_id, code = %err.code, "Nodo falló; siguiendo ruta alternativa");
                let error_value = serde_json::to_value(&err).unwrap_or(Value::Null);
                ctx.set_node_error(node_id, error_value.clone());
                self.continue_through(route_edges, Arc::new(error_value), ctx, state)
                    .await
            }
        }
    }
}
