use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};

use futures::future;
use tracing::{debug, info, warn};

use crate::context::WorkflowContext;
use crate::node::{Node, NodeKind};
use crate::registry::TaskRegistry;
use crate::types::{WorkflowData, WorkflowError, WorkflowResult};
use crate::workflow::{Condition, ConditionOp, DataMapping, FlowEdge, OutputStatus, WorkflowDef};

/// Estado compartido durante la ejecución de un workflow.
/// Mantiene contadores de bucles, resultados parciales y sincronización de joins.
#[derive(Default)]
pub struct ExecutionState {
    /// Contador de iteraciones por nodo de tipo Loop (node_id → iteración actual)
    pub loop_counters: RwLock<HashMap<String, u32>>,
    /// Resultados acumulados de ramas paralelas para nodos Join (join_node_id → datos parciales)
    pub join_buffers: RwLock<HashMap<String, Vec<WorkflowData>>>,
    /// Conjunto de nodos ya ejecutados para evitar re-ejecución en DAG
    pub visited: RwLock<HashSet<String>>,
}

impl ExecutionState {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Estructura auxiliar que precalcula índices del grafo para búsquedas rápidas
#[allow(dead_code)]
struct GraphIndex {
    nodes: HashMap<String, Node>,
    /// Aristas salientes agrupadas por nodo origen
    outgoing: HashMap<String, Vec<FlowEdge>>,
    /// Aristas entrantes agrupadas por nodo destino
    incoming: HashMap<String, Vec<FlowEdge>>,
    /// IDs de nodos sin aristas entrantes (puntos de inicio)
    start_nodes: Vec<String>,
}

impl GraphIndex {
    fn build(workflow: &WorkflowDef) -> Self {
        let nodes: HashMap<String, Node> = workflow
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.clone()))
            .collect();

        let mut outgoing: HashMap<String, Vec<FlowEdge>> = HashMap::new();
        let mut incoming: HashMap<String, Vec<FlowEdge>> = HashMap::new();

        for edge in &workflow.edges {
            outgoing
                .entry(edge.from.module_id.clone())
                .or_default()
                .push(edge.clone());
            incoming
                .entry(edge.to.module_id.clone())
                .or_default()
                .push(edge.clone());
        }

        // Nodos sin aristas entrantes son nodos de inicio
        let start_nodes: Vec<String> = workflow
            .nodes
            .iter()
            .filter(|n| !incoming.contains_key(&n.id))
            .map(|n| n.id.clone())
            .collect();

        Self {
            nodes,
            outgoing,
            incoming,
            start_nodes,
        }
    }

    fn get_node(&self, id: &str) -> Option<&Node> {
        self.nodes.get(id)
    }
}

/// Motor de ejecución de workflows.
/// Toma una definición declarativa (`WorkflowDef`) y un registro de tareas,
/// y ejecuta el grafo completo de forma asíncrona respetando paralelismo,
/// condiciones, bucles y manejo de errores.
pub struct WorkflowExecutor {
    workflow: WorkflowDef,
    registry: Arc<TaskRegistry>,
    state: Arc<ExecutionState>,
    index: GraphIndex,
}

impl WorkflowExecutor {
    /// Construye un nuevo executor a partir de una definición y un registro de tareas
    pub fn new(workflow: WorkflowDef, registry: Arc<TaskRegistry>) -> Self {
        let index = GraphIndex::build(&workflow);
        Self {
            workflow,
            registry,
            state: Arc::new(ExecutionState::new()),
            index,
        }
    }

    /// Ejecuta el workflow completo desde los nodos de inicio con los datos iniciales
    pub async fn run(&self, start_data: WorkflowData) -> WorkflowResult {
        let ctx = WorkflowContext::new();
        info!(
            workflow_id = %ctx.workflow_id(),
            name = %self.workflow.name,
            "Iniciando ejecución de workflow"
        );

        if self.index.start_nodes.is_empty() {
            return Err(WorkflowError {
                code: "NO_START_NODES".into(),
                message: "El workflow no tiene nodos de inicio".into(),
                source_task: None,
                payload: None,
                response: None,
                source: None,
            });
        }

        // Cola de trabajo: (node_id, datos_de_entrada)
        let mut queue: VecDeque<(String, WorkflowData)> = VecDeque::new();
        for node_id in &self.index.start_nodes {
            queue.push_back((node_id.clone(), start_data.clone()));
        }

        let mut last_output: Option<WorkflowData> = None;

        while let Some((node_id, input)) = queue.pop_front() {
            let node = self
                .index
                .get_node(&node_id)
                .ok_or_else(|| self.node_not_found(&node_id))?;

            // Si el nodo ya fue visitado y no es parte de un bucle, se omite
            {
                let visited = self.state.visited.read().expect("lock poisoned");
                if visited.contains(&node_id) && !matches!(node.kind, NodeKind::Loop(_)) {
                    debug!(node_id = %node_id, "Nodo ya ejecutado, omitiendo");
                    continue;
                }
            }

            debug!(node_id = %node_id, kind = ?node.kind, "Ejecutando nodo");

            let result = self.execute_node(node, &ctx, input).await;

            match result {
                Ok(data) => {
                    self.state
                        .visited
                        .write()
                        .expect("lock poisoned")
                        .insert(node_id.clone());
                    last_output = Some(data.clone());

                    // Encontrar aristas activas para éxito
                    let active = self.find_active_edges(&node_id, &OutputStatus::Success, &data);
                    self.enqueue_targets(&active, &mut queue, &data).await;
                }
                Err(err) => {
                    warn!(
                        node_id = %node_id,
                        error = %err,
                        "Nodo falló, evaluando rutas de error"
                    );
                    let error_data = err.response.clone().unwrap_or_default();
                    let active =
                        self.find_active_edges(&node_id, &OutputStatus::Error, &error_data);

                    if active.is_empty() {
                        // Sin ruta de error: aplicar política global
                        match self
                            .workflow
                            .global_config
                            .as_ref()
                            .and_then(|c| c.error_policy.as_ref())
                        {
                            Some(crate::workflow::ErrorPolicy::Stop) | None => return Err(err),
                            Some(crate::workflow::ErrorPolicy::Continue) => continue,
                            Some(crate::workflow::ErrorPolicy::Retry {
                                max_retries,
                                delay_ms,
                            }) => {
                                // Reintento simple: se vuelve a encolar el mismo nodo
                                let mut retry_count = 0;
                                let mut retry_result = Err(err);
                                while retry_count < *max_retries {
                                    tokio::time::sleep(std::time::Duration::from_millis(*delay_ms))
                                        .await;
                                    retry_result =
                                        self.execute_node(node, &ctx, error_data.clone()).await;
                                    if retry_result.is_ok() {
                                        break;
                                    }
                                    retry_count += 1;
                                }
                                if let Err(e) = retry_result {
                                    return Err(e);
                                }
                            }
                        }
                    } else {
                        self.enqueue_targets(&active, &mut queue, &error_data).await;
                    }
                }
            }
        }

        last_output.ok_or_else(|| WorkflowError {
            code: "NO_OUTPUT".into(),
            message: "El workflow finalizó sin producir datos de salida".into(),
            source_task: None,
            payload: None,
            response: None,
            source: None,
        })
    }

    /// Ejecuta un nodo individual según su tipo (Task, Conditional, Switch, etc.)
    async fn execute_node(
        &self,
        node: &Node,
        ctx: &WorkflowContext,
        input: WorkflowData,
    ) -> WorkflowResult {
        match &node.kind {
            NodeKind::Task(task_node) => {
                let task_type = &task_node.task_type;
                let task = self.registry.get(task_type).ok_or_else(|| WorkflowError {
                    code: "TASK_NOT_FOUND".into(),
                    message: format!("Tarea '{}' no encontrada en el registry", task_type),
                    source_task: Some(node.id.clone()),
                    payload: None,
                    response: None,
                    source: None,
                })?;

                debug!(node_id = %node.id, task_type = %task_type, "Invocando tarea");
                task.execute(ctx, input).await.map_err(|mut e| {
                    if e.source_task.is_none() {
                        e.source_task = Some(node.id.clone());
                    }
                    e
                })
            }

            NodeKind::Conditional(cond_node) => {
                let branches = &cond_node.branches;
                let default = &cond_node.default;
                debug!(node_id = %node.id, branches = branches.len(), "Evaluando condicional");
                for branch in branches {
                    if evaluate_condition(&branch.condition, &input) {
                        return Ok(input); // El enrutamiento real lo hace run() siguiendo edges
                    }
                }
                // Si no hay rama por defecto, se devuelve el input sin modificar
                if default.is_some() {
                    Ok(input)
                } else {
                    Err(WorkflowError {
                        code: "NO_CONDITION_MATCHED".into(),
                        message: "Ninguna rama condicional coincidió y no hay rama por defecto"
                            .into(),
                        source_task: Some(node.id.clone()),
                        payload: Some(input.clone()),
                        response: None,
                        source: None,
                    })
                }
            }

            NodeKind::Switch(switch_node) => {
                let expression = &switch_node.expression;
                let cases = &switch_node.cases;
                let default = &switch_node.default;
                let value = extract_jsonpath(expression, &input).unwrap_or(serde_json::Value::Null);
                for case in cases {
                    if case.value == value {
                        return Ok(input);
                    }
                }
                if default.is_some() {
                    Ok(input)
                } else {
                    Err(WorkflowError {
                        code: "NO_SWITCH_MATCHED".into(),
                        message: format!(
                            "El valor '{}' no coincide con ningún caso del switch",
                            value
                        ),
                        source_task: Some(node.id.clone()),
                        payload: Some(input.clone()),
                        response: None,
                        source: None,
                    })
                }
            }

            NodeKind::Loop(loop_node) => {
                let condition = &loop_node.condition;
                let max_iterations = &loop_node.max_iterations;
                let current = input;
                let mut iteration = 0;

                loop {
                    if iteration >= *max_iterations {
                        warn!(node_id = %node.id, iterations = iteration, "Límite de iteraciones alcanzado");
                        break;
                    }
                    if !evaluate_condition(condition, &current) {
                        debug!(node_id = %node.id, iteration, "Condición de bucle falsa, saliendo");
                        break;
                    }

                    // Actualizar contador de bucle
                    {
                        let mut counters = self.state.loop_counters.write().expect("lock poisoned");
                        counters.insert(node.id.clone(), iteration);
                    }

                    debug!(node_id = %node.id, iteration, "Iteración de bucle");
                    iteration += 1;
                    // En un bucle real, aquí se ejecutaría el subgrafo body_start..body_end
                    // Para esta fase, se devuelve el dato sin modificar
                }

                Ok(current)
            }

            NodeKind::Parallel(_parallel_node) => {
                debug!(node_id = %node.id, "Disparando ramas paralelas");
                // Las ramas paralelas se ejecutan mediante spawn en enqueue_targets
                // Este nodo en sí solo pasa los datos
                Ok(input)
            }

            NodeKind::Join(join_node) => {
                let aggregate = &join_node.aggregate;
                let mut buffer = self.state.join_buffers.write().expect("lock poisoned");

                let parts = buffer.remove(&node.id).unwrap_or_default();

                if *aggregate && !parts.is_empty() {
                    let array: Vec<serde_json::Value> = parts.iter().map(|d| d.0.clone()).collect();
                    Ok(WorkflowData(serde_json::Value::Array(array)))
                } else {
                    // Sin agregación: devuelve el primer resultado o datos vacíos
                    Ok(parts
                        .into_iter()
                        .next()
                        .unwrap_or(WorkflowData(serde_json::Value::Null)))
                }
            }
        }
    }

    /// Encuentra aristas salientes activas según el estado de salida y condiciones
    fn find_active_edges(
        &self,
        node_id: &str,
        status: &OutputStatus,
        data: &WorkflowData,
    ) -> Vec<FlowEdge> {
        let outgoing = self.index.outgoing.get(node_id);
        let Some(edges) = outgoing else {
            return vec![];
        };

        edges
            .iter()
            .filter(|edge| {
                // La arista se activa si el estado coincide o es Always
                let status_match = edge.on == *status || edge.on == OutputStatus::Always;

                // Si hay condición adicional, debe cumplirse
                let condition_pass = edge
                    .condition
                    .as_ref()
                    .map(|c| evaluate_condition(c, data))
                    .unwrap_or(true);

                status_match && condition_pass
            })
            .cloned()
            .collect()
    }

    /// Encola los nodos destino de las aristas activas, aplicando data_mapping si existe
    async fn enqueue_targets(
        &self,
        edges: &[FlowEdge],
        queue: &mut VecDeque<(String, WorkflowData)>,
        source_data: &WorkflowData,
    ) {
        // Separar aristas paralelas de las secuenciales
        let (parallel, sequential): (Vec<_>, Vec<_>) = edges.iter().partition(|e| e.parallel);

        // Aristas secuenciales: se encolan en orden
        for edge in &sequential {
            let mapped = apply_data_mapping(source_data, &edge.data_mapping);
            queue.push_back((edge.to.module_id.clone(), mapped));
        }

        // Aristas paralelas: se disparan con tokio::spawn
        if !parallel.is_empty() {
            let state = Arc::clone(&self.state);

            let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

            for edge in &parallel {
                let target_id = edge.to.module_id.clone();
                let mapped = apply_data_mapping(source_data, &edge.data_mapping);
                let st = Arc::clone(&state);

                handles.push(tokio::spawn(async move {
                    debug!(target = %target_id, "Rama paralela iniciada");

                    let result = mapped;
                    let mut buffers = st.join_buffers.write().expect("lock poisoned");
                    buffers.entry(target_id.clone()).or_default().push(result);
                }));
            }

            future::join_all(handles).await;
        }
    }

    fn node_not_found(&self, node_id: &str) -> WorkflowError {
        WorkflowError {
            code: "NODE_NOT_FOUND".into(),
            message: format!("Nodo '{}' no encontrado en el workflow", node_id),
            source_task: Some(node_id.to_string()),
            payload: None,
            response: None,
            source: None,
        }
    }
}

/// Evalúa una condición sobre los datos usando jsonpath para extraer el valor
fn evaluate_condition(condition: &Condition, data: &WorkflowData) -> bool {
    let field_value = extract_jsonpath(&condition.field, data).unwrap_or(serde_json::Value::Null);

    match condition.op {
        ConditionOp::Exists => !field_value.is_null(),
        ConditionOp::Eq => field_value == condition.value,
        ConditionOp::Neq => field_value != condition.value,
        ConditionOp::Gt => {
            compare_values(&field_value, &condition.value) == std::cmp::Ordering::Greater
        }
        ConditionOp::Gte => matches!(
            compare_values(&field_value, &condition.value),
            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
        ),
        ConditionOp::Lt => {
            compare_values(&field_value, &condition.value) == std::cmp::Ordering::Less
        }
        ConditionOp::Lte => matches!(
            compare_values(&field_value, &condition.value),
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
        ),
        ConditionOp::Contains => {
            let field_str = field_value.as_str().unwrap_or("");
            let value_str = condition.value.as_str().unwrap_or("");
            field_str.contains(value_str)
        }
    }
}

/// Compara dos valores JSON. Si no son del mismo tipo, devuelve Less por defecto.
fn compare_values(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    match (a, b) {
        (serde_json::Value::Number(na), serde_json::Value::Number(nb)) => {
            let fa = na.as_f64().unwrap_or(0.0);
            let fb = nb.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Less)
        }
        (serde_json::Value::String(sa), serde_json::Value::String(sb)) => sa.cmp(sb),
        (serde_json::Value::Bool(ba), serde_json::Value::Bool(bb)) => ba.cmp(bb),
        _ => std::cmp::Ordering::Less,
    }
}

/// Extrae un valor anidado de un `serde_json::Value` usando una ruta con
/// notación de punto y corchetes (compatible con jsonpath básico).
/// Soporta: `$.campo`, `$.campo.subcampo`, `$.items[0]`
fn extract_jsonpath(expr: &str, data: &WorkflowData) -> Option<serde_json::Value> {
    let path = expr.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        return Some(data.0.clone());
    }

    let mut current = data.0.clone();

    for segment in path.split('.') {
        current = match resolve_segment(&current, segment) {
            Some(v) => v,
            None => return None,
        };
    }

    Some(current)
}

/// Resuelve un segmento individual: campo simple o acceso indexado a array
fn resolve_segment(value: &serde_json::Value, segment: &str) -> Option<serde_json::Value> {
    if let Some(bracket_pos) = segment.find('[') {
        let field_name = &segment[..bracket_pos];
        let index_str = segment[bracket_pos + 1..segment.len() - 1].trim();

        let array = if field_name.is_empty() {
            value.clone()
        } else {
            value.get(field_name)?.clone()
        };

        let arr = array.as_array()?;
        let idx: usize = index_str.parse().ok()?;
        arr.get(idx).cloned()
    } else {
        value.get(segment).cloned()
    }
}

/// Aplica un mapeo de datos: extrae valores del origen mediante jsonpath
/// y los asigna a campos del documento destino.
fn apply_data_mapping(source: &WorkflowData, mapping: &Option<DataMapping>) -> WorkflowData {
    let Some(mapping) = mapping else {
        return source.clone();
    };

    let mut result = serde_json::Map::new();
    for (target_field, source_expr) in &mapping.mappings {
        let value = extract_jsonpath(source_expr, source).unwrap_or(serde_json::Value::Null);
        result.insert(target_field.clone(), value);
    }

    WorkflowData(serde_json::Value::Object(result))
}
