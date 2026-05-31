use std::collections::{HashMap, HashSet, VecDeque};

use tracing::{debug, info};

use crate::context::WorkflowContext;
use crate::node::{Node, NodeId, NodeKind};
use crate::registry::TaskRegistry;
use crate::types::{WorkflowData, WorkflowResult};
use crate::workflow::{FlowEdge, WorkflowDefinition};

/// Estructura auxiliar que precalcula índices del grafo para búsquedas rápidas.
struct GraphIndex {
    /// Mapa de NodeId → Node
    nodes: HashMap<NodeId, Node>,
    /// Aristas salientes agrupadas por nodo origen
    outgoing: HashMap<NodeId, Vec<FlowEdge>>,
    /// Aristas entrantes agrupadas por nodo destino
    #[allow(dead_code)]
    incoming: HashMap<NodeId, Vec<FlowEdge>>,
    /// IDs de nodos sin aristas entrantes (puntos de inicio del grafo)
    start_nodes: Vec<NodeId>,
}

impl GraphIndex {
    fn build(workflow: &WorkflowDefinition) -> Self {
        let nodes: HashMap<NodeId, Node> = workflow
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.clone()))
            .collect();

        let mut outgoing: HashMap<NodeId, Vec<FlowEdge>> = HashMap::new();
        let mut incoming: HashMap<NodeId, Vec<FlowEdge>> = HashMap::new();

        for edge in &workflow.edges {
            outgoing
                .entry(edge.from.clone())
                .or_default()
                .push(edge.clone());
            incoming
                .entry(edge.to.clone())
                .or_default()
                .push(edge.clone());
        }

        let start_nodes: Vec<NodeId> = workflow
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

    fn get_node(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.get(id)
    }

    fn outgoing_edges(&self, node_id: &NodeId) -> &[FlowEdge] {
        static EMPTY: &[FlowEdge] = &[];
        self.outgoing
            .get(node_id)
            .map(|v| v.as_slice())
            .unwrap_or(EMPTY)
    }
}

/// Motor de ejecución de workflows.
/// Toma una definición declarativa (`WorkflowDefinition`) y un registro de tareas,
/// y ejecuta el grafo de forma secuencial siguiendo las aristas del grafo.
pub struct WorkflowExecutor {
    workflow: WorkflowDefinition,
    registry: std::sync::Arc<TaskRegistry>,
    index: GraphIndex,
}

impl WorkflowExecutor {
    /// Construye un nuevo executor a partir de una definición y un registro de tareas
    pub fn new(workflow: WorkflowDefinition, registry: std::sync::Arc<TaskRegistry>) -> Self {
        let index = GraphIndex::build(&workflow);
        Self {
            workflow,
            registry,
            index,
        }
    }

    /// Ejecuta el workflow completo desde los nodos de inicio con los datos iniciales.
    /// Recorre el grafo en orden BFS respetando las aristas dirigidas.
    pub async fn run(&self, start_data: WorkflowData) -> WorkflowResult {
        let ctx = WorkflowContext::new();
        info!(
            workflow_id = %ctx.workflow_id(),
            name = %self.workflow.name,
            "Iniciando ejecución de workflow"
        );

        if self.index.start_nodes.is_empty() {
            return Err(crate::error::WorkflowError {
                code: "NO_START_NODES".into(),
                message: "El workflow no tiene nodos de inicio".into(),
                source_task: None,
                payload: None,
                response: None,
                source: None,
            });
        }

        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut queue: VecDeque<(NodeId, WorkflowData)> = VecDeque::new();
        let mut last_output: Option<WorkflowData> = None;

        // Encolar todos los nodos de inicio
        for node_id in &self.index.start_nodes {
            queue.push_back((node_id.clone(), start_data.clone()));
        }

        while let Some((node_id, input)) = queue.pop_front() {
            // Evitar re-ejecución de nodos ya visitados
            if visited.contains(&node_id) {
                debug!(node_id = %node_id, "Nodo ya ejecutado, omitiendo");
                continue;
            }

            let node = self
                .index
                .get_node(&node_id)
                .ok_or_else(|| self.node_not_found(&node_id))?;

            debug!(node_id = %node_id, kind = ?node.kind, "Ejecutando nodo");

            let result = self.execute_node(node, &ctx, input).await;

            match result {
                Ok(data) => {
                    visited.insert(node_id.clone());
                    last_output = Some(data.clone());

                    // Encolar nodos destino según las aristas salientes
                    for edge in self.index.outgoing_edges(&node_id) {
                        queue.push_back((edge.to.clone(), data.clone()));
                    }
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        last_output.ok_or_else(|| crate::error::WorkflowError {
            code: "NO_OUTPUT".into(),
            message: "El workflow finalizó sin producir datos de salida".into(),
            source_task: None,
            payload: None,
            response: None,
            source: None,
        })
    }

    /// Ejecuta un nodo individual según su tipo.
    async fn execute_node(
        &self,
        node: &Node,
        ctx: &WorkflowContext,
        input: WorkflowData,
    ) -> WorkflowResult {
        match &node.kind {
            NodeKind::Start(start_node) => {
                debug!(node_id = %node.id, "Iniciando workflow");

                // Validar input contra el schema del nodo Start si existe
                if let Some(schema) = &start_node.schema {
                    if let Err(e) = jsonschema_validate(schema, &input) {
                        return Err(crate::error::WorkflowError {
                            code: "SCHEMA_VALIDATION_FAILED".into(),
                            message: format!(
                                "Los datos de entrada no cumplen el schema del nodo '{}': {}",
                                node.id, e
                            ),
                            source_task: Some(node.id.to_string()),
                            payload: Some(input),
                            response: None,
                            source: None,
                        });
                    }
                }

                // Aplicar defaults si existen: fusionar con los datos de entrada
                let mut data = input;
                if let Some(defaults) = &start_node.defaults {
                    let mut map = match data.0 {
                        serde_json::Value::Object(m) => m,
                        other => {
                            let mut m = serde_json::Map::new();
                            m.insert("_input".to_string(), other);
                            m
                        }
                    };
                    for (key, value) in defaults {
                        map.entry(key.clone()).or_insert_with(|| value.0.clone());
                    }
                    data = WorkflowData(serde_json::Value::Object(map));
                }

                Ok(data)
            }

            NodeKind::End(end_node) => {
                debug!(
                    node_id = %node.id,
                    status = ?end_node.status,
                    "Finalizando workflow"
                );

                // Si el End tiene schema, validar la salida
                if let Some(schema) = &end_node.schema {
                    if let Err(e) = jsonschema_validate(schema, &input) {
                        return Err(crate::error::WorkflowError {
                            code: "OUTPUT_SCHEMA_VALIDATION_FAILED".into(),
                            message: format!(
                                "Los datos de salida no cumplen el schema del nodo '{}': {}",
                                node.id, e
                            ),
                            source_task: Some(node.id.to_string()),
                            payload: Some(input),
                            response: None,
                            source: None,
                        });
                    }
                }

                Ok(input)
            }

            NodeKind::Task(task_node) => {
                let task_id = &task_node.task_id;
                let task =
                    self.registry
                        .get(task_id)
                        .ok_or_else(|| crate::error::WorkflowError {
                            code: "TASK_NOT_FOUND".into(),
                            message: format!("Tarea '{}' no encontrada en el registry", task_id),
                            source_task: Some(node.id.to_string()),
                            payload: None,
                            response: None,
                            source: None,
                        })?;

                debug!(node_id = %node.id, task_id = %task_id, "Invocando tarea");
                task.execute(ctx, input).await.map_err(|mut e| {
                    if e.source_task.is_none() {
                        e.source_task = Some(node.id.to_string());
                    }
                    e
                })
            }
        }
    }

    fn node_not_found(&self, node_id: &NodeId) -> crate::error::WorkflowError {
        crate::error::WorkflowError {
            code: "NODE_NOT_FOUND".into(),
            message: format!("Nodo '{}' no encontrado en el workflow", node_id),
            source_task: Some(node_id.to_string()),
            payload: None,
            response: None,
            source: None,
        }
    }
}

/// Valida un `WorkflowData` contra un `schemars::Schema`.
/// Retorna `Ok(())` si es válido, o `Err(String)` con el mensaje de error.
fn jsonschema_validate(schema: &schemars::Schema, data: &WorkflowData) -> Result<(), String> {
    let schema_json = serde_json::to_value(schema).map_err(|e| e.to_string())?;
    let compiled = jsonschema::validator_for(&schema_json).map_err(|e| e.to_string())?;

    if compiled.is_valid(&data.0) {
        Ok(())
    } else {
        let errors: Vec<String> = compiled
            .iter_errors(&data.0)
            .map(|e| e.to_string())
            .collect();
        Err(errors.join("; "))
    }
}
