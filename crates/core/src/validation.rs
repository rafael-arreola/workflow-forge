use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::WorkflowError;
use crate::node::gateway::GatewayKind;
use crate::node::{Node, NodeId, NodeKind};
use crate::workflow::{EdgeTrigger, WorkflowDefinition};

/// Versiones de spec que este core sabe ejecutar
pub const SUPPORTED_SPECS: &[&str] = &["1.0"];

/// Valida la estructura del grafo de un workflow. Acumula y devuelve
/// todos los errores encontrados, no solo el primero.
pub fn validate(workflow: &WorkflowDefinition) -> Result<(), Vec<WorkflowError>> {
    let mut errors: Vec<WorkflowError> = Vec::new();

    if !SUPPORTED_SPECS.contains(&workflow.spec.as_str()) {
        errors.push(WorkflowError::new(
            "UNSUPPORTED_SPEC",
            format!(
                "Spec '{}' no soportada; este core soporta: {}",
                workflow.spec,
                SUPPORTED_SPECS.join(", ")
            ),
        ));
    }

    // --- ids únicos ---
    let mut seen: HashSet<&NodeId> = HashSet::new();
    for node in &workflow.nodes {
        if !seen.insert(&node.id) {
            errors.push(
                WorkflowError::new(
                    "DUPLICATE_NODE_ID",
                    format!("El id de nodo '{}' está duplicado", node.id),
                )
                .with_source_task(node.id.to_string()),
            );
        }
    }
    let nodes: HashMap<&NodeId, &Node> = workflow.nodes.iter().map(|n| (&n.id, n)).collect();

    // --- aristas referencian nodos existentes ---
    let mut outgoing: HashMap<&NodeId, Vec<&crate::workflow::FlowEdge>> = HashMap::new();
    let mut incoming: HashMap<&NodeId, Vec<&crate::workflow::FlowEdge>> = HashMap::new();
    for edge in &workflow.edges {
        for (end, id) in [("origen", &edge.from), ("destino", &edge.to)] {
            if !nodes.contains_key(id) {
                errors.push(WorkflowError::new(
                    "UNKNOWN_NODE_REF",
                    format!(
                        "La arista {}→{} referencia un {} inexistente",
                        edge.from, edge.to, end
                    ),
                ));
            }
        }
        outgoing.entry(&edge.from).or_default().push(edge);
        incoming.entry(&edge.to).or_default().push(edge);
    }

    // --- start / end ---
    let starts: Vec<&Node> = workflow
        .nodes
        .iter()
        .filter(|n| matches!(n.kind, NodeKind::Start(_)))
        .collect();
    match starts.len() {
        0 => errors.push(WorkflowError::new(
            "NO_START_NODE",
            "El workflow no tiene nodo start",
        )),
        1 => {}
        _ => errors.push(WorkflowError::new(
            "MULTIPLE_START_NODES",
            "El workflow tiene más de un nodo start; la spec 1.0 exige exactamente uno",
        )),
    }
    if !workflow
        .nodes
        .iter()
        .any(|n| matches!(n.kind, NodeKind::End(_)))
    {
        errors.push(WorkflowError::new(
            "NO_END_NODE",
            "El workflow no tiene ningún nodo end",
        ));
    }

    // --- reglas por nodo ---
    for node in &workflow.nodes {
        let out = outgoing.get(&node.id).map(Vec::as_slice).unwrap_or(&[]);
        let inc = incoming.get(&node.id).map(Vec::as_slice).unwrap_or(&[]);

        match &node.kind {
            NodeKind::Start(_) => {
                if !inc.is_empty() {
                    errors.push(
                        WorkflowError::new(
                            "START_HAS_INCOMING",
                            format!(
                                "El nodo start '{}' no puede tener aristas entrantes",
                                node.id
                            ),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
            }
            NodeKind::End(_) => {
                if !out.is_empty() {
                    errors.push(
                        WorkflowError::new(
                            "END_HAS_OUTGOING",
                            format!("El nodo end '{}' no puede tener aristas salientes", node.id),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
            }
            NodeKind::Task(_) => {}
            NodeKind::Foreach(foreach) => {
                if foreach.concurrency == 0 {
                    errors.push(
                        WorkflowError::new(
                            "FOREACH_INVALID_CONCURRENCY",
                            format!(
                                "El foreach '{}' declara concurrency 0; debe ser >= 1",
                                node.id
                            ),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
            }
            NodeKind::Gateway(gw) => match gw.gateway {
                GatewayKind::Exclusive => {
                    if gw.branches.is_empty() {
                        errors.push(
                            WorkflowError::new(
                                "GATEWAY_NO_BRANCHES",
                                format!("El gateway exclusive '{}' no declara branches", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if gw.branches.iter().filter(|b| b.is_else).count() > 1 {
                        errors.push(
                            WorkflowError::new(
                                "GATEWAY_MULTIPLE_ELSE",
                                format!("El gateway '{}' tiene más de una rama else", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    for branch in &gw.branches {
                        if !branch.is_else && branch.when.is_none() {
                            errors.push(
                                WorkflowError::new(
                                    "GATEWAY_BRANCH_WITHOUT_WHEN",
                                    format!(
                                        "Una rama del gateway '{}' no tiene `when` ni es `else`",
                                        node.id
                                    ),
                                )
                                .with_source_task(node.id.to_string()),
                            );
                        }
                        if !out.iter().any(|e| e.label.as_deref() == Some(&branch.edge)) {
                            errors.push(
                                WorkflowError::new(
                                    "GATEWAY_BRANCH_WITHOUT_EDGE",
                                    format!(
                                        "La rama '{}' del gateway '{}' no tiene arista saliente con ese label",
                                        branch.edge, node.id
                                    ),
                                )
                                .with_source_task(node.id.to_string()),
                            );
                        }
                    }
                    for edge in out {
                        let label = edge.label.as_deref();
                        if !gw.branches.iter().any(|b| Some(b.edge.as_str()) == label) {
                            errors.push(
                                WorkflowError::new(
                                    "GATEWAY_EDGE_WITHOUT_BRANCH",
                                    format!(
                                        "La arista {}→{} (label {:?}) no corresponde a ninguna rama del gateway",
                                        edge.from, edge.to, label
                                    ),
                                )
                                .with_source_task(node.id.to_string()),
                            );
                        }
                    }
                }
                GatewayKind::Parallel => {
                    if !gw.branches.is_empty() {
                        errors.push(
                            WorkflowError::new(
                                "GATEWAY_BRANCHES_IGNORED",
                                format!("El gateway parallel '{}' no acepta branches", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if out.len() < 2 {
                        errors.push(
                            WorkflowError::new(
                                "PARALLEL_TOO_FEW_OUTPUTS",
                                format!(
                                    "El gateway parallel '{}' necesita al menos 2 aristas salientes",
                                    node.id
                                ),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                }
                GatewayKind::Join => {
                    if !gw.branches.is_empty() {
                        errors.push(
                            WorkflowError::new(
                                "GATEWAY_BRANCHES_IGNORED",
                                format!("El gateway join '{}' no acepta branches", node.id),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                    if inc.iter().filter(|e| e.on.is_none()).count() < 2 {
                        errors.push(
                            WorkflowError::new(
                                "JOIN_TOO_FEW_INPUTS",
                                format!(
                                    "El gateway join '{}' necesita al menos 2 aristas entrantes",
                                    node.id
                                ),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                }
            },
            NodeKind::Subworkflow(_) => {
                errors.push(
                    WorkflowError::new(
                        "SUBWORKFLOW_NOT_SUPPORTED",
                        format!(
                            "El nodo '{}' usa el kind reservado 'subworkflow', aún no soportado",
                            node.id
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }

        // aristas on:error solo salen de nodos que ejecutan tareas
        for edge in out {
            if edge.on == Some(EdgeTrigger::Error)
                && !matches!(node.kind, NodeKind::Task(_) | NodeKind::Foreach(_))
            {
                errors.push(
                    WorkflowError::new(
                        "ERROR_EDGE_INVALID_SOURCE",
                        format!(
                            "La arista de error {}→{} debe originarse en un nodo task o foreach",
                            edge.from, edge.to
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }

        // aristas on:error no pueden entrar a un join: el conteo de llegadas
        // del join solo considera el flujo normal
        for edge in inc {
            if edge.on == Some(EdgeTrigger::Error)
                && matches!(
                    &node.kind,
                    NodeKind::Gateway(gw) if gw.gateway == GatewayKind::Join
                )
            {
                errors.push(
                    WorkflowError::new(
                        "ERROR_EDGE_TO_JOIN",
                        format!(
                            "La arista de error {}→{} no puede apuntar a un gateway join",
                            edge.from, edge.to
                        ),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }

    // --- alcanzabilidad y ciclos (solo si las referencias son sanas) ---
    if errors.iter().all(|e| e.code != "UNKNOWN_NODE_REF") {
        if let Some(start) = starts.first() {
            let mut reachable: HashSet<&NodeId> = HashSet::new();
            let mut queue: VecDeque<&NodeId> = VecDeque::from([&start.id]);
            while let Some(id) = queue.pop_front() {
                if reachable.insert(id) {
                    for edge in outgoing.get(id).map(Vec::as_slice).unwrap_or(&[]) {
                        queue.push_back(&edge.to);
                    }
                }
            }
            for node in &workflow.nodes {
                if !reachable.contains(&node.id) {
                    errors.push(
                        WorkflowError::new(
                            "UNREACHABLE_NODE",
                            format!("El nodo '{}' no es alcanzable desde el start", node.id),
                        )
                        .with_source_task(node.id.to_string()),
                    );
                }
            }
        }

        // Kahn: si no se pueden ordenar todos los nodos, hay ciclo
        let mut in_degree: HashMap<&NodeId, usize> = nodes.keys().map(|id| (*id, 0)).collect();
        for edge in &workflow.edges {
            *in_degree.get_mut(&edge.to).expect("ref validada") += 1;
        }
        let mut queue: VecDeque<&NodeId> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(id, _)| *id)
            .collect();
        let mut sorted = 0usize;
        while let Some(id) = queue.pop_front() {
            sorted += 1;
            for edge in outgoing.get(id).map(Vec::as_slice).unwrap_or(&[]) {
                let deg = in_degree.get_mut(&edge.to).expect("ref validada");
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(&edge.to);
                }
            }
        }
        if sorted != nodes.len() {
            errors.push(WorkflowError::new(
                "CYCLE_DETECTED",
                "El grafo contiene ciclos; la spec 1.0 exige un grafo acíclico",
            ));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Verifica que toda tarea referenciada por el workflow esté registrada.
pub fn validate_tasks(
    workflow: &WorkflowDefinition,
    registry: &crate::registry::TaskRegistry,
) -> Result<(), Vec<WorkflowError>> {
    let errors: Vec<WorkflowError> = workflow
        .nodes
        .iter()
        .filter_map(|node| {
            let task = match &node.kind {
                NodeKind::Task(task_node) => &task_node.task,
                NodeKind::Foreach(foreach) => &foreach.task,
                _ => return None,
            };
            (!registry.contains(task)).then(|| {
                WorkflowError::new(
                    "TASK_NOT_FOUND",
                    format!(
                        "El nodo '{}' referencia la tarea '{}' que no está registrada",
                        node.id, task
                    ),
                )
                .with_source_task(node.id.to_string())
            })
        })
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn wf(value: serde_json::Value) -> WorkflowDefinition {
        serde_json::from_value(value).expect("workflow deserializable")
    }

    fn codes(workflow: &WorkflowDefinition) -> Vec<String> {
        match validate(workflow) {
            Ok(()) => vec![],
            Err(errors) => errors.into_iter().map(|e| e.code).collect(),
        }
    }

    #[test]
    fn workflow_minimo_valido() {
        let workflow = wf(json!({
            "name": "ok", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [ { "from": "start", "to": "end" } ]
        }));
        assert_eq!(codes(&workflow), Vec::<String>::new());
    }

    #[test]
    fn detecta_problemas_basicos() {
        let workflow = wf(json!({
            "spec": "9.9",
            "name": "bad", "version": "0.1.0",
            "nodes": [
                { "id": "a", "kind": "start" },
                { "id": "a", "kind": "end" }
            ],
            "edges": [ { "from": "a", "to": "fantasma" } ]
        }));
        let found = codes(&workflow);
        for expected in ["UNSUPPORTED_SPEC", "DUPLICATE_NODE_ID", "UNKNOWN_NODE_REF"] {
            assert!(
                found.contains(&expected.to_string()),
                "falta {expected} en {found:?}"
            );
        }
    }

    #[test]
    fn gateway_exclusive_coherente_con_aristas() {
        let workflow = wf(json!({
            "name": "gw", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "check", "kind": "gateway", "gateway": "exclusive", "branches": [
                    { "when": { "path": "$.trigger.x", "eq": 1 }, "edge": "ok" },
                    { "else": true, "edge": "fail" }
                ]},
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "check" },
                { "from": "check", "to": "end", "label": "ok" },
                { "from": "check", "to": "end", "label": "huerfana" }
            ]
        }));
        let found = codes(&workflow);
        assert!(found.contains(&"GATEWAY_BRANCH_WITHOUT_EDGE".to_string())); // falta "fail"
        assert!(found.contains(&"GATEWAY_EDGE_WITHOUT_BRANCH".to_string())); // sobra "huerfana"
    }

    #[test]
    fn detecta_ciclos_y_huerfanos() {
        let workflow = wf(json!({
            "name": "cyc", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "a", "kind": "task", "task": "noop" },
                { "id": "b", "kind": "task", "task": "noop" },
                { "id": "isla", "kind": "task", "task": "noop" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "a" },
                { "from": "a", "to": "b" },
                { "from": "b", "to": "a" },
                { "from": "a", "to": "end" }
            ]
        }));
        let found = codes(&workflow);
        assert!(found.contains(&"CYCLE_DETECTED".to_string()));
        assert!(found.contains(&"UNREACHABLE_NODE".to_string()));
    }

    #[test]
    fn arista_de_error_no_puede_entrar_a_join() {
        let workflow = wf(json!({
            "name": "ej", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "fan", "kind": "gateway", "gateway": "parallel" },
                { "id": "a", "kind": "task", "task": "noop" },
                { "id": "b", "kind": "task", "task": "noop" },
                { "id": "meet", "kind": "gateway", "gateway": "join" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "fan" },
                { "from": "fan", "to": "a" },
                { "from": "fan", "to": "b" },
                { "from": "a", "to": "meet" },
                { "from": "b", "to": "meet" },
                { "from": "a", "on": "error", "to": "meet" },
                { "from": "meet", "to": "end" }
            ]
        }));
        assert!(codes(&workflow).contains(&"ERROR_EDGE_TO_JOIN".to_string()));
    }

    #[test]
    fn subworkflow_es_rechazado() {
        let workflow = wf(json!({
            "name": "sub", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "child", "kind": "subworkflow", "workflow": "otro" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "child" },
                { "from": "child", "to": "end" }
            ]
        }));
        assert!(codes(&workflow).contains(&"SUBWORKFLOW_NOT_SUPPORTED".to_string()));
    }

    #[test]
    fn join_requiere_dos_entradas_y_parallel_dos_salidas() {
        let workflow = wf(json!({
            "name": "pj", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "fan", "kind": "gateway", "gateway": "parallel" },
                { "id": "meet", "kind": "gateway", "gateway": "join" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "fan" },
                { "from": "fan", "to": "meet" },
                { "from": "meet", "to": "end" }
            ]
        }));
        let found = codes(&workflow);
        assert!(found.contains(&"PARALLEL_TOO_FEW_OUTPUTS".to_string()));
        assert!(found.contains(&"JOIN_TOO_FEW_INPUTS".to_string()));
    }
}
