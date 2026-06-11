//! Reglas globales del grafo: alcanzabilidad desde el start y aciclicidad.
//!
//! Ambas se auto-desactivan si una regla previa reportó `UNKNOWN_NODE_REF`:
//! con referencias rotas, los recorridos producirían ruido en vez de
//! diagnóstico.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::{WorkflowError, codes};
use crate::spec::node::NodeId;
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{ValidationCtx, ValidationRule};

fn refs_are_sane(errors: &[WorkflowError]) -> bool {
    errors.iter().all(|e| e.code != codes::UNKNOWN_NODE_REF)
}

/// Todo nodo debe ser alcanzable desde el start (BFS por aristas salientes,
/// incluidas las de error: una ruta `on: error` cuenta como alcanzable).
pub struct Reachability;

impl ValidationRule for Reachability {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::UNREACHABLE_NODE]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        if !refs_are_sane(errors) {
            return;
        }
        let Some(start) = ctx.starts().first() else {
            return;
        };

        let mut reachable: HashSet<&NodeId> = HashSet::new();
        let mut queue: VecDeque<&NodeId> = VecDeque::from([&start.id]);
        while let Some(id) = queue.pop_front() {
            if reachable.insert(id) {
                for edge in ctx.outgoing(id) {
                    queue.push_back(&edge.to);
                }
            }
        }
        for node in &workflow.nodes {
            if !reachable.contains(&node.id) {
                errors.push(
                    WorkflowError::new(
                        codes::UNREACHABLE_NODE,
                        format!("El nodo '{}' no es alcanzable desde el start", node.id),
                    )
                    .with_source_task(node.id.to_string()),
                );
            }
        }
    }
}

/// El grafo debe ser acíclico (orden topológico de Kahn: si no se pueden
/// ordenar todos los nodos, hay ciclo).
pub struct Acyclicity;

impl ValidationRule for Acyclicity {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::CYCLE_DETECTED]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        if !refs_are_sane(errors) {
            return;
        }

        let mut in_degree: HashMap<&NodeId, usize> = ctx.node_ids().map(|id| (id, 0)).collect();
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
            for edge in ctx.outgoing(id) {
                let deg = in_degree.get_mut(&edge.to).expect("ref validada");
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(&edge.to);
                }
            }
        }
        if sorted != ctx.node_count() {
            errors.push(WorkflowError::new(
                codes::CYCLE_DETECTED,
                "El grafo contiene ciclos; la spec 1.0 exige un grafo acíclico",
            ));
        }
    }
}
