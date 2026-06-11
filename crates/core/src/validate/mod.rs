//! La familia **validate**: reglas estáticas sobre la definición.
//!
//! Todo lo que pueda detectarse antes de ejecutar se detecta aquí. La
//! validación es un pipeline de reglas ([`ValidationRule`]): cada regla es
//! una unidad con sus códigos declarados, testeable y documentable por
//! separado. Las reglas integradas viven en [`rules`] y se ejecutan en
//! orden ([`rules::BUILTIN`]); un host puede sumar reglas propias con
//! [`validate_with`].
//!
//! La validación **acumula** todos los errores encontrados (no corta en el
//! primero), para que un documento se corrija en una sola pasada.

mod context;
pub mod rules;

pub use context::ValidationCtx;

use crate::error::WorkflowError;
use crate::spec::workflow::WorkflowDefinition;

/// Versiones de spec que este core sabe ejecutar
pub const SUPPORTED_SPECS: &[&str] = &["1.0"];

/// Una regla de validación: un invariante verificable sobre la definición.
///
/// Las reglas corren en orden y comparten el vector de errores: una regla
/// puede inspeccionar lo ya reportado (p. ej. las reglas de grafo se
/// desactivan si hay referencias rotas). Las reglas custom de un host corren
/// después de las integradas.
pub trait ValidationRule: Send + Sync {
    /// Códigos de error que esta regla puede emitir (ver [`crate::error::codes`]).
    /// Es documentación viva: el conjunto de reglas declara qué se valida.
    fn codes(&self) -> &'static [&'static str];

    /// Verifica el invariante y agrega los errores encontrados a `errors`.
    fn check(
        &self,
        workflow: &WorkflowDefinition,
        ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    );
}

/// Valida la estructura de un workflow con las reglas integradas.
/// Acumula y devuelve todos los errores encontrados, no solo el primero.
pub fn validate(workflow: &WorkflowDefinition) -> Result<(), Vec<WorkflowError>> {
    validate_with(workflow, &[])
}

/// Como [`validate`], con reglas adicionales del host que corren después
/// de las integradas (también sobre cada sub-workflow inline).
pub fn validate_with(
    workflow: &WorkflowDefinition,
    extra: &[&dyn ValidationRule],
) -> Result<(), Vec<WorkflowError>> {
    let mut errors: Vec<WorkflowError> = Vec::new();

    let ctx = ValidationCtx::build(workflow);
    for rule in rules::BUILTIN.iter().chain(extra.iter()) {
        rule.check(workflow, &ctx, &mut errors);
    }

    // Los sub-workflows inline se validan recursivamente con las mismas
    // reglas; sus errores suben con el nombre del hijo como contexto
    for child in &workflow.workflows {
        if let Err(child_errors) = validate_with(child, extra) {
            errors.extend(child_errors.into_iter().map(|mut e| {
                e.message = format!("en el sub-workflow inline '{}': {}", child.name, e.message);
                e
            }));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Verifica que toda tarea referenciada por el workflow esté registrada.
/// Es una validación aparte porque depende del [`crate::task::TaskRegistry`],
/// no solo del documento.
pub fn validate_tasks(
    workflow: &WorkflowDefinition,
    registry: &crate::task::TaskRegistry,
) -> Result<(), Vec<WorkflowError>> {
    use crate::error::codes;
    use crate::spec::node::NodeKind;

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
                    codes::TASK_NOT_FOUND,
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
    fn subworkflow_estructuralmente_valido() {
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
        // El nombre se resuelve al construir el executor, no aquí
        assert_eq!(codes(&workflow), Vec::<String>::new());
    }

    #[test]
    fn subworkflows_inline_con_nombre_duplicado_o_invalidos() {
        let inner_ok = json!({
            "name": "hijo", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [ { "from": "start", "to": "end" } ]
        });
        let inner_bad = json!({
            "name": "hijo", "version": "0.1.0",
            "nodes": [ { "id": "start", "kind": "start" } ],
            "edges": []
        });
        let workflow = wf(json!({
            "name": "padre", "version": "0.1.0",
            "workflows": [inner_ok, inner_bad],
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "child", "kind": "subworkflow", "workflow": "hijo" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "child" },
                { "from": "child", "to": "end" }
            ]
        }));
        let found = codes(&workflow);
        assert!(found.contains(&"SUBWORKFLOW_DUPLICATE_NAME".to_string()));
        // El segundo inline no tiene end: el error sube con contexto
        assert!(found.contains(&"NO_END_NODE".to_string()));
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

    #[test]
    fn toda_regla_integrada_declara_codigos_del_catalogo() {
        use crate::error::codes::ALL;
        for rule in rules::BUILTIN {
            for code in rule.codes() {
                assert!(
                    ALL.contains(code),
                    "la regla declara el código '{code}', que no está en error::codes::ALL"
                );
            }
        }
    }

    #[test]
    fn reglas_custom_del_host_se_ejecutan() {
        struct SinNodosProhibidos;
        impl ValidationRule for SinNodosProhibidos {
            fn codes(&self) -> &'static [&'static str] {
                &["HOST_FORBIDDEN_ID"]
            }
            fn check(
                &self,
                workflow: &WorkflowDefinition,
                _ctx: &ValidationCtx<'_>,
                errors: &mut Vec<WorkflowError>,
            ) {
                for node in &workflow.nodes {
                    if node.id.0 == "prohibido" {
                        errors.push(WorkflowError::new(
                            "HOST_FORBIDDEN_ID",
                            "id de nodo prohibido por el host",
                        ));
                    }
                }
            }
        }

        let workflow = wf(json!({
            "name": "custom", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "prohibido", "kind": "task", "task": "noop" },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "prohibido" },
                { "from": "prohibido", "to": "end" }
            ]
        }));
        let errors = validate_with(&workflow, &[&SinNodosProhibidos]).unwrap_err();
        assert!(errors.iter().any(|e| e.code == "HOST_FORBIDDEN_ID"));
    }
}
