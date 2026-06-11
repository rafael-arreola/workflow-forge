//! Regla de vocabulario de condiciones: todo operador custom usado en un
//! gateway debe estar registrado antes de construir el executor.

use crate::error::{WorkflowError, codes};
use crate::expr::operators;
use crate::spec::condition::{CompareOp, Condition};
use crate::spec::node::NodeKind;
use crate::spec::workflow::WorkflowDefinition;
use crate::validate::{ValidationCtx, ValidationRule};

/// Los operadores fuera de la spec ([`CompareOp::Custom`]) deben existir en
/// el registro global de operadores ([`operators::global`]): detecta typos
/// y extensiones no registradas en build-time, no en plena ejecución.
pub struct KnownConditionOperators;

impl ValidationRule for KnownConditionOperators {
    fn codes(&self) -> &'static [&'static str] {
        &[codes::UNKNOWN_CONDITION_OPERATOR]
    }

    fn check(
        &self,
        workflow: &WorkflowDefinition,
        _ctx: &ValidationCtx<'_>,
        errors: &mut Vec<WorkflowError>,
    ) {
        for node in &workflow.nodes {
            let NodeKind::Gateway(gw) = &node.kind else {
                continue;
            };
            for branch in &gw.branches {
                let Some(when) = &branch.when else { continue };
                walk(when, &mut |op| {
                    if let CompareOp::Custom { key, .. } = op
                        && !operators::global().contains(key)
                    {
                        errors.push(
                            WorkflowError::new(
                                codes::UNKNOWN_CONDITION_OPERATOR,
                                format!(
                                    "La rama '{}' del gateway '{}' usa el operador '{}', que no \
                                     es de la spec 1.0 ni está registrado en el registro de \
                                     operadores",
                                    branch.edge, node.id, key
                                ),
                            )
                            .with_source_task(node.id.to_string()),
                        );
                    }
                });
            }
        }
    }
}

/// Recorre todas las comparaciones de un árbol de condición.
fn walk(cond: &Condition, visit: &mut impl FnMut(&CompareOp)) {
    match cond {
        Condition::And { and } => and.iter().for_each(|c| walk(c, visit)),
        Condition::Or { or } => or.iter().for_each(|c| walk(c, visit)),
        Condition::Not { not } => walk(not, visit),
        Condition::Compare(cmp) => visit(&cmp.op),
    }
}

#[cfg(test)]
mod tests {
    use crate::validate::validate;
    use serde_json::json;

    #[test]
    fn operador_desconocido_se_detecta_en_validacion() {
        let workflow = serde_json::from_value(json!({
            "name": "ops", "version": "0.1.0",
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "check", "kind": "gateway", "gateway": "exclusive", "branches": [
                    { "when": { "and": [
                        { "path": "$.trigger.x", "eq": 1 },
                        { "path": "$.trigger.y", "operador_inexistente_xyz": 2 }
                    ]}, "edge": "ok" },
                    { "else": true, "edge": "fail" }
                ]},
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "check" },
                { "from": "check", "to": "end", "label": "ok" },
                { "from": "check", "to": "end", "label": "fail" }
            ]
        }))
        .unwrap();
        let errors = validate(&workflow).unwrap_err();
        assert!(errors.iter().any(|e| e.code == "UNKNOWN_CONDITION_OPERATOR"
            && e.message.contains("operador_inexistente_xyz")));
    }
}
