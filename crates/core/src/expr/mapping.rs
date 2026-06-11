//! Resolución de *mappings* `$.`: la convención de los `input` de nodos,
//! los `items` de foreach y el `output` de los nodos end, siempre contra el
//! documento de contexto de la ejecución.

use serde_json::Value;

use crate::error::{WorkflowError, codes};
use crate::expr::path::query_first;

/// Resuelve un input mapping contra el documento de contexto.
///
/// Reglas de la spec 1.0:
/// - Strings que empiezan con `$.` se resuelven como JSONPath contra el contexto.
/// - `$$.` escapa: produce el literal `$.` sin resolver.
/// - Objetos y arrays se recorren recursivamente; el resto son literales.
/// - Un path que no resuelve es error (`MAPPING_PATH_NOT_FOUND`): en un input
///   es casi siempre un bug de definición. Los valores opcionales se preparan
///   con un nodo `data.*` previo.
/// - Los paths son singulares: se toma el primer match. Wildcards no están
///   soportados en mappings v1.
pub fn resolve(mapping: &Value, context: &Value) -> Result<Value, WorkflowError> {
    match mapping {
        Value::String(s) => resolve_string(s, context),
        Value::Array(items) => items
            .iter()
            .map(|item| resolve(item, context))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(map) => map
            .iter()
            .map(|(key, value)| resolve(value, context).map(|r| (key.clone(), r)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(Value::Object),
        literal => Ok(literal.clone()),
    }
}

fn resolve_string(s: &str, context: &Value) -> Result<Value, WorkflowError> {
    if let Some(rest) = s.strip_prefix("$$.") {
        return Ok(Value::String(format!("$.{rest}")));
    }
    if !s.starts_with("$.") {
        return Ok(Value::String(s.to_string()));
    }

    query_first(context, s)
        .map_err(|e| {
            WorkflowError::new(
                codes::INVALID_JSONPATH,
                format!("Path '{}' inválido en mapping: {}", s, e),
            )
        })?
        .cloned()
        .ok_or_else(|| {
            WorkflowError::new(
                codes::MAPPING_PATH_NOT_FOUND,
                format!("El path '{}' no resuelve ningún valor del contexto", s),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> Value {
        json!({
            "trigger": { "api_base": "https://api.example.com", "user_id": 42 },
            "nodes": { "fetch": { "output": { "body": { "name": "ada" }, "status": 200 } } }
        })
    }

    #[test]
    fn resuelve_paths_literales_y_anidados() {
        let mapping = json!({
            "url": "$.trigger.api_base",
            "method": "GET",
            "timeout": 5000,
            "meta": {
                "user": "$.nodes.fetch.output.body.name",
                "flags": [true, "$.nodes.fetch.output.status"]
            }
        });
        let resolved = resolve(&mapping, &ctx()).unwrap();
        assert_eq!(
            resolved,
            json!({
                "url": "https://api.example.com",
                "method": "GET",
                "timeout": 5000,
                "meta": { "user": "ada", "flags": [true, 200] }
            })
        );
    }

    #[test]
    fn escape_produce_literal() {
        let resolved = resolve(&json!("$$.no.es.un.path"), &ctx()).unwrap();
        assert_eq!(resolved, json!("$.no.es.un.path"));
    }

    #[test]
    fn path_ausente_es_error() {
        let err = resolve(&json!("$.no.existe"), &ctx()).unwrap_err();
        assert_eq!(err.code, "MAPPING_PATH_NOT_FOUND");
    }

    #[test]
    fn path_invalido_es_error() {
        let err = resolve(&json!("$.["), &ctx()).unwrap_err();
        assert_eq!(err.code, "INVALID_JSONPATH");
    }

    #[test]
    fn valores_compuestos_se_copian() {
        let resolved = resolve(&json!("$.nodes.fetch.output.body"), &ctx()).unwrap();
        assert_eq!(resolved, json!({ "name": "ada" }));
    }
}
