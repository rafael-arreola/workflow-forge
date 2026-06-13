//! Resolution of *mappings* `$.`: the convention for node `input`,
//! foreach `items`, and end node `output`, always against the
//! execution context document.

use serde_json::Value;

use crate::error::{WorkflowError, codes};
use crate::expr::path::query_first;

/// Resolves an input mapping against the context document.
///
/// Spec 1.0 rules:
/// - Strings starting with `$.` are resolved as JSONPath against the context.
/// - `$$.` escapes: produces the literal `$.` without resolving.
/// - Objects and arrays are traversed recursively; the rest are literals.
/// - A path that does not resolve is an error (`MAPPING_PATH_NOT_FOUND`): in an input
///   it is almost always a definition bug. Optional values are prepared
///   with a prior `data.*` node.
/// - Paths are singular: the first match is taken. Wildcards are not
///   supported in v1 mappings.
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
                format!("Invalid path '{}' in mapping: {}", s, e),
            )
        })?
        .cloned()
        .ok_or_else(|| {
            WorkflowError::new(
                codes::MAPPING_PATH_NOT_FOUND,
                format!("Path '{}' does not resolve any value from the context", s),
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
