//! Resolución de *shapes*: estructuras JSON donde los strings con prefijo `@`
//! son JSONPath relativos a un documento fuente. Es la convención compartida
//! por `data.transform`/`data.map` y por el `bind`/`output` de los perfiles
//! de tarea ([`crate::profile::TaskProfile`]).
//!
//! Reglas:
//! - `"@"` → el source completo.
//! - `"@.a.b"` → JSONPath `$.a.b` sobre el source; un path ausente produce `null`.
//! - `"@@."` → escape: produce el literal `"@."`.
//! - Cualquier otro valor (strings sin prefijo, números, bools, null) es literal.
//! - Objetos y arrays se resuelven recursivamente.

use jsonpath_rust::JsonPath;
use serde_json::Value;

use crate::error::WorkflowError;

/// Resuelve recursivamente un `shape` contra el documento `source`.
pub fn apply_shape(shape: &Value, source: &Value) -> Result<Value, WorkflowError> {
    match shape {
        Value::String(s) => {
            if s == "@" {
                return Ok(source.clone());
            }
            if let Some(rest) = s.strip_prefix("@@.") {
                return Ok(Value::String(format!("@.{rest}")));
            }
            let Some(rest) = s.strip_prefix("@.") else {
                return Ok(shape.clone());
            };
            let path = format!("$.{rest}");
            let matches = source.query(&path).map_err(|e| {
                WorkflowError::new(
                    "INVALID_JSONPATH",
                    format!("Path '@.{rest}' inválido en shape: {e}"),
                )
            })?;
            Ok(matches.into_iter().next().cloned().unwrap_or(Value::Null))
        }
        Value::Array(items) => items
            .iter()
            .map(|item| apply_shape(item, source))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(map) => map
            .iter()
            .map(|(key, value)| apply_shape(value, source).map(|r| (key.clone(), r)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(Value::Object),
        literal => Ok(literal.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn arroba_sola_devuelve_el_source_completo() {
        let source = json!({ "a": 1 });
        assert_eq!(apply_shape(&json!("@"), &source).unwrap(), source);
        assert_eq!(
            apply_shape(&json!({ "body": "@" }), &source).unwrap(),
            json!({ "body": { "a": 1 } })
        );
    }

    #[test]
    fn resuelve_paths_relativos_escapes_y_literales() {
        let source = json!({ "user": { "name": "ada", "tags": ["a", "b"] } });
        let shape = json!({
            "nombre": "@.user.name",
            "primera": "@.user.tags[0]",
            "no_existe": "@.user.email",
            "literal": "@@.escapado",
            "fijo": 7
        });
        assert_eq!(
            apply_shape(&shape, &source).unwrap(),
            json!({
                "nombre": "ada",
                "primera": "a",
                "no_existe": null,
                "literal": "@.escapado",
                "fijo": 7
            })
        );
    }
}
