use jsonpath_rust::JsonPath;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::WorkflowError;

/// Condición del mini-DSL declarativo de la spec.
/// Es un objeto JSON validable con JSON Schema: una comparación sobre un
/// path JSONPath, o una composición lógica de otras condiciones.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Condition {
    /// Conjunción: todas las condiciones deben cumplirse
    And { and: Vec<Condition> },
    /// Disyunción: al menos una condición debe cumplirse
    Or { or: Vec<Condition> },
    /// Negación
    Not { not: Box<Condition> },
    /// Comparación sobre un path del contexto
    Compare(Comparison),
}

// Deserialize manual: el derive untagged produce errores inservibles
// ("data did not match any variant"); aquí señalamos qué llave falta o sobra.
impl<'de> Deserialize<'de> for Condition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let value = Value::deserialize(deserializer)?;
        let Value::Object(map) = &value else {
            return Err(D::Error::custom("una condición debe ser un objeto JSON"));
        };

        let logical = ["and", "or", "not"]
            .iter()
            .find(|key| map.contains_key(**key));
        if let Some(key) = logical {
            if map.len() != 1 {
                return Err(D::Error::custom(format!(
                    "una condición '{key}' no admite llaves adicionales"
                )));
            }
            let inner = map.get(*key).expect("llave comprobada").clone();
            return match *key {
                "and" => serde_json::from_value(inner)
                    .map(|and| Condition::And { and })
                    .map_err(|e| D::Error::custom(format!("en 'and': {e}"))),
                "or" => serde_json::from_value(inner)
                    .map(|or| Condition::Or { or })
                    .map_err(|e| D::Error::custom(format!("en 'or': {e}"))),
                _ => serde_json::from_value(inner)
                    .map(|not| Condition::Not { not: Box::new(not) })
                    .map_err(|e| D::Error::custom(format!("en 'not': {e}"))),
            };
        }

        if map.contains_key("path") {
            return serde_json::from_value(value.clone())
                .map(Condition::Compare)
                .map_err(|e| D::Error::custom(format!("comparación inválida: {e}")));
        }

        let keys: Vec<&str> = map.keys().map(String::as_str).collect();
        Err(D::Error::custom(format!(
            "condición inválida: se esperaba 'and', 'or', 'not' o una comparación con 'path'; \
             llaves encontradas: [{}]",
            keys.join(", ")
        )))
    }
}

/// Comparación entre el valor resuelto de un path JSONPath y un operando.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Comparison {
    /// Path JSONPath evaluado contra el contexto de ejecución
    pub path: String,
    /// Operador y operando de la comparación
    #[serde(flatten)]
    pub op: CompareOp,
}

/// Operadores de comparación de la spec 1.0.
/// Un path que no resuelve no es error: `exists` da `false` y el resto
/// de operadores dan `false`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    /// El valor del path está dentro de la lista
    In(Vec<Value>),
    /// El valor del path (array o string) contiene al operando
    Contains(Value),
    Exists(bool),
    IsNull(bool),
    StartsWith(String),
    EndsWith(String),
    /// El valor del path (string) cumple la expresión regular
    Matches(String),
}

impl Condition {
    /// Evalúa la condición contra el documento de contexto.
    /// Solo falla por errores de definición (JSONPath o regex inválidos);
    /// un path que no resuelve produce `false`, nunca error.
    pub fn evaluate(&self, context: &Value) -> Result<bool, WorkflowError> {
        match self {
            Condition::And { and } => {
                for cond in and {
                    if !cond.evaluate(context)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Condition::Or { or } => {
                for cond in or {
                    if cond.evaluate(context)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Condition::Not { not } => Ok(!not.evaluate(context)?),
            Condition::Compare(cmp) => cmp.evaluate(context),
        }
    }
}

impl Comparison {
    /// Evalúa la comparación. El path se resuelve al primer valor que
    /// matchea; los paths de condiciones deben ser singulares.
    pub fn evaluate(&self, context: &Value) -> Result<bool, WorkflowError> {
        let resolved = context.query(&self.path).map_err(|e| {
            WorkflowError::new(
                "INVALID_JSONPATH",
                format!("Path '{}' inválido en condición: {}", self.path, e),
            )
        })?;
        let value = resolved.into_iter().next();

        if let CompareOp::Exists(expected) = &self.op {
            return Ok(value.is_some() == *expected);
        }
        let Some(value) = value else {
            return Ok(false);
        };

        Ok(match &self.op {
            CompareOp::Eq(operand) => value == operand,
            CompareOp::Ne(operand) => value != operand,
            CompareOp::Gt(operand) => {
                compare_order(value, operand).is_some_and(|o| o == std::cmp::Ordering::Greater)
            }
            CompareOp::Gte(operand) => {
                compare_order(value, operand).is_some_and(|o| o != std::cmp::Ordering::Less)
            }
            CompareOp::Lt(operand) => {
                compare_order(value, operand).is_some_and(|o| o == std::cmp::Ordering::Less)
            }
            CompareOp::Lte(operand) => {
                compare_order(value, operand).is_some_and(|o| o != std::cmp::Ordering::Greater)
            }
            CompareOp::In(list) => list.iter().any(|item| item == value),
            CompareOp::Contains(operand) => match value {
                Value::Array(items) => items.contains(operand),
                Value::String(s) => operand.as_str().is_some_and(|needle| s.contains(needle)),
                _ => false,
            },
            CompareOp::Exists(_) => unreachable!("manejado arriba"),
            CompareOp::IsNull(expected) => value.is_null() == *expected,
            CompareOp::StartsWith(prefix) => value.as_str().is_some_and(|s| s.starts_with(prefix)),
            CompareOp::EndsWith(suffix) => value.as_str().is_some_and(|s| s.ends_with(suffix)),
            CompareOp::Matches(pattern) => {
                let re = cached_regex(pattern)?;
                value.as_str().is_some_and(|s| re.is_match(s))
            }
        })
    }
}

/// Compila una regex con cache global del proceso: los workflows evalúan
/// los mismos patrones estáticos una y otra vez (retries, ejecuciones).
fn cached_regex(pattern: &str) -> Result<regex::Regex, WorkflowError> {
    use std::collections::HashMap;
    use std::sync::{OnceLock, RwLock};

    static CACHE: OnceLock<RwLock<HashMap<String, regex::Regex>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| RwLock::new(HashMap::new()));

    if let Some(re) = cache.read().expect("regex cache poisoned").get(pattern) {
        return Ok(re.clone());
    }
    let re = regex::Regex::new(pattern).map_err(|e| {
        WorkflowError::new(
            "INVALID_REGEX",
            format!("Regex '{}' inválida en condición: {}", pattern, e),
        )
    })?;
    cache
        .write()
        .expect("regex cache poisoned")
        .insert(pattern.to_string(), re.clone());
    Ok(re)
}

/// Orden entre dos valores JSON: números entre sí (como f64) y strings
/// entre sí (lexicográfico). Tipos mezclados no son comparables.
fn compare_order(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.as_str().cmp(y.as_str())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> Value {
        json!({
            "trigger": { "priority": "high", "retry_count": 5, "tags": ["a", "b"], "empty": null },
            "nodes": { "fetch": { "output": { "status": 200, "url": "https://api.example.com/v1" } } }
        })
    }

    fn eval(cond: Value) -> bool {
        let cond: Condition = serde_json::from_value(cond).expect("condición válida");
        cond.evaluate(&ctx()).expect("evaluación sin errores")
    }

    #[test]
    fn deserializa_ejemplo_de_la_spec() {
        let cond = json!({
            "and": [
                { "path": "$.nodes.fetch.output.status", "eq": 200 },
                { "or": [
                    { "path": "$.trigger.priority", "in": ["high", "urgent"] },
                    { "path": "$.trigger.retry_count", "gt": 3 }
                ]}
            ]
        });
        let parsed: Condition = serde_json::from_value(cond.clone()).unwrap();
        // round-trip estable
        assert_eq!(serde_json::to_value(&parsed).unwrap(), cond);
        assert!(eval(cond));
    }

    #[test]
    fn comparadores_numericos_y_strings() {
        assert!(eval(
            json!({ "path": "$.nodes.fetch.output.status", "gte": 200 })
        ));
        assert!(eval(
            json!({ "path": "$.nodes.fetch.output.status", "lt": 300 })
        ));
        assert!(!eval(json!({ "path": "$.trigger.priority", "gt": 5 }))); // tipos mezclados
        assert!(eval(json!({ "path": "$.trigger.priority", "ne": "low" })));
    }

    #[test]
    fn operadores_de_string() {
        assert!(eval(
            json!({ "path": "$.nodes.fetch.output.url", "starts_with": "https://" })
        ));
        assert!(eval(
            json!({ "path": "$.nodes.fetch.output.url", "ends_with": "/v1" })
        ));
        assert!(eval(
            json!({ "path": "$.nodes.fetch.output.url", "contains": "example" })
        ));
        assert!(eval(
            json!({ "path": "$.nodes.fetch.output.url", "matches": "^https://[a-z.]+/v\\d$" })
        ));
    }

    #[test]
    fn pertenencia_y_nulos() {
        assert!(eval(json!({ "path": "$.trigger.tags", "contains": "a" })));
        assert!(!eval(json!({ "path": "$.trigger.tags", "contains": "z" })));
        assert!(eval(json!({ "path": "$.trigger.empty", "is_null": true })));
    }

    #[test]
    fn paths_ausentes_no_fallan() {
        assert!(!eval(json!({ "path": "$.no.existe", "eq": 1 })));
        assert!(!eval(json!({ "path": "$.no.existe", "ne": 1 }))); // ausente => false, incluso ne
        assert!(eval(json!({ "path": "$.no.existe", "exists": false })));
        assert!(eval(
            json!({ "path": "$.trigger.priority", "exists": true })
        ));
        assert!(!eval(json!({ "path": "$.no.existe", "is_null": true })));
    }

    #[test]
    fn logicos_not() {
        assert!(eval(
            json!({ "not": { "path": "$.trigger.priority", "eq": "low" } })
        ));
    }

    #[test]
    fn errores_de_deserializacion_utiles() {
        let err = serde_json::from_value::<Condition>(json!({ "foo": 1 }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("'and', 'or', 'not'"), "mensaje: {err}");
        assert!(err.contains("foo"), "mensaje: {err}");

        let err = serde_json::from_value::<Condition>(json!({
            "and": [{ "path": "$.x", "eq": 1 }],
            "extra": true
        }))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("no admite llaves adicionales"),
            "mensaje: {err}"
        );
    }

    #[test]
    fn jsonpath_invalido_es_error() {
        let cond: Condition = serde_json::from_value(json!({ "path": "$.[", "eq": 1 })).unwrap();
        let err = cond.evaluate(&ctx()).unwrap_err();
        assert_eq!(err.code, "INVALID_JSONPATH");
    }
}
