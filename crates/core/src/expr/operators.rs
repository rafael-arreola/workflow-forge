//! Evaluación de condiciones y el registro de operadores: el punto de
//! extensión del vocabulario de comparación.
//!
//! Los 13 operadores de la spec 1.0 (`eq`, `ne`, `gt`, …, `matches`) son
//! vocabulario cerrado y se evalúan aquí con su semántica tipada. Cualquier
//! otra llave deserializa como [`CompareOp::Custom`] y se resuelve contra el
//! [`OperatorRegistry`]: un host puede registrar operadores propios
//! implementando [`ConditionOperator`].
//!
//! ```
//! use serde_json::{json, Value};
//! use workflow_forge_core::error::WorkflowError;
//! use workflow_forge_core::expr::operators::{self, ConditionOperator};
//! use workflow_forge_core::spec::Condition;
//!
//! struct Between;
//! impl ConditionOperator for Between {
//!     fn key(&self) -> &'static str { "between" }
//!     fn evaluate(&self, value: Option<&Value>, operand: &Value) -> Result<bool, WorkflowError> {
//!         let (Some(v), Some(range)) = (value.and_then(Value::as_f64), operand.as_array()) else {
//!             return Ok(false);
//!         };
//!         let (Some(lo), Some(hi)) = (range.first().and_then(Value::as_f64),
//!                                     range.get(1).and_then(Value::as_f64)) else {
//!             return Ok(false);
//!         };
//!         Ok(lo <= v && v <= hi)
//!     }
//! }
//!
//! operators::global().register(Between);
//! let cond: Condition =
//!     serde_json::from_value(json!({ "path": "$.x", "between": [1, 10] })).unwrap();
//! assert!(cond.evaluate(&json!({ "x": 5 })).unwrap());
//! ```
//!
//! Reglas para operadores custom:
//! - La llave no puede colisionar con un operador de la spec (esas llaves
//!   nunca llegan como `Custom`).
//! - Reciben `None` cuando el path no resuelve: deciden su propia semántica
//!   de ausencia (los built-in devuelven `false`, salvo `exists`).
//! - Un operador custom no registrado es `UNKNOWN_CONDITION_OPERATOR` en
//!   runtime, y la regla de validación correspondiente lo detecta al
//!   construir el executor.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use serde_json::Value;

use crate::error::{WorkflowError, codes};
use crate::expr::path::{cached_regex, query_first};
use crate::spec::condition::{CompareOp, Comparison, Condition};

/// Un operador de comparación: la unidad de extensión del vocabulario de
/// condiciones.
pub trait ConditionOperator: Send + Sync + 'static {
    /// Llave del operador tal como aparece en el JSON (ej. `"between"`)
    fn key(&self) -> &'static str;

    /// Evalúa el operador. `value` es el valor resuelto del path (`None` si
    /// el path no resolvió) y `operand` el operando declarado en el JSON.
    /// Solo debe fallar por errores de definición, no de datos.
    fn evaluate(&self, value: Option<&Value>, operand: &Value) -> Result<bool, WorkflowError>;
}

/// Registro de operadores custom, indexados por llave.
/// Thread-safe; los operadores se comparten vía `Arc`.
#[derive(Default)]
pub struct OperatorRegistry {
    ops: RwLock<HashMap<String, Arc<dyn ConditionOperator>>>,
}

impl OperatorRegistry {
    /// Crea un registro vacío
    pub fn new() -> Self {
        Self::default()
    }

    /// Registra un operador. Si ya existe uno con la misma llave, lo
    /// sobrescribe.
    pub fn register<O: ConditionOperator>(&self, op: O) {
        let mut ops = self.ops.write().expect("OperatorRegistry lock poisoned");
        ops.insert(op.key().to_string(), Arc::new(op));
    }

    /// Obtiene un operador por su llave
    pub fn get(&self, key: &str) -> Option<Arc<dyn ConditionOperator>> {
        let ops = self.ops.read().expect("OperatorRegistry lock poisoned");
        ops.get(key).cloned()
    }

    /// `true` si hay un operador registrado bajo esa llave
    pub fn contains(&self, key: &str) -> bool {
        let ops = self.ops.read().expect("OperatorRegistry lock poisoned");
        ops.contains_key(key)
    }

    /// Llaves de todos los operadores registrados
    pub fn list(&self) -> Vec<String> {
        let ops = self.ops.read().expect("OperatorRegistry lock poisoned");
        ops.keys().cloned().collect()
    }
}

/// Registro global del proceso. Las condiciones se evalúan contra este
/// registro; los hosts registran aquí sus operadores al arrancar (antes de
/// construir executors, para que la validación los conozca).
pub fn global() -> &'static OperatorRegistry {
    static GLOBAL: OnceLock<OperatorRegistry> = OnceLock::new();
    GLOBAL.get_or_init(OperatorRegistry::new)
}

impl Condition {
    /// Evalúa la condición contra el documento de contexto usando el
    /// registro global para operadores custom.
    /// Solo falla por errores de definición (JSONPath o regex inválidos,
    /// operador custom no registrado); un path que no resuelve produce
    /// `false`, nunca error.
    pub fn evaluate(&self, context: &Value) -> Result<bool, WorkflowError> {
        self.evaluate_with(context, global())
    }

    /// Como [`Condition::evaluate`], con un registro de operadores explícito
    /// (útil para tests o vocabularios aislados por host).
    pub fn evaluate_with(
        &self,
        context: &Value,
        registry: &OperatorRegistry,
    ) -> Result<bool, WorkflowError> {
        match self {
            Condition::And { and } => {
                for cond in and {
                    if !cond.evaluate_with(context, registry)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Condition::Or { or } => {
                for cond in or {
                    if cond.evaluate_with(context, registry)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Condition::Not { not } => Ok(!not.evaluate_with(context, registry)?),
            Condition::Compare(cmp) => cmp.evaluate_with(context, registry),
        }
    }
}

impl Comparison {
    /// Evalúa la comparación usando el registro global para operadores
    /// custom. El path se resuelve al primer valor que matchea; los paths
    /// de condiciones deben ser singulares.
    pub fn evaluate(&self, context: &Value) -> Result<bool, WorkflowError> {
        self.evaluate_with(context, global())
    }

    /// Como [`Comparison::evaluate`], con un registro explícito.
    pub fn evaluate_with(
        &self,
        context: &Value,
        registry: &OperatorRegistry,
    ) -> Result<bool, WorkflowError> {
        let value = query_first(context, &self.path).map_err(|e| {
            WorkflowError::new(
                codes::INVALID_JSONPATH,
                format!("Path '{}' inválido en condición: {}", self.path, e),
            )
        })?;

        // Operadores que deciden sobre la AUSENCIA del valor
        match &self.op {
            CompareOp::Exists(expected) => return Ok(value.is_some() == *expected),
            CompareOp::Custom { key, operand } => {
                let op = registry.get(key).ok_or_else(|| {
                    WorkflowError::new(
                        codes::UNKNOWN_CONDITION_OPERATOR,
                        format!(
                            "El operador '{key}' no es de la spec 1.0 ni está registrado \
                             en el registro de operadores"
                        ),
                    )
                })?;
                return op.evaluate(value, operand);
            }
            _ => {}
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
            CompareOp::IsNull(expected) => value.is_null() == *expected,
            CompareOp::StartsWith(prefix) => value.as_str().is_some_and(|s| s.starts_with(prefix)),
            CompareOp::EndsWith(suffix) => value.as_str().is_some_and(|s| s.ends_with(suffix)),
            CompareOp::Matches(pattern) => {
                let re = cached_regex(pattern)?;
                value.as_str().is_some_and(|s| re.is_match(s))
            }
            CompareOp::Exists(_) | CompareOp::Custom { .. } => unreachable!("manejados arriba"),
        })
    }
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
    fn evalua_ejemplo_de_la_spec() {
        assert!(eval(json!({
            "and": [
                { "path": "$.nodes.fetch.output.status", "eq": 200 },
                { "or": [
                    { "path": "$.trigger.priority", "in": ["high", "urgent"] },
                    { "path": "$.trigger.retry_count", "gt": 3 }
                ]}
            ]
        })));
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
    fn jsonpath_invalido_es_error() {
        let cond: Condition = serde_json::from_value(json!({ "path": "$.[", "eq": 1 })).unwrap();
        let err = cond.evaluate(&ctx()).unwrap_err();
        assert_eq!(err.code, "INVALID_JSONPATH");
    }

    #[test]
    fn operador_custom_no_registrado_es_error() {
        let cond: Condition =
            serde_json::from_value(json!({ "path": "$.trigger.retry_count", "jamas_existira": 1 }))
                .unwrap();
        let err = cond.evaluate(&ctx()).unwrap_err();
        assert_eq!(err.code, "UNKNOWN_CONDITION_OPERATOR");
    }

    #[test]
    fn operador_custom_registrado_se_evalua() {
        struct LenEq;
        impl ConditionOperator for LenEq {
            fn key(&self) -> &'static str {
                "len_eq"
            }
            fn evaluate(
                &self,
                value: Option<&Value>,
                operand: &Value,
            ) -> Result<bool, WorkflowError> {
                let len = match value {
                    Some(Value::Array(items)) => items.len() as u64,
                    Some(Value::String(s)) => s.len() as u64,
                    _ => return Ok(false),
                };
                Ok(operand.as_u64() == Some(len))
            }
        }

        let registry = OperatorRegistry::new();
        registry.register(LenEq);

        let cond: Condition =
            serde_json::from_value(json!({ "path": "$.trigger.tags", "len_eq": 2 })).unwrap();
        assert!(cond.evaluate_with(&ctx(), &registry).unwrap());

        let cond: Condition =
            serde_json::from_value(json!({ "path": "$.trigger.tags", "len_eq": 3 })).unwrap();
        assert!(!cond.evaluate_with(&ctx(), &registry).unwrap());
    }

    #[test]
    fn operador_custom_recibe_ausencia() {
        struct Missing;
        impl ConditionOperator for Missing {
            fn key(&self) -> &'static str {
                "missing"
            }
            fn evaluate(
                &self,
                value: Option<&Value>,
                operand: &Value,
            ) -> Result<bool, WorkflowError> {
                Ok(value.is_none() == operand.as_bool().unwrap_or(false))
            }
        }
        let registry = OperatorRegistry::new();
        registry.register(Missing);

        let cond: Condition =
            serde_json::from_value(json!({ "path": "$.no.existe", "missing": true })).unwrap();
        assert!(cond.evaluate_with(&ctx(), &registry).unwrap());
    }
}
