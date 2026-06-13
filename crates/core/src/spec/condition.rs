//! Syntax of the spec's condition mini-DSL (types and serde only).
//!
//! A condition is a JSON object validatable with JSON Schema: a
//! comparison over a JSONPath path, or a logical composition (`and`,
//! `or`, `not`). **Evaluation** lives in [`crate::expr::operators`], along
//! with the custom operator registry.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Condition from the spec's declarative mini-DSL.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Condition {
    /// Conjunction: all conditions must hold
    And {
        /// Conditions that must all hold
        and: Vec<Condition>,
    },
    /// Disjunction: at least one condition must hold
    Or {
        /// Conditions of which at least one must hold
        or: Vec<Condition>,
    },
    /// Negation
    Not {
        /// Condition to negate
        not: Box<Condition>,
    },
    /// Comparison over a context path
    Compare(Comparison),
}

// Manual Deserialize: the untagged derive produces useless errors
// ("data did not match any variant"); here we point out which key is missing or extra.
impl<'de> Deserialize<'de> for Condition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let value = Value::deserialize(deserializer)?;
        let Value::Object(map) = &value else {
            return Err(D::Error::custom("a condition must be a JSON object"));
        };

        let logical = ["and", "or", "not"]
            .iter()
            .find(|key| map.contains_key(**key));
        if let Some(key) = logical {
            if map.len() != 1 {
                return Err(D::Error::custom(format!(
                    "a '{key}' condition does not allow extra keys"
                )));
            }
            let inner = map.get(*key).expect("key already checked").clone();
            return match *key {
                "and" => serde_json::from_value(inner)
                    .map(|and| Condition::And { and })
                    .map_err(|e| D::Error::custom(format!("in 'and': {e}"))),
                "or" => serde_json::from_value(inner)
                    .map(|or| Condition::Or { or })
                    .map_err(|e| D::Error::custom(format!("in 'or': {e}"))),
                _ => serde_json::from_value(inner)
                    .map(|not| Condition::Not { not: Box::new(not) })
                    .map_err(|e| D::Error::custom(format!("in 'not': {e}"))),
            };
        }

        if map.contains_key("path") {
            return serde_json::from_value(value.clone())
                .map(Condition::Compare)
                .map_err(|e| D::Error::custom(format!("invalid comparison: {e}")));
        }

        let keys: Vec<&str> = map.keys().map(String::as_str).collect();
        Err(D::Error::custom(format!(
            "invalid condition: expected 'and', 'or', 'not' or a comparison with 'path'; \
             found keys: [{}]",
            keys.join(", ")
        )))
    }
}

/// Comparison between the resolved value of a JSONPath path and an operand.
/// In JSON: `{ "path": "$.x", "<operator>": <operand> }`.
#[derive(Debug, Clone)]
pub struct Comparison {
    /// JSONPath path evaluated against the execution context
    pub path: String,
    /// Operator and operand of the comparison
    pub op: CompareOp,
}

impl Serialize for Comparison {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("path", &self.path)?;
        map.serialize_entry(self.op.key(), &self.op.operand())?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for Comparison {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let value = Value::deserialize(deserializer)?;
        let Value::Object(mut map) = value else {
            return Err(D::Error::custom("a comparison must be a JSON object"));
        };
        let path = match map.remove("path") {
            Some(Value::String(path)) => path,
            Some(_) => return Err(D::Error::custom("'path' must be a JSONPath string")),
            None => return Err(D::Error::custom("a comparison requires 'path'")),
        };
        if map.len() != 1 {
            let keys: Vec<&str> = map.keys().map(String::as_str).collect();
            return Err(D::Error::custom(format!(
                "a comparison requires exactly one operator alongside 'path'; \
                 found keys: [{}]",
                keys.join(", ")
            )));
        }
        let (key, operand) = map.into_iter().next().expect("len already checked");
        let op = CompareOp::from_key_operand(key, operand).map_err(D::Error::custom)?;
        Ok(Comparison { path, op })
    }
}

/// Comparison operators. The 13 from spec 1.0 are a closed vocabulary;
/// any other key deserializes as [`CompareOp::Custom`] and resolves
/// against the operator registry
/// ([`crate::expr::operators::OperatorRegistry`]) at evaluation time.
///
/// A path that does not resolve is not an error: `exists` yields `false` and the rest of
/// the built-in operators yield `false`.
#[derive(Debug, Clone)]
pub enum CompareOp {
    /// Strict equality of JSON values
    Eq(Value),
    /// Inequality (a missing path yields `false`, not `true`)
    Ne(Value),
    /// Greater than (numbers among themselves, strings among themselves)
    Gt(Value),
    /// Greater than or equal
    Gte(Value),
    /// Less than
    Lt(Value),
    /// Less than or equal
    Lte(Value),
    /// The path value is within the list
    In(Vec<Value>),
    /// The path value (array or string) contains the operand
    Contains(Value),
    /// The path resolves (true) or does not resolve (false)
    Exists(bool),
    /// The path value is `null` (true) or is not (false)
    IsNull(bool),
    /// The path value (string) starts with the prefix
    StartsWith(String),
    /// The path value (string) ends with the suffix
    EndsWith(String),
    /// The path value (string) matches the regular expression
    Matches(String),
    /// Operator outside the spec: resolved against the operator
    /// registry at evaluation time (host vocabulary extension)
    Custom {
        /// Operator key in the JSON
        key: String,
        /// Declared operand
        operand: Value,
    },
}

impl CompareOp {
    /// Builds the operator from the JSON key and operand.
    /// Unknown keys produce [`CompareOp::Custom`].
    pub fn from_key_operand(key: String, operand: Value) -> Result<Self, String> {
        fn expect_bool(key: &str, operand: Value) -> Result<bool, String> {
            operand
                .as_bool()
                .ok_or_else(|| format!("'{key}' expects a boolean"))
        }
        fn expect_string(key: &str, operand: Value) -> Result<String, String> {
            match operand {
                Value::String(s) => Ok(s),
                _ => Err(format!("'{key}' expects a string")),
            }
        }

        Ok(match key.as_str() {
            "eq" => CompareOp::Eq(operand),
            "ne" => CompareOp::Ne(operand),
            "gt" => CompareOp::Gt(operand),
            "gte" => CompareOp::Gte(operand),
            "lt" => CompareOp::Lt(operand),
            "lte" => CompareOp::Lte(operand),
            "in" => match operand {
                Value::Array(items) => CompareOp::In(items),
                _ => return Err("'in' expects a list of values".to_string()),
            },
            "contains" => CompareOp::Contains(operand),
            "exists" => CompareOp::Exists(expect_bool("exists", operand)?),
            "is_null" => CompareOp::IsNull(expect_bool("is_null", operand)?),
            "starts_with" => CompareOp::StartsWith(expect_string("starts_with", operand)?),
            "ends_with" => CompareOp::EndsWith(expect_string("ends_with", operand)?),
            "matches" => CompareOp::Matches(expect_string("matches", operand)?),
            _ => CompareOp::Custom { key, operand },
        })
    }

    /// Operator key as it appears in JSON
    pub fn key(&self) -> &str {
        match self {
            CompareOp::Eq(_) => "eq",
            CompareOp::Ne(_) => "ne",
            CompareOp::Gt(_) => "gt",
            CompareOp::Gte(_) => "gte",
            CompareOp::Lt(_) => "lt",
            CompareOp::Lte(_) => "lte",
            CompareOp::In(_) => "in",
            CompareOp::Contains(_) => "contains",
            CompareOp::Exists(_) => "exists",
            CompareOp::IsNull(_) => "is_null",
            CompareOp::StartsWith(_) => "starts_with",
            CompareOp::EndsWith(_) => "ends_with",
            CompareOp::Matches(_) => "matches",
            CompareOp::Custom { key, .. } => key,
        }
    }

    /// Operand as `Value` (for serialization and tooling)
    pub fn operand(&self) -> Value {
        match self {
            CompareOp::Eq(v)
            | CompareOp::Ne(v)
            | CompareOp::Gt(v)
            | CompareOp::Gte(v)
            | CompareOp::Lt(v)
            | CompareOp::Lte(v)
            | CompareOp::Contains(v) => v.clone(),
            CompareOp::In(items) => Value::Array(items.clone()),
            CompareOp::Exists(b) | CompareOp::IsNull(b) => Value::Bool(*b),
            CompareOp::StartsWith(s) | CompareOp::EndsWith(s) | CompareOp::Matches(s) => {
                Value::String(s.clone())
            }
            CompareOp::Custom { operand, .. } => operand.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserializa_y_roundtrip_del_ejemplo_de_la_spec() {
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
        assert_eq!(serde_json::to_value(&parsed).unwrap(), cond);
    }

    #[test]
    fn operador_desconocido_deserializa_como_custom() {
        let cond: Condition =
            serde_json::from_value(json!({ "path": "$.x", "between": [1, 10] })).unwrap();
        let Condition::Compare(cmp) = &cond else {
            panic!("expected comparison");
        };
        assert_eq!(cmp.op.key(), "between");
        assert_eq!(cmp.op.operand(), json!([1, 10]));
        // roundtrip is also stable for custom
        assert_eq!(
            serde_json::to_value(&cond).unwrap(),
            json!({ "path": "$.x", "between": [1, 10] })
        );
    }

    #[test]
    fn errores_de_deserializacion_utiles() {
        let err = serde_json::from_value::<Condition>(json!({ "foo": 1 }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("'and', 'or', 'not'"), "message: {err}");
        assert!(err.contains("foo"), "message: {err}");

        let err = serde_json::from_value::<Condition>(json!({
            "and": [{ "path": "$.x", "eq": 1 }],
            "extra": true
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("does not allow extra keys"), "message: {err}");

        let err = serde_json::from_value::<Condition>(json!({
            "path": "$.x", "eq": 1, "gt": 2
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("exactly one operator"), "message: {err}");

        let err = serde_json::from_value::<Condition>(json!({ "path": "$.x" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("exactly one operator"), "message: {err}");
    }

    #[test]
    fn operandos_tipados_se_verifican() {
        let err = serde_json::from_value::<Condition>(json!({ "path": "$.x", "in": 5 }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("expects a list"), "message: {err}");

        let err = serde_json::from_value::<Condition>(json!({ "path": "$.x", "exists": "si" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("expects a boolean"), "message: {err}");

        let err = serde_json::from_value::<Condition>(json!({ "path": "$.x", "matches": 1 }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("expects a string"), "message: {err}");
    }
}
