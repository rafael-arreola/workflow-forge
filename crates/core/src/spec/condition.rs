//! Sintaxis del mini-DSL de condiciones de la spec (solo tipos y serde).
//!
//! Una condición es un objeto JSON validable con JSON Schema: una
//! comparación sobre un path JSONPath, o una composición lógica (`and`,
//! `or`, `not`). La **evaluación** vive en [`crate::expr::operators`], junto
//! con el registro de operadores custom.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Condición del mini-DSL declarativo de la spec.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Condition {
    /// Conjunción: todas las condiciones deben cumplirse
    And {
        /// Condiciones que deben cumplirse todas
        and: Vec<Condition>,
    },
    /// Disyunción: al menos una condición debe cumplirse
    Or {
        /// Condiciones de las que al menos una debe cumplirse
        or: Vec<Condition>,
    },
    /// Negación
    Not {
        /// Condición a negar
        not: Box<Condition>,
    },
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
/// En JSON: `{ "path": "$.x", "<operador>": <operando> }`.
#[derive(Debug, Clone)]
pub struct Comparison {
    /// Path JSONPath evaluado contra el contexto de ejecución
    pub path: String,
    /// Operador y operando de la comparación
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
            return Err(D::Error::custom("una comparación debe ser un objeto JSON"));
        };
        let path = match map.remove("path") {
            Some(Value::String(path)) => path,
            Some(_) => return Err(D::Error::custom("'path' debe ser un string JSONPath")),
            None => return Err(D::Error::custom("una comparación requiere 'path'")),
        };
        if map.len() != 1 {
            let keys: Vec<&str> = map.keys().map(String::as_str).collect();
            return Err(D::Error::custom(format!(
                "una comparación requiere exactamente un operador junto a 'path'; \
                 llaves encontradas: [{}]",
                keys.join(", ")
            )));
        }
        let (key, operand) = map.into_iter().next().expect("len comprobado");
        let op = CompareOp::from_key_operand(key, operand).map_err(D::Error::custom)?;
        Ok(Comparison { path, op })
    }
}

/// Operadores de comparación. Los 13 de la spec 1.0 son vocabulario cerrado;
/// cualquier otra llave deserializa como [`CompareOp::Custom`] y se resuelve
/// contra el registro de operadores
/// ([`crate::expr::operators::OperatorRegistry`]) al evaluar.
///
/// Un path que no resuelve no es error: `exists` da `false` y el resto de
/// operadores built-in dan `false`.
#[derive(Debug, Clone)]
pub enum CompareOp {
    /// Igualdad estricta de valores JSON
    Eq(Value),
    /// Desigualdad (un path ausente da `false`, no `true`)
    Ne(Value),
    /// Mayor que (números entre sí, strings entre sí)
    Gt(Value),
    /// Mayor o igual que
    Gte(Value),
    /// Menor que
    Lt(Value),
    /// Menor o igual que
    Lte(Value),
    /// El valor del path está dentro de la lista
    In(Vec<Value>),
    /// El valor del path (array o string) contiene al operando
    Contains(Value),
    /// El path resuelve (true) o no resuelve (false)
    Exists(bool),
    /// El valor del path es `null` (true) o no lo es (false)
    IsNull(bool),
    /// El valor del path (string) empieza con el prefijo
    StartsWith(String),
    /// El valor del path (string) termina con el sufijo
    EndsWith(String),
    /// El valor del path (string) cumple la expresión regular
    Matches(String),
    /// Operador fuera de la spec: se resuelve contra el registro de
    /// operadores en la evaluación (extensión del vocabulario por el host)
    Custom {
        /// Llave del operador en el JSON
        key: String,
        /// Operando declarado
        operand: Value,
    },
}

impl CompareOp {
    /// Construye el operador desde la llave y el operando del JSON.
    /// Llaves desconocidas producen [`CompareOp::Custom`].
    pub fn from_key_operand(key: String, operand: Value) -> Result<Self, String> {
        fn expect_bool(key: &str, operand: Value) -> Result<bool, String> {
            operand
                .as_bool()
                .ok_or_else(|| format!("'{key}' espera un booleano"))
        }
        fn expect_string(key: &str, operand: Value) -> Result<String, String> {
            match operand {
                Value::String(s) => Ok(s),
                _ => Err(format!("'{key}' espera un string")),
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
                _ => return Err("'in' espera una lista de valores".to_string()),
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

    /// Llave del operador tal como aparece en el JSON
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

    /// Operando como `Value` (para serialización y tooling)
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
            panic!("se esperaba comparación");
        };
        assert_eq!(cmp.op.key(), "between");
        assert_eq!(cmp.op.operand(), json!([1, 10]));
        // roundtrip estable también para custom
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

        let err = serde_json::from_value::<Condition>(json!({
            "path": "$.x", "eq": 1, "gt": 2
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("exactamente un operador"), "mensaje: {err}");

        let err = serde_json::from_value::<Condition>(json!({ "path": "$.x" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("exactamente un operador"), "mensaje: {err}");
    }

    #[test]
    fn operandos_tipados_se_verifican() {
        let err = serde_json::from_value::<Condition>(json!({ "path": "$.x", "in": 5 }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("espera una lista"), "mensaje: {err}");

        let err = serde_json::from_value::<Condition>(json!({ "path": "$.x", "exists": "si" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("espera un booleano"), "mensaje: {err}");

        let err = serde_json::from_value::<Condition>(json!({ "path": "$.x", "matches": 1 }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("espera un string"), "mensaje: {err}");
    }
}
