//! `data.cast`: conversiones declarativas por campo sobre objetos o filas.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::{Task, TaskManifest, WorkflowData, WorkflowResult};

use crate::{codes, schema};

/// Tarea `data.cast`: convierte campos de un objeto o de un array de filas
/// (fechas, números, enteros, booleanos, strings, trim, upper/lower, replace
/// y defaults), con política `on_invalid` para valores inconvertibles.
pub struct CastTask {
    manifest: TaskManifest,
}

impl Default for CastTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("data.cast");
        manifest.description = Some(
            "Conversiones declarativas por campo sobre un objeto o un array de \
             filas: fechas, números, enteros, booleanos, strings, trim, \
             upper/lower, replace y defaults. `on_invalid` controla qué pasa \
             con un valor inconvertible: fail (default), null o collect"
                .into(),
        );
        let op_schema = json!({
            "oneOf": [
                { "type": "object", "required": ["op", "from"],
                  "properties": {
                      "op": { "const": "date" },
                      "from": { "type": "string", "description": "Formato de entrada (strftime, ej. %d/%m/%Y)" },
                      "to": { "type": "string", "description": "Formato de salida; default %Y-%m-%d" }
                  }, "additionalProperties": false },
                { "type": "object", "required": ["op"],
                  "properties": {
                      "op": { "const": "number" },
                      "decimal": { "type": "string", "maxLength": 1, "description": "Separador decimal de entrada, ej. \",\"" },
                      "thousands": { "type": "string", "maxLength": 1, "description": "Separador de miles a remover, ej. \".\"" }
                  }, "additionalProperties": false },
                { "type": "object", "required": ["op"],
                  "properties": { "op": { "enum": ["int", "bool", "string", "trim", "upper", "lower"] } },
                  "additionalProperties": false },
                { "type": "object", "required": ["op", "value"],
                  "properties": { "op": { "const": "default" }, "value": {} },
                  "additionalProperties": false },
                { "type": "object", "required": ["op", "from", "to"],
                  "properties": {
                      "op": { "const": "replace" },
                      "from": { "type": "string" },
                      "to": { "type": "string" }
                  }, "additionalProperties": false }
            ]
        });
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["source", "fields"],
            "properties": {
                "source": {
                    "description": "Objeto o array de objetos (filas) a convertir",
                    "type": ["object", "array"]
                },
                "fields": {
                    "description": "Operaciones por campo; la llave es un path con puntos relativo a cada fila",
                    "type": "object",
                    "additionalProperties": { "type": "array", "items": op_schema, "minItems": 1 }
                },
                "on_invalid": {
                    "description": "fail: la tarea falla con CAST_FIELD_INVALID; null: el campo queda null; collect: separa filas en { ok, failed }",
                    "enum": ["fail", "null", "collect"],
                    "default": "fail"
                }
            }
        })));
        Self { manifest }
    }
}

#[derive(Deserialize)]
struct CastInput {
    source: Value,
    fields: BTreeMap<String, Vec<CastOp>>,
    #[serde(default)]
    on_invalid: OnInvalid,
}

#[derive(Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OnInvalid {
    #[default]
    Fail,
    Null,
    Collect,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum CastOp {
    Date {
        from: String,
        #[serde(default = "iso_date")]
        to: String,
    },
    Number {
        #[serde(default)]
        decimal: Option<char>,
        #[serde(default)]
        thousands: Option<char>,
    },
    Int,
    Bool,
    String,
    Trim,
    Upper,
    Lower,
    Default {
        value: Value,
    },
    Replace {
        from: String,
        to: String,
    },
}

fn iso_date() -> String {
    "%Y-%m-%d".into()
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn expect_str(value: &Value, op: &str) -> Result<String, String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("{op} espera string, recibió {}", json_type(value)))
}

fn apply_op(value: Value, op: &CastOp) -> Result<Value, String> {
    match op {
        // Default se aplica en apply_ops (sustituye null); aquí pasa de largo
        CastOp::Default { .. } => Ok(value),
        CastOp::Date { from, to } => {
            let s = expect_str(&value, "date")?;
            let s = s.trim();
            let parsed = chrono::NaiveDateTime::parse_from_str(s, from).or_else(|_| {
                chrono::NaiveDate::parse_from_str(s, from)
                    .map(|d| d.and_hms_opt(0, 0, 0).expect("medianoche válida"))
            });
            let parsed =
                parsed.map_err(|_| format!("'{s}' no coincide con el formato '{from}'"))?;
            let mut out = std::string::String::new();
            use std::fmt::Write;
            write!(out, "{}", parsed.format(to))
                .map_err(|_| format!("el formato de salida '{to}' es inválido"))?;
            Ok(Value::String(out))
        }
        CastOp::Number { decimal, thousands } => match value {
            Value::Number(_) => Ok(value),
            Value::String(s) => {
                let mut t = s.trim().to_string();
                if let Some(sep) = thousands {
                    t = t.replace(*sep, "");
                }
                if let Some(sep) = decimal {
                    t = t.replace(*sep, ".");
                }
                if let Ok(i) = t.parse::<i64>() {
                    return Ok(json!(i));
                }
                let n: f64 = t.parse().map_err(|_| format!("'{s}' no es un número"))?;
                serde_json::Number::from_f64(n)
                    .map(Value::Number)
                    .ok_or_else(|| format!("'{s}' no es un número JSON representable"))
            }
            other => Err(format!(
                "number espera string o number, recibió {}",
                json_type(&other)
            )),
        },
        CastOp::Int => match value {
            Value::Number(ref n) if n.is_i64() || n.is_u64() => Ok(value),
            Value::Number(n) => {
                let f = n.as_f64().unwrap_or(f64::NAN);
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(json!(f as i64))
                } else {
                    Err(format!("{n} no es un entero"))
                }
            }
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(|i| json!(i))
                .map_err(|_| format!("'{s}' no es un entero")),
            other => Err(format!(
                "int espera string o number, recibió {}",
                json_type(&other)
            )),
        },
        CastOp::Bool => match value {
            Value::Bool(_) => Ok(value),
            Value::String(s) => match s.trim().to_lowercase().as_str() {
                "true" | "1" | "yes" | "si" | "sí" => Ok(json!(true)),
                "false" | "0" | "no" => Ok(json!(false)),
                _ => Err(format!("'{s}' no es un booleano")),
            },
            Value::Number(n) if n.as_i64() == Some(1) => Ok(json!(true)),
            Value::Number(n) if n.as_i64() == Some(0) => Ok(json!(false)),
            other => Err(format!(
                "bool espera string, number o bool, recibió {}",
                json_type(&other)
            )),
        },
        CastOp::String => match value {
            Value::String(_) => Ok(value),
            Value::Number(n) => Ok(Value::String(n.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            other => Err(format!(
                "string espera un escalar, recibió {}",
                json_type(&other)
            )),
        },
        CastOp::Trim => expect_str(&value, "trim").map(|s| Value::String(s.trim().to_string())),
        CastOp::Upper => expect_str(&value, "upper").map(|s| Value::String(s.to_uppercase())),
        CastOp::Lower => expect_str(&value, "lower").map(|s| Value::String(s.to_lowercase())),
        CastOp::Replace { from, to } => {
            expect_str(&value, "replace").map(|s| Value::String(s.replace(from, to)))
        }
    }
}

/// Null/ausente atraviesa las ops sin error; solo `default` lo sustituye.
fn apply_ops(mut value: Value, ops: &[CastOp]) -> Result<Value, String> {
    for op in ops {
        if let CastOp::Default { value: fallback } = op {
            if value.is_null() {
                value = fallback.clone();
            }
            continue;
        }
        if value.is_null() {
            continue;
        }
        value = apply_op(value, op)?;
    }
    Ok(value)
}

fn get_path<'a>(row: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(row, |cur, key| cur.get(key))
}

fn set_path(row: &mut Value, path: &str, new: Value) -> Result<(), String> {
    let mut segments = path.split('.').peekable();
    let mut current = row;
    while let Some(segment) = segments.next() {
        let object = current.as_object_mut().ok_or_else(|| {
            format!("no se puede escribir '{path}': la ruta atraviesa un valor que no es objeto")
        })?;
        if segments.peek().is_none() {
            object.insert(segment.to_string(), new);
            return Ok(());
        }
        current = object
            .entry(segment.to_string())
            .or_insert_with(|| json!({}));
    }
    Ok(())
}

/// Convierte los campos de una fila in-place. Devuelve los errores
/// `{ field, message }` encontrados (vacío si todo convirtió).
fn cast_row(
    row: &mut Value,
    fields: &BTreeMap<String, Vec<CastOp>>,
    on_invalid: OnInvalid,
) -> Vec<Value> {
    let mut errors = Vec::new();
    for (path, ops) in fields {
        let existed = get_path(row, path).is_some();
        let current = get_path(row, path).cloned().unwrap_or(Value::Null);
        let outcome = apply_ops(current, ops).and_then(|value| {
            if value.is_null() && !existed {
                // Un campo ausente que sigue null no se inserta
                return Ok(());
            }
            set_path(row, path, value)
        });
        if let Err(message) = outcome {
            if on_invalid == OnInvalid::Null && existed {
                let _ = set_path(row, path, Value::Null);
            }
            errors.push(json!({ "field": path, "message": message }));
        }
    }
    errors
}

fn field_error(row_index: Option<usize>, error: &Value) -> WorkflowError {
    let field = error["field"].as_str().unwrap_or("?");
    let message = error["message"].as_str().unwrap_or("valor inconvertible");
    let location = match row_index {
        Some(index) => format!("fila {index}, campo '{field}'"),
        None => format!("campo '{field}'"),
    };
    WorkflowError::new(
        codes::CAST_FIELD_INVALID,
        format!("data.cast: {location}: {message}"),
    )
}

#[async_trait]
impl Task for CastTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: CastInput = serde_json::from_value(input.0).map_err(|e| {
            WorkflowError::new(
                codes::CAST_INPUT_INVALID,
                format!("input de data.cast inválido: {e}"),
            )
        })?;

        // Un objeto suelto se procesa como lote de una fila; la forma del
        // output respeta la forma del source (salvo en collect: siempre {ok, failed})
        let source_was_object = parsed.source.is_object();
        let rows: Vec<Value> = match parsed.source {
            Value::Array(rows) => rows,
            object @ Value::Object(_) => vec![object],
            other => {
                return Err(WorkflowError::new(
                    codes::CAST_INPUT_INVALID,
                    format!(
                        "source debe ser objeto o array, recibió {}",
                        json_type(&other)
                    ),
                ));
            }
        };

        match parsed.on_invalid {
            OnInvalid::Collect => {
                let mut ok = Vec::new();
                let mut failed = Vec::new();
                for (index, row) in rows.into_iter().enumerate() {
                    let mut cast = row.clone();
                    let errors = cast_row(&mut cast, &parsed.fields, OnInvalid::Collect);
                    if errors.is_empty() {
                        ok.push(cast);
                    } else {
                        failed.push(json!({ "index": index, "item": row, "errors": errors }));
                    }
                }
                Ok(WorkflowData(json!({ "ok": ok, "failed": failed })))
            }
            mode => {
                let mut out = Vec::with_capacity(rows.len());
                for (index, mut row) in rows.into_iter().enumerate() {
                    let errors = cast_row(&mut row, &parsed.fields, mode);
                    if mode == OnInvalid::Fail
                        && let Some(error) = errors.first()
                    {
                        let row_index = (!source_was_object).then_some(index);
                        return Err(field_error(row_index, error));
                    }
                    out.push(row);
                }
                if source_was_object {
                    Ok(WorkflowData(out.into_iter().next().unwrap_or(Value::Null)))
                } else {
                    Ok(WorkflowData(Value::Array(out)))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops(value: Value) -> Vec<CastOp> {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn cast_fechas_numeros_y_strings() {
        let cases = [
            (
                json!("10/06/2026"),
                json!([{ "op": "date", "from": "%d/%m/%Y" }]),
                json!("2026-06-10"),
            ),
            (
                json!("10/06/2026 14:30"),
                json!([{ "op": "date", "from": "%d/%m/%Y %H:%M", "to": "%Y-%m-%dT%H:%M:%S" }]),
                json!("2026-06-10T14:30:00"),
            ),
            (
                json!("1.234,56"),
                json!([{ "op": "number", "decimal": ",", "thousands": "." }]),
                json!(1234.56),
            ),
            (json!("1234"), json!([{ "op": "number" }]), json!(1234)),
            (json!("42"), json!([{ "op": "int" }]), json!(42)),
            (json!(42.0), json!([{ "op": "int" }]), json!(42)),
            (json!("Sí"), json!([{ "op": "bool" }]), json!(true)),
            (json!(0), json!([{ "op": "bool" }]), json!(false)),
            (json!(99), json!([{ "op": "string" }]), json!("99")),
            (
                json!("  abc-123 "),
                json!([{ "op": "trim" }, { "op": "upper" }, { "op": "replace", "from": "-", "to": "_" }]),
                json!("ABC_123"),
            ),
            (json!("MiXto"), json!([{ "op": "lower" }]), json!("mixto")),
        ];
        for (input, ops_json, expected) in cases {
            let result = apply_ops(input.clone(), &ops(ops_json.clone())).unwrap();
            assert_eq!(result, expected, "input {input} ops {ops_json}");
        }
    }

    #[test]
    fn cast_null_atraviesa_y_default_sustituye() {
        let chain = ops(json!([{ "op": "trim" }, { "op": "upper" }]));
        assert_eq!(apply_ops(Value::Null, &chain).unwrap(), Value::Null);

        let with_default = ops(json!([{ "op": "default", "value": "n/a" }, { "op": "upper" }]));
        assert_eq!(apply_ops(Value::Null, &with_default).unwrap(), json!("N/A"));
        // default no pisa valores presentes
        assert_eq!(apply_ops(json!("x"), &with_default).unwrap(), json!("X"));
    }

    #[test]
    fn cast_valores_inconvertibles_dan_error() {
        let invalid = [
            (json!("abc"), json!([{ "op": "date", "from": "%d/%m/%Y" }])),
            (json!("abc"), json!([{ "op": "number" }])),
            (json!(12.5), json!([{ "op": "int" }])),
            (json!("quizá"), json!([{ "op": "bool" }])),
            (json!({ "a": 1 }), json!([{ "op": "string" }])),
            (json!(5), json!([{ "op": "trim" }])),
        ];
        for (input, ops_json) in invalid {
            assert!(
                apply_ops(input.clone(), &ops(ops_json.clone())).is_err(),
                "input {input} ops {ops_json} debió fallar"
            );
        }
    }

    #[test]
    fn cast_row_paths_anidados_y_errores() {
        let fields: BTreeMap<String, Vec<CastOp>> = serde_json::from_value(json!({
            "cliente.nombre": [{ "op": "trim" }],
            "total": [{ "op": "number", "decimal": "," }],
            "notas": [{ "op": "default", "value": "" }]
        }))
        .unwrap();

        let mut row = json!({ "cliente": { "nombre": "  ada " }, "total": "12,5" });
        let errors = cast_row(&mut row, &fields, OnInvalid::Fail);
        assert!(errors.is_empty());
        assert_eq!(
            row,
            json!({ "cliente": { "nombre": "ada" }, "total": 12.5, "notas": "" })
        );

        let mut bad = json!({ "cliente": { "nombre": "ada" }, "total": "abc" });
        let errors = cast_row(&mut bad, &fields, OnInvalid::Null);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["field"], json!("total"));
        assert_eq!(bad["total"], Value::Null);
    }
}
