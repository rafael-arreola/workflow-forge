//! Extensión `data` de workflow-forge: aquí viven todas las transformaciones
//! (decisión #14 de la spec: los mappings de nodos no transforman).
//!
//! | Tarea | Contrato |
//! |-------|----------|
//! | `data.transform` | Reshape de `source` según `shape` (paths `@.` relativos al source) |
//! | `data.map` | Aplica `shape` a CADA elemento de `items` (paths `@.` relativos al elemento) |
//! | `data.merge` | Merge profundo de `objects`; las llaves posteriores ganan |
//! | `data.template` | Interpola `{path.con.puntos}` de `values` en `template` |

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::context::WorkflowContext;
use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::shape::apply_shape;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Registra todas las tareas de la extensión en el registry
pub fn register(registry: &TaskRegistry) {
    registry.register(TransformTask::default());
    registry.register(MapTask::default());
    registry.register(MergeTask::default());
    registry.register(TemplateTask::default());
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("schema estático válido")
}

// ---------------------------------------------------------------------------
// data.transform
// ---------------------------------------------------------------------------

pub struct TransformTask {
    manifest: TaskManifest,
}

impl Default for TransformTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("data.transform");
        manifest.description = Some(
            "Reshape de `source` según `shape`. Los strings de `shape` que empiezan \
             con `@.` son JSONPath relativos al source (`@` solo es el source \
             completo); un path ausente produce null"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["source", "shape"],
            "properties": {
                "source": { "description": "Documento de origen (típicamente un mapping $.nodes.x.output)" },
                "shape": { "description": "Estructura de salida; strings `@.path` se resuelven contra source" }
            }
        })));
        Self { manifest }
    }
}

#[async_trait]
impl Task for TransformTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let source = input.get("source").cloned().unwrap_or(Value::Null);
        let shape = input.get("shape").cloned().unwrap_or(Value::Null);
        apply_shape(&shape, &source).map(WorkflowData)
    }
}

// ---------------------------------------------------------------------------
// data.map
// ---------------------------------------------------------------------------

pub struct MapTask {
    manifest: TaskManifest,
}

impl Default for MapTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("data.map");
        manifest.description = Some(
            "Aplica `shape` a cada elemento de `items` y devuelve el array \
             resultante. Los strings `@.path` del shape se resuelven contra \
             cada elemento (`@` solo es el elemento completo); un path ausente \
             produce null"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["items", "shape"],
            "properties": {
                "items": {
                    "description": "Array a transformar (p.ej. $.nodes.parse.output.rows)",
                    "type": "array"
                },
                "shape": { "description": "Estructura de salida por elemento; strings `@.path` se resuelven contra el elemento" }
            }
        })));
        manifest.output_schema = Some(schema(json!({ "type": "array" })));
        Self { manifest }
    }
}

#[async_trait]
impl Task for MapTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let Some(Value::Array(items)) = input.get("items").cloned() else {
            return Ok(WorkflowData(Value::Array(vec![])));
        };
        let shape = input.get("shape").cloned().unwrap_or(Value::Null);

        let mapped = items
            .iter()
            .map(|item| apply_shape(&shape, item))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(WorkflowData(Value::Array(mapped)))
    }
}

// ---------------------------------------------------------------------------
// data.merge
// ---------------------------------------------------------------------------

pub struct MergeTask {
    manifest: TaskManifest,
}

impl Default for MergeTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("data.merge");
        manifest.description = Some(
            "Merge profundo de los objetos de `objects`, en orden: las llaves \
             posteriores ganan; arrays y escalares se reemplazan completos"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["objects"],
            "properties": {
                "objects": {
                    "type": "array",
                    "items": { "type": "object" },
                    "minItems": 1
                }
            }
        })));
        Self { manifest }
    }
}

fn deep_merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(existing) => deep_merge(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

#[async_trait]
impl Task for MergeTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let Some(Value::Array(objects)) = input.get("objects").cloned() else {
            return Ok(WorkflowData(json!({})));
        };
        let mut merged = Value::Object(serde_json::Map::new());
        for object in objects {
            deep_merge(&mut merged, object);
        }
        Ok(WorkflowData(merged))
    }
}

// ---------------------------------------------------------------------------
// data.template
// ---------------------------------------------------------------------------

pub struct TemplateTask {
    manifest: TaskManifest,
}

impl Default for TemplateTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("data.template");
        manifest.description = Some(
            "Interpola placeholders `{path.con.puntos}` de `values` en `template`; \
             `{{` y `}}` escapan llaves literales"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["template", "values"],
            "properties": {
                "template": { "type": "string" },
                "values": { "type": "object" }
            }
        })));
        manifest.output_schema = Some(schema(json!({ "type": "string" })));
        Self { manifest }
    }
}

fn lookup<'a>(values: &'a Value, dotted: &str) -> Option<&'a Value> {
    dotted
        .split('.')
        .try_fold(values, |current, key| current.get(key))
}

fn render(template: &str, values: &Value) -> Result<String, WorkflowError> {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                out.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                out.push('}');
            }
            '{' => {
                let mut key = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some(c) => key.push(c),
                        None => {
                            return Err(WorkflowError::new(
                                "TEMPLATE_INVALID",
                                format!("Placeholder sin cerrar en el template: '{{{key}'"),
                            ));
                        }
                    }
                }
                let value = lookup(values, &key).ok_or_else(|| {
                    WorkflowError::new(
                        "TEMPLATE_VALUE_MISSING",
                        format!("El placeholder '{{{key}}}' no existe en values"),
                    )
                })?;
                match value {
                    Value::String(s) => out.push_str(s),
                    other => out.push_str(&other.to_string()),
                }
            }
            c => out.push(c),
        }
    }
    Ok(out)
}

#[async_trait]
impl Task for TemplateTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let template = input
            .get("template")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let values = input.get("values").cloned().unwrap_or(json!({}));
        render(template, &values).map(|s| WorkflowData(Value::String(s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_profundo_posteriores_ganan() {
        let mut base = json!({ "a": { "x": 1, "y": 2 }, "lista": [1, 2] });
        deep_merge(
            &mut base,
            json!({ "a": { "y": 99, "z": 3 }, "lista": [9], "nuevo": true }),
        );
        assert_eq!(
            base,
            json!({ "a": { "x": 1, "y": 99, "z": 3 }, "lista": [9], "nuevo": true })
        );
    }

    #[test]
    fn template_interpola_y_escapa() {
        let values = json!({ "user": { "name": "ada" }, "count": 3 });
        let out = render("Hola {user.name}, tienes {count} ({{literal}})", &values).unwrap();
        assert_eq!(out, "Hola ada, tienes 3 ({literal})");

        let err = render("falta {nope}", &values).unwrap_err();
        assert_eq!(err.code, "TEMPLATE_VALUE_MISSING");

        let err = render("sin cerrar {abc", &values).unwrap_err();
        assert_eq!(err.code, "TEMPLATE_INVALID");
    }
}
