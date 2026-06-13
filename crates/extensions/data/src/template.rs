//! `data.template`: interpolación de placeholders sobre un objeto de valores.

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::{Task, TaskManifest, WorkflowData, WorkflowResult};

use crate::{codes, schema};

/// Tarea `data.template`: interpola placeholders `{path.con.puntos}` de
/// `values` en `template`; `{{` y `}}` escapan llaves literales.
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
                                codes::TEMPLATE_INVALID,
                                format!("Placeholder sin cerrar en el template: '{{{key}'"),
                            ));
                        }
                    }
                }
                let value = lookup(values, &key).ok_or_else(|| {
                    WorkflowError::new(
                        codes::TEMPLATE_VALUE_MISSING,
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
    fn template_interpola_y_escapa() {
        let values = json!({ "user": { "name": "ada" }, "count": 3 });
        let out = render("Hola {user.name}, tienes {count} ({{literal}})", &values).unwrap();
        assert_eq!(out, "Hola ada, tienes 3 ({literal})");

        let err = render("falta {nope}", &values).unwrap_err();
        assert_eq!(err.code, codes::TEMPLATE_VALUE_MISSING);

        let err = render("sin cerrar {abc", &values).unwrap_err();
        assert_eq!(err.code, codes::TEMPLATE_INVALID);
    }
}
