//! `data.map`: aplica un shape a cada elemento de un array.

use async_trait::async_trait;
use serde_json::Value;

use workflow_forge_core::expr::apply_shape;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::{Task, TaskManifest, WorkflowData, WorkflowResult};

use crate::schema;

/// Tarea `data.map`: aplica un `shape` (paths `@.` relativos a cada
/// elemento) a cada elemento de `items` y devuelve el array resultante.
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
        manifest.input_schema = Some(schema(serde_json::json!({
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
        manifest.output_schema = Some(schema(serde_json::json!({ "type": "array" })));
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
