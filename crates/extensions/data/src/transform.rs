//! `data.transform`: reshape de un documento según un shape declarativo.

use async_trait::async_trait;
use serde_json::Value;

use workflow_forge_core::expr::apply_shape;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::{Task, TaskManifest, WorkflowData, WorkflowResult};

use crate::schema;

/// Tarea `data.transform`: aplica un `shape` (paths `@.` relativos a
/// `source`) y devuelve la estructura resultante.
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
        manifest.input_schema = Some(schema(serde_json::json!({
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
