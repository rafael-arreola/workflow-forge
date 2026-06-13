//! `data.merge`: merge profundo de objetos, en orden.

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::{Task, TaskManifest, WorkflowData, WorkflowResult};

use crate::schema;

/// Tarea `data.merge`: fusiona los objetos de `objects` en orden; las llaves
/// posteriores ganan, arrays y escalares se reemplazan completos.
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
}
