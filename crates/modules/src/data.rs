use serde_json::{Value, json};
use std::sync::Arc;
use workflow_forge_protocol::*;

#[derive(Clone, Copy)]
enum DataKind {
    Identity,
    Trim,
    Equals,
}

struct DataOperation {
    descriptor: OperationDescriptor,
    kind: DataKind,
}

impl Operation for DataOperation {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let value = match self.kind {
                DataKind::Identity => invocation.input,
                DataKind::Trim => Value::String(
                    invocation
                        .input
                        .as_str()
                        .ok_or_else(|| OperationError {
                            code: "forge.text.invalid_input".into(),
                            class: ErrorClass::InvalidInput,
                            certainty: EffectCertainty::NotApplied,
                            message: "Expected a string".into(),
                        })?
                        .trim()
                        .into(),
                ),
                DataKind::Equals => {
                    let left = invocation.input.get("left");
                    let right = invocation.input.get("right");
                    if left.is_none() || right.is_none() {
                        return Err(OperationError {
                            code: "forge.data.invalid_input".into(),
                            class: ErrorClass::InvalidInput,
                            certainty: EffectCertainty::NotApplied,
                            message: "Expected left and right values".into(),
                        });
                    }
                    Value::Bool(left == right)
                }
            };
            Ok(OperationOutput::json(value))
        })
    }
}

pub fn data_operations() -> OperationBundle {
    let operations: Vec<Arc<dyn Operation>> = [
        ("forge.data.identity", DataKind::Identity, "Return the input JSON unchanged"),
        ("forge.text.trim", DataKind::Trim, "Remove leading and trailing whitespace"),
        ("forge.data.equals", DataKind::Equals, "Compare two JSON values with structural equality"),
    ].into_iter().map(|(id, kind, description)| {
        let (input_schema, output_schema, examples) = match kind {
            DataKind::Identity => (Value::Bool(true), Value::Bool(true), vec![json!({"input":42,"output":42})]),
            DataKind::Trim => (json!({"$schema":SCHEMA_DIALECT,"type":"string"}), json!({"$schema":SCHEMA_DIALECT,"type":"string"}), vec![json!({"input":" Ada ","output":"Ada"})]),
            DataKind::Equals => (json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["left","right"],"properties":{"left":true,"right":true},"additionalProperties":false}), json!({"$schema":SCHEMA_DIALECT,"type":"boolean"}), vec![json!({"input":{"left":404,"right":404},"output":true})]),
        };
        Arc::new(DataOperation {
            descriptor: OperationDescriptor {
                revision: OperationRevision::new(id, "1", "r1"),
                schema_dialect: SCHEMA_DIALECT.into(),
                config_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","additionalProperties":false}),
                input_schema, output_schema,
                effect: EffectKind::Pure, repetition: Repetition::Safe, reconciliation: false,
                required_resources: Default::default(), description:description.into(), examples,
            }, kind,
        }) as Arc<dyn Operation>
    }).collect();
    OperationBundle {
        inspectors: Vec::new(),
        module: ModuleDescriptor {
            id: "forge.data".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: operations
                .iter()
                .map(|o| o.descriptor().revision.clone())
                .collect(),
        },
        operations,
    }
}
