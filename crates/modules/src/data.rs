use serde_json::{Value, json};
use std::sync::Arc;
use workflow_forge_protocol::*;

struct DataOperation {
    descriptor: OperationDescriptor,
    trim: bool,
}

impl Operation for DataOperation {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let value = if self.trim {
                let text = invocation.input.as_str().ok_or_else(|| OperationError {
                    code: "forge.text.invalid_input".into(),
                    class: ErrorClass::InvalidInput,
                    certainty: EffectCertainty::NotApplied,
                    message: "Expected a string".into(),
                })?;
                Value::String(text.trim().into())
            } else {
                invocation.input
            };
            Ok(OperationOutput::json(value))
        })
    }
}

pub fn data_operations() -> OperationBundle {
    let operations: Vec<Arc<dyn Operation>> = [("forge.data.identity", false), ("forge.text.trim", true)].into_iter().map(|(id, trim)| {
        Arc::new(DataOperation {
            descriptor: OperationDescriptor {
                revision: OperationRevision::new(id, "1", "r1"),
                schema_dialect: SCHEMA_DIALECT.into(),
                config_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","additionalProperties":false}),
                input_schema: if trim { json!({"$schema":SCHEMA_DIALECT,"type":"string"}) } else { Value::Bool(true) },
                output_schema: if trim { json!({"$schema":SCHEMA_DIALECT,"type":"string"}) } else { Value::Bool(true) },
                effect: EffectKind::Pure, repetition: Repetition::Safe, reconciliation: false,
                required_resources: Default::default(),
                description: if trim { "Remove leading and trailing whitespace" } else { "Return the input JSON unchanged" }.into(),
                examples: vec![json!({"input":" Ada ","output":if trim {"Ada"} else {" Ada "}})],
            }, trim,
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
