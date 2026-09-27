//! A small extension recipe: immutable config, per-invocation data and no engine imports.
use serde_json::json;
use std::sync::Arc;
use workflow_forge_protocol::*;

struct Prefix {
    descriptor: OperationDescriptor,
}

impl Operation for Prefix {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }

    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let prefix = invocation.config.get("prefix").and_then(|v| v.as_str());
            let text = invocation.input.as_str();
            let (Some(prefix), Some(text)) = (prefix, text) else {
                return Err(OperationError {
                    code: "example.text.invalid_input".into(),
                    class: ErrorClass::InvalidInput,
                    certainty: EffectCertainty::NotApplied,
                    message: "Expected string input and prefix configuration".into(),
                });
            };
            Ok(OperationOutput::json(json!(format!("{prefix}{text}"))))
        })
    }
}

/// Factory Function: return explicit contributions without changing host providers.
pub fn text_operations() -> OperationBundle {
    let revision = OperationRevision::new("example.text.prefix", "1", "r1");
    let operation = Prefix {
        descriptor: OperationDescriptor {
            revision: revision.clone(),
            schema_dialect: SCHEMA_DIALECT.into(),
            config_schema: json!({
                "$schema":SCHEMA_DIALECT,"type":"object","required":["prefix"],
                "properties":{"prefix":{"type":"string","maxLength":128}},
                "additionalProperties":false
            }),
            input_schema: json!({"$schema":SCHEMA_DIALECT,"type":"string","maxLength":1024}),
            output_schema: json!({"$schema":SCHEMA_DIALECT,"type":"string","maxLength":1152}),
            effect: EffectKind::Pure,
            repetition: Repetition::Safe,
            reconciliation: false,
            required_resources: Default::default(),
            description: "Prefix an input string with explicit reusable configuration".into(),
            examples: vec![json!({"config":{"prefix":"ID-"},"input":"42","output":"ID-42"})],
        },
    };
    OperationBundle {
        module: ModuleDescriptor {
            id: "example.text".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![revision],
        },
        operations: vec![Arc::new(operation)],
        inspectors: Vec::new(),
    }
}
