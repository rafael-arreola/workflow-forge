use serde_json::json;
use std::sync::Arc;
use workflow_forge_protocol::*;

struct Saludar {
    descriptor: OperationDescriptor,
}

impl Operation for Saludar {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }

    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let Some(nombre) = invocation.input.as_str() else {
                return Err(OperationError {
                    code: "manual.saludos.input".into(),
                    class: ErrorClass::InvalidInput,
                    certainty: EffectCertainty::NotApplied,
                    message: "Se esperaba un nombre de tipo string".into(),
                });
            };
            let Some(prefijo) = invocation.config["prefijo"].as_str() else {
                return Err(OperationError {
                    code: "manual.saludos.config".into(),
                    class: ErrorClass::InvalidInput,
                    certainty: EffectCertainty::NotApplied,
                    message: "Falta el prefijo de tipo string".into(),
                });
            };
            Ok(OperationOutput::json(json!(format!("{prefijo}{nombre}"))))
        })
    }
}

pub fn operaciones() -> OperationBundle {
    let revision = OperationRevision::new("manual.saludos.crear", "1", "r1");
    let descriptor = OperationDescriptor {
        revision: revision.clone(),
        schema_dialect: SCHEMA_DIALECT.into(),
        config_schema: json!({
            "$schema": SCHEMA_DIALECT, "type": "object",
            "required": ["prefijo"], "additionalProperties": false,
            "properties": {"prefijo": {"type": "string", "maxLength": 64}}
        }),
        input_schema: json!({"$schema": SCHEMA_DIALECT, "type": "string", "maxLength": 128}),
        output_schema: json!({"$schema": SCHEMA_DIALECT, "type": "string", "maxLength": 192}),
        effect: EffectKind::Pure,
        repetition: Repetition::Safe,
        reconciliation: false,
        required_resources: Default::default(),
        description: "Construye un saludo sin producir efectos externos".into(),
        examples: vec![
            json!({"config":{"prefijo":"Hola, "},"input":"Rafael","output":"Hola, Rafael"}),
        ],
    };
    OperationBundle {
        module: ModuleDescriptor {
            id: "manual.saludos".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![revision],
        },
        operations: vec![Arc::new(Saludar { descriptor })],
        inspectors: vec![],
    }
}
