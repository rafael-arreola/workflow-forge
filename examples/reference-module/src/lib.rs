//! An external extension: no dependencies on the engine, facade or official modules.
pub mod inventory;
pub mod text;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use workflow_forge_protocol::*;

pub trait CustomerDirectory: Send + Sync {
    fn active<'a>(&'a self, customer: &'a str) -> PortFuture<'a, bool, OperationError>;
}

pub struct StaticDirectory(pub BTreeMap<String, bool>);
impl CustomerDirectory for StaticDirectory {
    fn active<'a>(&'a self, customer: &'a str) -> PortFuture<'a, bool, OperationError> {
        Box::pin(async move { Ok(self.0.get(customer).copied().unwrap_or(false)) })
    }
}

struct Lookup {
    descriptor: OperationDescriptor,
    directory: Arc<dyn CustomerDirectory>,
}
impl Operation for Lookup {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let customer = invocation
                .input
                .get("customer")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| OperationError {
                    code: "reference.customer.invalid".into(),
                    class: ErrorClass::InvalidInput,
                    certainty: EffectCertainty::NotApplied,
                    message: "Expected a customer identifier".into(),
                })?;
            let active = self.directory.active(customer).await?;
            Ok(OperationOutput::json(json!({"active":active})))
        })
    }
}

pub fn customer_operations(directory: Arc<dyn CustomerDirectory>) -> OperationBundle {
    let revision = OperationRevision::new("reference.customer_lookup", "1", "r1");
    let descriptor = OperationDescriptor {
        revision: revision.clone(),
        schema_dialect: SCHEMA_DIALECT.into(),
        config_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","additionalProperties":false}),
        input_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["customer"],"properties":{"customer":{"type":"string","minLength":1}},"additionalProperties":false}),
        output_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["active"],"properties":{"active":{"type":"boolean"}},"additionalProperties":false}),
        effect: EffectKind::Read,
        repetition: Repetition::Safe,
        reconciliation: false,
        required_resources: Default::default(),
        description: "Look up customer availability through an injected directory".into(),
        examples: vec![json!({"input":{"customer":"C-9"},"output":{"active":true}})],
    };
    OperationBundle {
        inspectors: Vec::new(),
        module: ModuleDescriptor {
            id: "reference.customers".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![revision],
        },
        operations: vec![Arc::new(Lookup {
            descriptor,
            directory,
        })],
    }
}
