//! Adapters use only protocol ports. The host supplies immutable resource policy;
//! workflows select exact exported revisions, never arbitrary credentials/targets.
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Write},
    sync::Arc,
};
use workflow_forge_protocol::*;

mod csv;
mod files;
mod http;
pub use csv::{CsvOptions, csv_operations};
pub use files::{FileReadProfile, file_operations};
pub use http::{HttpJsonProfile, HttpMethod, http_json_operations};

fn configured(message: &str) -> ForgeError {
    ForgeError::new("module.config", message)
}
fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}
fn revision(profile: &impl Serialize) -> Result<String, ForgeError> {
    let bytes =
        serde_json::to_vec(profile).map_err(|_| configured("Profile is not serializable"))?;
    Ok(format!("r1-{:x}", Sha256::digest(bytes)))
}
fn bundle(id: &str, operations: Vec<Arc<dyn Operation>>) -> OperationBundle {
    let exports: Vec<_> = operations
        .iter()
        .map(|o| o.descriptor().revision.clone())
        .collect();
    OperationBundle {
        module: ModuleDescriptor {
            id: id.into(),
            version: revision(&exports).expect("operation revisions serialize"),
            protocol_version: PROTOCOL_VERSION,
            exports,
        },
        operations,
        inspectors: Vec::new(),
    }
}
fn failure(
    code: &str,
    class: ErrorClass,
    certainty: EffectCertainty,
    message: &str,
) -> OperationError {
    OperationError {
        code: code.into(),
        class,
        certainty,
        message: message.into(),
    }
}
fn input_error() -> OperationError {
    failure(
        "data.invalid",
        ErrorClass::InvalidInput,
        EffectCertainty::NotApplied,
        "Input does not match the operation contract",
    )
}
fn budget_error() -> OperationError {
    failure(
        "resource.limit",
        ErrorClass::Resource,
        EffectCertainty::NotApplied,
        "Operation data budget exceeded",
    )
}
fn artifact_schema() -> Value {
    json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["id","scope","bytes","media_type"],"properties":{
        "id":{"type":"string","minLength":1,"maxLength":256},"scope":{"type":"string","minLength":1,"maxLength":256},
        "bytes":{"type":"integer","minimum":0},"media_type":{"type":"string","minLength":1,"maxLength":256}
    },"additionalProperties":false})
}
struct JsonBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for JsonBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("JSON byte budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn json_bytes(value: &Value, limit: usize) -> Result<Vec<u8>, OperationError> {
    let mut output = JsonBuffer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut output, value).map_err(|_| budget_error())?;
    Ok(output.bytes)
}
