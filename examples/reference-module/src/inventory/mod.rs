//! C-02 extension. CSV, inventory and reports remain outside the orchestration engine.
mod destination;
mod reports;
mod schemas;
mod source;

pub use destination::{InventoryDestination, InventorySnapshot, InventoryUpdate, MemoryInventory};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use workflow_forge_protocol::*;

const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
const MAX_ROWS: usize = 10_000;
const MAX_BATCH: usize = 100;
const MAX_REPORT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Summary {
    rows: usize,
    succeeded: usize,
    failed: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    source: ArtifactRef,
    cursor: usize,
    more: bool,
    summary: Summary,
    report: Option<ArtifactRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    index: usize,
    line: u64,
    sku: String,
    quantity: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    issue: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Page {
    cursor: usize,
    next_cursor: usize,
    total: usize,
    rows: Vec<Row>,
}

fn invalid(message: &str) -> OperationError {
    error(
        "reference.inventory.invalid",
        ErrorClass::InvalidInput,
        EffectCertainty::NotApplied,
        message,
    )
}
fn error(
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
fn artifact_error(certainty: EffectCertainty) -> OperationError {
    error(
        "reference.inventory.artifact",
        ErrorClass::Resource,
        certainty,
        "Artifact operation failed",
    )
}
fn decode<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, OperationError> {
    serde_json::from_value(value)
        .map_err(|_| invalid("Input does not match the inventory contract"))
}
fn output(value: &impl Serialize) -> Result<OperationOutput, OperationError> {
    serde_json::to_value(value)
        .map(OperationOutput::json)
        .map_err(|_| invalid("Result cannot be represented as JSON"))
}
async fn read_bytes(
    ctx: &OperationContext,
    reference: &ArtifactRef,
    limit: usize,
) -> Result<Vec<u8>, OperationError> {
    if reference.bytes > limit as u64 {
        return Err(invalid("Artifact exceeds the reader budget"));
    }
    let mut input = ctx
        .read_artifact(reference)
        .await
        .map_err(|_| artifact_error(EffectCertainty::NotApplied))?;
    let mut bytes = Vec::new();
    while let Some(chunk) = input.next().await {
        let chunk = chunk.map_err(|_| artifact_error(EffectCertainty::NotApplied))?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(invalid("Artifact exceeds the reader budget"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() as u64 != reference.bytes {
        return Err(invalid("Artifact length changed"));
    }
    Ok(bytes)
}
async fn write_json(
    ctx: &OperationContext,
    value: &impl Serialize,
    media_type: &str,
) -> Result<ArtifactRef, OperationError> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid("Report cannot be encoded"))?;
    if bytes.len() > MAX_REPORT_BYTES {
        return Err(invalid("Batch report exceeds its budget"));
    }
    ctx.write_artifact(Box::pin(stream::once(async { Ok(bytes) })), media_type)
        .await
        .map_err(|_| artifact_error(EffectCertainty::Unknown))
}

pub fn inventory_operations(destination: Arc<dyn InventoryDestination>) -> OperationBundle {
    let revision = OperationRevision::new("reference.inventory.apply_row", "1", "r1");
    let operations: Vec<Arc<dyn Operation>> = vec![
        Arc::new(source::ReadPage {
            descriptor: schemas::read_page(),
        }),
        Arc::new(destination::ApplyRow {
            descriptor: schemas::apply_row(),
            destination: destination.clone(),
        }),
        Arc::new(reports::ReportBatch {
            descriptor: schemas::report_batch(),
        }),
        Arc::new(reports::PublishReport {
            descriptor: schemas::publish_report(),
        }),
    ];
    OperationBundle {
        module: ModuleDescriptor {
            id: "reference.inventory".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: operations
                .iter()
                .map(|op| op.descriptor().revision.clone())
                .collect(),
        },
        operations,
        inspectors: vec![Arc::new(destination::Inspector {
            revision,
            destination,
        })],
    }
}
