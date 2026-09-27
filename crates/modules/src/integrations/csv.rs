use super::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CsvOptions {
    pub max_bytes: usize,
    pub max_rows: usize,
    pub max_columns: usize,
    pub max_field_bytes: usize,
    pub max_batch: usize,
}
impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            max_bytes: 4 * 1024 * 1024,
            max_rows: 10_000,
            max_columns: 128,
            max_field_bytes: 16 * 1024,
            max_batch: 1000,
        }
    }
}
struct Csv {
    options: CsvOptions,
    descriptor: OperationDescriptor,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    source: ArtifactRef,
    #[serde(default)]
    offset: usize,
    size: Option<usize>,
}
#[derive(Serialize)]
struct Row {
    index: usize,
    line: u64,
    fields: Vec<String>,
    valid_columns: bool,
}

pub fn csv_operations(options: CsvOptions) -> Result<OperationBundle, ForgeError> {
    if !(1..=64 * 1024 * 1024).contains(&options.max_bytes)
        || !(1..=1_000_000).contains(&options.max_rows)
        || !(1..=1024).contains(&options.max_columns)
        || !(1..=1024 * 1024).contains(&options.max_field_bytes)
        || !(1..=1000).contains(&options.max_batch)
    {
        return Err(configured("CSV options violate their data policy"));
    }
    let descriptor = OperationDescriptor {
        revision: OperationRevision::new("forge.csv.read_batch", "1", &revision(&options)?),
        schema_dialect: SCHEMA_DIALECT.into(),
        config_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","additionalProperties":false}),
        input_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["source"],"properties":{"source":artifact_schema(),"offset":{"type":"integer","minimum":0,"maximum":options.max_rows},"size":{"type":"integer","minimum":1,"maximum":options.max_batch}},"additionalProperties":false}),
        output_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["headers","rows","next_offset"],"properties":{
            "headers":{"type":"array","items":{"type":"string"}},"next_offset":{"type":["integer","null"],"minimum":0},
            "rows":{"type":"array","items":{"type":"object","required":["index","line","fields","valid_columns"],"properties":{
                "index":{"type":"integer","minimum":0},"line":{"type":"integer","minimum":1},"fields":{"type":"array","items":{"type":"string"}},"valid_columns":{"type":"boolean"}
            },"additionalProperties":false}}
        },"additionalProperties":false}),
        effect: EffectKind::Read,
        repetition: Repetition::Safe,
        reconciliation: false,
        required_resources: BTreeSet::from(["artifacts".into()]),
        description: "Read a bounded CSV artifact in logical record batches".into(),
        examples: Vec::new(),
    };
    Ok(bundle(
        "forge.csv",
        vec![Arc::new(Csv {
            options,
            descriptor,
        })],
    ))
}
impl Operation for Csv {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> OperationFuture<'a> {
        Box::pin(async move {
            let input: Input =
                serde_json::from_value(invocation.input).map_err(|_| input_error())?;
            let size = input.size.unwrap_or(100.min(self.options.max_batch));
            if size == 0 || size > self.options.max_batch || input.offset > self.options.max_rows {
                return Err(input_error());
            }
            if input.source.bytes > self.options.max_bytes as u64 {
                return Err(budget_error());
            }
            let mut stream = context.read_artifact(&input.source).await.map_err(|_| {
                failure(
                    "resource.unavailable",
                    ErrorClass::Resource,
                    EffectCertainty::NotApplied,
                    "CSV artifact is unavailable",
                )
            })?;
            let mut bytes = Vec::new();
            loop {
                let chunk = tokio::select! {
                    biased;
                    _=context.cancellation.cancelled()=>return Err(failure("operation.cancelled",ErrorClass::Cancelled,EffectCertainty::NotApplied,"CSV read cancelled")),
                    next=stream.next()=>next,
                };
                let Some(chunk) = chunk else {
                    break;
                };
                let chunk = chunk.map_err(|_| {
                    failure(
                        "resource.unavailable",
                        ErrorClass::Resource,
                        EffectCertainty::NotApplied,
                        "CSV artifact stream failed",
                    )
                })?;
                if chunk.len() > self.options.max_bytes.saturating_sub(bytes.len()) {
                    return Err(budget_error());
                }
                bytes.extend_from_slice(&chunk);
            }
            if bytes.len() as u64 != input.source.bytes {
                return Err(input_error());
            }
            let options = self.options.clone();
            // Parsing is bounded but CPU work; do not occupy an async worker.
            let output =
                tokio::task::spawn_blocking(move || parse(&bytes, input.offset, size, &options))
                    .await
                    .map_err(|_| {
                        failure(
                            "csv.failed",
                            ErrorClass::Internal,
                            EffectCertainty::NotApplied,
                            "CSV worker failed",
                        )
                    })??;
            Ok(OperationOutput::json(output))
        })
    }
}
fn parse(
    bytes: &[u8],
    offset: usize,
    size: usize,
    options: &CsvOptions,
) -> Result<Value, OperationError> {
    let mut reader = ::csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(bytes);
    let header = reader.headers().map_err(|_| input_error())?.clone();
    if header.is_empty()
        || header.len() > options.max_columns
        || header
            .iter()
            .any(|value| value.is_empty() || value.len() > options.max_field_bytes)
        || header.iter().collect::<BTreeSet<_>>().len() != header.len()
    {
        return Err(input_error());
    }
    let mut rows = Vec::new();
    let mut total = 0;
    for (index, record) in reader.records().enumerate() {
        if index >= options.max_rows {
            return Err(budget_error());
        }
        let record = record.map_err(|_| input_error())?;
        if record.len() > options.max_columns
            || record
                .iter()
                .any(|value| value.len() > options.max_field_bytes)
        {
            return Err(budget_error());
        }
        if index >= offset && rows.len() < size {
            rows.push(Row {
                index,
                line: record.position().map_or(1, ::csv::Position::line),
                valid_columns: record.len() == header.len(),
                fields: record.iter().map(str::to_owned).collect(),
            });
        }
        total = index + 1;
    }
    if offset > total {
        return Err(input_error());
    }
    let end = offset + rows.len();
    Ok(
        json!({"headers":header.iter().collect::<Vec<_>>(),"rows":rows,"next_offset":(end<total).then_some(end)}),
    )
}
