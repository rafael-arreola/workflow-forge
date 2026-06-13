//! workflow-forge `tabular` extension: CSV and XLSX to/from JSON,
//! using the `$blob` convention for files.
//!
//! | Task | Contract |
//! |-------|----------|
//! | `tabular.parse` | `{ file: $blob, format?, headers?, delimiter?, sheet?, raw? }` → `{ rows, count }` |
//! | `tabular.write` | `{ rows, format, name?, headers?, delimiter?, sheet? }` → `{ file: $blob }` |

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::io::blob::BlobRef;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Registers all extension tasks in the registry
pub fn register(registry: &TaskRegistry) {
    registry.register(ParseTask::default());
    registry.register(WriteTask::default());
}

/// Error codes this extension can emit. Same contract as
/// [`workflow_forge_core::error::codes`]: stable constants, never change
/// value. Blob errors reuse the core codes.
pub mod codes {
    /// The input of a `tabular.*` task does not deserialize against its contract.
    pub const TABULAR_INPUT_INVALID: &str = "TABULAR_INPUT_INVALID";
    /// Could not infer the blob format (no explicit `format` and the
    /// name extension is not `.csv`/`.tsv`/`.xlsx`).
    pub const TABULAR_FORMAT_UNKNOWN: &str = "TABULAR_FORMAT_UNKNOWN";
    /// Failed to parse or write the tabular file (CSV or XLSX).
    pub const TABULAR_ERROR: &str = "TABULAR_ERROR";
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("valid static schema")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Format {
    Csv,
    Xlsx,
}

/// Infers the format from the blob name's extension
fn infer_format(explicit: Option<Format>, blob: &BlobRef) -> Result<Format, WorkflowError> {
    if let Some(format) = explicit {
        return Ok(format);
    }
    let name = blob.name.as_deref().unwrap_or_default().to_lowercase();
    if name.ends_with(".csv") || name.ends_with(".tsv") {
        Ok(Format::Csv)
    } else if name.ends_with(".xlsx") {
        Ok(Format::Xlsx)
    } else {
        Err(WorkflowError::new(
            codes::TABULAR_FORMAT_UNKNOWN,
            format!(
                "Could not infer the format of blob '{}'; specify `format`",
                blob.name.as_deref().unwrap_or(&blob.id)
            ),
        ))
    }
}

fn tabular_error(message: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::new(codes::TABULAR_ERROR, message.to_string())
}

// ---------------------------------------------------------------------------
// tabular.parse
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ParseInput {
    file: BlobRef,
    #[serde(default)]
    format: Option<Format>,
    /// First row is headers → rows as objects
    #[serde(default = "default_true")]
    headers: bool,
    #[serde(default = "default_delimiter")]
    delimiter: String,
    /// Sheet to read (xlsx only; default: the first one)
    #[serde(default)]
    sheet: Option<String>,
    /// Disables type inference in CSV (everything stays as string)
    #[serde(default)]
    raw: bool,
}

fn default_true() -> bool {
    true
}

fn default_delimiter() -> String {
    ",".to_string()
}

pub struct ParseTask {
    manifest: TaskManifest,
}

impl Default for ParseTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("tabular.parse");
        manifest.description = Some(
            "Parses a CSV/XLSX blob into JSON rows. With headers, each row is an \
             object {column: value}; without headers, an array of cells"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": {
                "file": { "type": "object", "required": ["$blob"] },
                "format": { "enum": ["csv", "xlsx"] },
                "headers": { "type": "boolean", "default": true },
                "delimiter": { "type": "string", "minLength": 1, "maxLength": 1, "default": "," },
                "sheet": { "type": "string" },
                "raw": { "type": "boolean", "default": false }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["rows", "count"],
            "properties": {
                "rows": { "type": "array" },
                "count": { "type": "integer" }
            }
        })));
        Self { manifest }
    }
}

/// Type inference for CSV cells: null, bool, integer, float, or string
fn parse_scalar(s: &str) -> Value {
    if s.is_empty() {
        return Value::Null;
    }
    match s {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    // "007" must remain a string: only numbers without leading zeros
    let leading_zero = (s.len() > 1 && s.starts_with('0') && !s.starts_with("0."))
        || (s.len() > 2 && s.starts_with("-0") && !s.starts_with("-0."));
    if !leading_zero {
        if let Ok(i) = s.parse::<i64>() {
            return json!(i);
        }
        if s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit() || c == '-')
            && let Ok(f) = s.parse::<f64>()
        {
            return json!(f);
        }
    }
    Value::String(s.to_string())
}

fn rows_to_value(rows: Vec<Vec<Value>>, headers: bool) -> Result<Value, WorkflowError> {
    if !headers {
        return Ok(Value::Array(rows.into_iter().map(Value::Array).collect()));
    }
    let mut iter = rows.into_iter();
    let Some(header_row) = iter.next() else {
        return Ok(Value::Array(vec![]));
    };
    let names: Vec<String> = header_row
        .into_iter()
        .enumerate()
        .map(|(i, cell)| match cell {
            Value::String(s) if !s.is_empty() => s,
            Value::Null => format!("col_{i}"),
            other => other.to_string(),
        })
        .collect();

    let objects: Vec<Value> = iter
        .map(|row| {
            let map: serde_json::Map<String, Value> = names
                .iter()
                .cloned()
                .zip(row.into_iter().chain(std::iter::repeat(Value::Null)))
                .collect();
            Value::Object(map)
        })
        .collect();
    Ok(Value::Array(objects))
}

fn parse_csv(path: &std::path::Path, input: &ParseInput) -> Result<Value, WorkflowError> {
    let delimiter = input.delimiter.as_bytes()[0];
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .from_path(path)
        .map_err(tabular_error)?;

    let mut rows: Vec<Vec<Value>> = Vec::new();
    for record in reader.records() {
        let record = record.map_err(tabular_error)?;
        rows.push(
            record
                .iter()
                .map(|cell| {
                    if input.raw {
                        Value::String(cell.to_string())
                    } else {
                        parse_scalar(cell)
                    }
                })
                .collect(),
        );
    }
    rows_to_value(rows, input.headers)
}

fn parse_xlsx(path: &std::path::Path, input: &ParseInput) -> Result<Value, WorkflowError> {
    use calamine::{Data, Reader};

    let mut workbook: calamine::Xlsx<_> = calamine::open_workbook(path).map_err(tabular_error)?;
    let sheet_name = match &input.sheet {
        Some(name) => name.clone(),
        None => workbook
            .sheet_names()
            .first()
            .cloned()
            .ok_or_else(|| tabular_error("the xlsx file has no sheets"))?,
    };
    let range = workbook
        .worksheet_range(&sheet_name)
        .map_err(|e| tabular_error(format!("sheet '{sheet_name}': {e}")))?;

    let rows: Vec<Vec<Value>> = range
        .rows()
        .map(|row| {
            row.iter()
                .map(|cell| match cell {
                    Data::Empty => Value::Null,
                    Data::Int(i) => json!(i),
                    // xlsx stores integers as floats; normalize them back
                    Data::Float(f) if f.fract() == 0.0 && f.abs() < (i64::MAX as f64) => {
                        json!(*f as i64)
                    }
                    Data::Float(f) => json!(f),
                    Data::Bool(b) => Value::Bool(*b),
                    // Excel dates: serial number (days since 1900)
                    Data::DateTime(dt) => json!(dt.as_f64()),
                    Data::String(s) => Value::String(s.clone()),
                    Data::DateTimeIso(s) | Data::DurationIso(s) => Value::String(s.clone()),
                    Data::Error(e) => Value::String(format!("#ERROR:{e:?}")),
                })
                .collect()
        })
        .collect();
    rows_to_value(rows, input.headers)
}

#[async_trait]
impl Task for ParseTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: ParseInput = serde_json::from_value(input.0)
            .map_err(|e| WorkflowError::new(codes::TABULAR_INPUT_INVALID, e.to_string()))?;
        let format = infer_format(parsed.format, &parsed.file)?;
        let path = ctx.blobs().local_path(&parsed.file)?;

        let rows = match format {
            Format::Csv => parse_csv(&path, &parsed)?,
            Format::Xlsx => parse_xlsx(&path, &parsed)?,
        };
        let count = rows.as_array().map(Vec::len).unwrap_or(0);
        Ok(WorkflowData(json!({ "rows": rows, "count": count })))
    }
}

// ---------------------------------------------------------------------------
// tabular.write
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct WriteInput {
    rows: Vec<Value>,
    format: Format,
    /// Name of the resulting file (default based on format)
    #[serde(default)]
    name: Option<String>,
    /// Explicit column order; default: sorted keys of the first row
    #[serde(default)]
    headers: Option<Vec<String>>,
    #[serde(default = "default_delimiter")]
    delimiter: String,
    #[serde(default)]
    sheet: Option<String>,
}

pub struct WriteTask {
    manifest: TaskManifest,
}

impl Default for WriteTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("tabular.write");
        manifest.description = Some(
            "Writes JSON rows (objects or arrays) as a CSV/XLSX blob and \
             returns its reference"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["rows", "format"],
            "properties": {
                "rows": { "type": "array", "items": { "type": ["object", "array"] } },
                "format": { "enum": ["csv", "xlsx"] },
                "name": { "type": "string" },
                "headers": { "type": "array", "items": { "type": "string" } },
                "delimiter": { "type": "string", "minLength": 1, "maxLength": 1, "default": "," },
                "sheet": { "type": "string" }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": { "file": { "type": "object", "required": ["$blob"] } }
        })));
        Self { manifest }
    }
}

/// Columns to write: explicit or the (sorted) keys of the first row
fn resolve_columns(input: &WriteInput) -> Option<Vec<String>> {
    if let Some(headers) = &input.headers {
        return Some(headers.clone());
    }
    match input.rows.first() {
        Some(Value::Object(map)) => Some(map.keys().cloned().collect()),
        _ => None, // rows as arrays: no headers
    }
}

fn cell_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn row_cells(row: &Value, columns: &Option<Vec<String>>) -> Vec<Value> {
    match (row, columns) {
        (Value::Object(map), Some(cols)) => cols
            .iter()
            .map(|c| map.get(c).cloned().unwrap_or(Value::Null))
            .collect(),
        (Value::Array(cells), _) => cells.clone(),
        (other, _) => vec![other.clone()],
    }
}

fn write_csv(input: &WriteInput, columns: &Option<Vec<String>>) -> Result<Vec<u8>, WorkflowError> {
    let mut writer = csv::WriterBuilder::new()
        .delimiter(input.delimiter.as_bytes()[0])
        .from_writer(Vec::new());

    if let Some(cols) = columns {
        writer.write_record(cols).map_err(tabular_error)?;
    }
    for row in &input.rows {
        let cells: Vec<String> = row_cells(row, columns).iter().map(cell_string).collect();
        writer.write_record(&cells).map_err(tabular_error)?;
    }
    writer.into_inner().map_err(tabular_error)
}

fn write_xlsx(input: &WriteInput, columns: &Option<Vec<String>>) -> Result<Vec<u8>, WorkflowError> {
    let mut workbook = rust_xlsxwriter::Workbook::new();
    let worksheet = workbook.add_worksheet();
    if let Some(sheet) = &input.sheet {
        worksheet.set_name(sheet).map_err(tabular_error)?;
    }

    let mut row_idx: u32 = 0;
    if let Some(cols) = columns {
        for (col_idx, name) in cols.iter().enumerate() {
            worksheet
                .write(row_idx, col_idx as u16, name)
                .map_err(tabular_error)?;
        }
        row_idx += 1;
    }
    for row in &input.rows {
        for (col_idx, cell) in row_cells(row, columns).iter().enumerate() {
            let col_idx = col_idx as u16;
            match cell {
                Value::Null => {}
                Value::Bool(b) => {
                    worksheet
                        .write(row_idx, col_idx, *b)
                        .map_err(tabular_error)?;
                }
                Value::Number(n) => {
                    worksheet
                        .write(row_idx, col_idx, n.as_f64().unwrap_or_default())
                        .map_err(tabular_error)?;
                }
                Value::String(s) => {
                    worksheet
                        .write(row_idx, col_idx, s.as_str())
                        .map_err(tabular_error)?;
                }
                other => {
                    worksheet
                        .write(row_idx, col_idx, other.to_string())
                        .map_err(tabular_error)?;
                }
            }
        }
        row_idx += 1;
    }
    workbook.save_to_buffer().map_err(tabular_error)
}

#[async_trait]
impl Task for WriteTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: WriteInput = serde_json::from_value(input.0)
            .map_err(|e| WorkflowError::new(codes::TABULAR_INPUT_INVALID, e.to_string()))?;
        let columns = resolve_columns(&parsed);

        let (bytes, default_name) = match parsed.format {
            Format::Csv => (write_csv(&parsed, &columns)?, "out.csv"),
            Format::Xlsx => (write_xlsx(&parsed, &columns)?, "out.xlsx"),
        };
        let name = parsed.name.clone().unwrap_or_else(|| default_name.into());
        let blob = ctx.blobs().put(bytes, Some(name)).await?;
        Ok(WorkflowData(json!({ "file": blob })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_type_inference() {
        assert_eq!(parse_scalar(""), Value::Null);
        assert_eq!(parse_scalar("true"), Value::Bool(true));
        assert_eq!(parse_scalar("42"), json!(42));
        assert_eq!(parse_scalar("3.5"), json!(3.5));
        assert_eq!(parse_scalar("hello"), json!("hello"));
        assert_eq!(parse_scalar("007"), json!("007")); // preserved as string
        assert_eq!(parse_scalar("1e3"), json!(1000.0));
    }

    #[test]
    fn rows_with_headers_to_objects() {
        let rows = vec![
            vec![json!("a"), json!("b")],
            vec![json!(1), json!(2)],
            vec![json!(3)], // short row → null
        ];
        let value = rows_to_value(rows, true).unwrap();
        assert_eq!(value, json!([ { "a": 1, "b": 2 }, { "a": 3, "b": null } ]));
    }
}
