//! workflow-forge `compress` extension: gzip and zip over the `$blob`
//! convention. The typical file-based integration case: partners send
//! `.csv.gz` or `.zip` via SFTP and they need to be opened (or produced) in the
//! flow.
//!
//! | Task | Contract |
//! |-------|----------|
//! | `compress.gzip` | `{ file: $blob, name? }` → `{ file: $blob }` (compresses a blob) |
//! | `compress.gunzip` | `{ file: $blob, name? }` → `{ file: $blob }` (decompresses a `.gz`) |
//! | `compress.zip` | `{ entries: [{ name, file: $blob }], name? }` → `{ file: $blob }` |
//! | `compress.unzip` | `{ file: $blob }` → `{ entries: [{ name, file: $blob }] }` |
//!
//! Everything is done via streaming through temporary files (bounded
//! memory even for large files); compression uses pure deflate in
//! Rust (no C dependencies, consistent with the rest of the workspace using rustls).

use std::fs::File;
use std::path::{Path, PathBuf};

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
    registry.register(GzipTask::default());
    registry.register(GunzipTask::default());
    registry.register(ZipTask::default());
    registry.register(UnzipTask::default());
}

/// Error codes this extension can emit. Same contract as
/// [`workflow_forge_core::error::codes`]: stable constants, never change
/// value. Blob errors reuse the core codes.
pub mod codes {
    /// The input of a `compress.*` task does not deserialize against its contract.
    pub const COMPRESS_INPUT_INVALID: &str = "COMPRESS_INPUT_INVALID";
    /// Compression/decompression failure or temporary file I/O error.
    pub const COMPRESS_ERROR: &str = "COMPRESS_ERROR";
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("valid static schema")
}

fn compress_error(e: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::new(codes::COMPRESS_ERROR, e.to_string())
}

fn input_invalid(e: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::new(codes::COMPRESS_INPUT_INVALID, format!("invalid input: {e}"))
}

/// Unique temporary path for the intermediate result of an operation
fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("wf-{tag}-{}", uuid::Uuid::now_v7()))
}

/// Reusable JSON schema for an input/output blob
fn blob_schema() -> Value {
    json!({ "type": "object", "required": ["$blob"] })
}

// ---------------------------------------------------------------------------
// compress.gzip
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct SingleBlobInput {
    file: BlobRef,
    #[serde(default)]
    name: Option<String>,
}

pub struct GzipTask {
    manifest: TaskManifest,
}

impl Default for GzipTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("compress.gzip");
        manifest.description =
            Some("Compresses a blob with gzip and returns the `.gz` reference".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": {
                "file": blob_schema(),
                "name": { "type": "string", "description": "Name of the resulting blob (default: original name + .gz)" }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": { "file": blob_schema() }
        })));
        Self { manifest }
    }
}

fn gzip_file(src: &Path, dest: &Path) -> Result<(), WorkflowError> {
    use flate2::{Compression, write::GzEncoder};
    let mut input = File::open(src).map_err(compress_error)?;
    let output = File::create(dest).map_err(compress_error)?;
    let mut encoder = GzEncoder::new(output, Compression::default());
    std::io::copy(&mut input, &mut encoder).map_err(compress_error)?;
    encoder.finish().map_err(compress_error)?;
    Ok(())
}

#[async_trait]
impl Task for GzipTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: SingleBlobInput = serde_json::from_value(input.0).map_err(input_invalid)?;
        let src = ctx.blobs().local_path(&parsed.file)?;
        let temp = temp_path("gzip");

        let temp_blocking = temp.clone();
        run_blocking(move || gzip_file(&src, &temp_blocking)).await?;

        let name = parsed
            .name
            .or_else(|| parsed.file.name.as_ref().map(|n| format!("{n}.gz")))
            .unwrap_or_else(|| format!("{}.gz", parsed.file.id));
        let blob = ctx.blobs().import_file(&temp, Some(name)).await;
        let _ = tokio::fs::remove_file(&temp).await;
        Ok(WorkflowData(json!({ "file": blob? })))
    }
}

// ---------------------------------------------------------------------------
// compress.gunzip
// ---------------------------------------------------------------------------

pub struct GunzipTask {
    manifest: TaskManifest,
}

impl Default for GunzipTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("compress.gunzip");
        manifest.description =
            Some("Decompresses a gzip blob (`.gz`) and returns the original reference".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": {
                "file": blob_schema(),
                "name": { "type": "string", "description": "Name of the resulting blob (default: original name without .gz)" }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": { "file": blob_schema() }
        })));
        Self { manifest }
    }
}

fn gunzip_file(src: &Path, dest: &Path) -> Result<(), WorkflowError> {
    use flate2::read::GzDecoder;
    let input = File::open(src).map_err(compress_error)?;
    let mut decoder = GzDecoder::new(input);
    let mut output = File::create(dest).map_err(compress_error)?;
    std::io::copy(&mut decoder, &mut output).map_err(compress_error)?;
    Ok(())
}

#[async_trait]
impl Task for GunzipTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: SingleBlobInput = serde_json::from_value(input.0).map_err(input_invalid)?;
        let src = ctx.blobs().local_path(&parsed.file)?;
        let temp = temp_path("gunzip");

        let temp_blocking = temp.clone();
        run_blocking(move || gunzip_file(&src, &temp_blocking)).await?;

        let name = parsed
            .name
            .or_else(|| {
                parsed
                    .file
                    .name
                    .as_ref()
                    .map(|n| n.strip_suffix(".gz").unwrap_or(n).to_string())
            })
            .unwrap_or_else(|| parsed.file.id.clone());
        let blob = ctx.blobs().import_file(&temp, Some(name)).await;
        let _ = tokio::fs::remove_file(&temp).await;
        Ok(WorkflowData(json!({ "file": blob? })))
    }
}

// ---------------------------------------------------------------------------
// compress.zip
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ZipEntry {
    /// Path/name of the entry within the archive
    name: String,
    file: BlobRef,
}

#[derive(Deserialize)]
struct ZipInput {
    entries: Vec<ZipEntry>,
    #[serde(default)]
    name: Option<String>,
}

pub struct ZipTask {
    manifest: TaskManifest,
}

impl Default for ZipTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("compress.zip");
        manifest.description =
            Some("Packs multiple blobs into a zip file (deflate) and returns its reference".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["entries"],
            "properties": {
                "entries": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["name", "file"],
                        "properties": {
                            "name": { "type": "string", "description": "Entry path within the zip" },
                            "file": blob_schema()
                        }
                    }
                },
                "name": { "type": "string", "description": "Name of the resulting zip (default: archive.zip)" }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": { "file": blob_schema() }
        })));
        Self { manifest }
    }
}

fn zip_files(entries: Vec<(String, PathBuf)>, dest: &Path) -> Result<(), WorkflowError> {
    use zip::write::SimpleFileOptions;
    let output = File::create(dest).map_err(compress_error)?;
    let mut writer = zip::ZipWriter::new(output);
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    for (name, path) in entries {
        writer.start_file(name, options).map_err(compress_error)?;
        let mut file = File::open(&path).map_err(compress_error)?;
        std::io::copy(&mut file, &mut writer).map_err(compress_error)?;
    }
    writer.finish().map_err(compress_error)?;
    Ok(())
}

#[async_trait]
impl Task for ZipTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: ZipInput = serde_json::from_value(input.0).map_err(input_invalid)?;
        // Local paths are resolved here (synchronous and cheap); the heavy
        // compression work goes to spawn_blocking
        let entries: Vec<(String, PathBuf)> = parsed
            .entries
            .iter()
            .map(|e| ctx.blobs().local_path(&e.file).map(|p| (e.name.clone(), p)))
            .collect::<Result<_, _>>()?;
        let temp = temp_path("zip");

        let temp_blocking = temp.clone();
        run_blocking(move || zip_files(entries, &temp_blocking)).await?;

        let name = parsed.name.unwrap_or_else(|| "archive.zip".to_string());
        let blob = ctx.blobs().import_file(&temp, Some(name)).await;
        let _ = tokio::fs::remove_file(&temp).await;
        Ok(WorkflowData(json!({ "file": blob? })))
    }
}

// ---------------------------------------------------------------------------
// compress.unzip
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct UnzipInput {
    file: BlobRef,
}

pub struct UnzipTask {
    manifest: TaskManifest,
}

impl Default for UnzipTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("compress.unzip");
        manifest.description = Some(
            "Extracts entries from a zip file; each is registered as a \
             blob and returned as `{ name, file }`"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": { "file": blob_schema() }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["entries"],
            "properties": {
                "entries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["name", "file"],
                        "properties": {
                            "name": { "type": "string" },
                            "file": blob_schema()
                        }
                    }
                }
            }
        })));
        Self { manifest }
    }
}

/// Extracts each entry (file, not directory) to a temp file and returns the
/// `(name within the zip, temp path)` pairs.
fn unzip_to_temps(src: &Path) -> Result<Vec<(String, PathBuf)>, WorkflowError> {
    let file = File::open(src).map_err(compress_error)?;
    let mut archive = zip::ZipArchive::new(file).map_err(compress_error)?;
    let mut produced = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).map_err(compress_error)?;
        if entry.is_dir() {
            continue;
        }
        // `name()` may contain paths with `..`; it is used only as a label, not
        // as a write path (the destination is a temp file with its own id)
        let name = entry.name().to_string();
        let temp = temp_path("unzip");
        let mut out = File::create(&temp).map_err(compress_error)?;
        std::io::copy(&mut entry, &mut out).map_err(compress_error)?;
        produced.push((name, temp));
    }
    Ok(produced)
}

#[async_trait]
impl Task for UnzipTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: UnzipInput = serde_json::from_value(input.0).map_err(input_invalid)?;
        let src = ctx.blobs().local_path(&parsed.file)?;

        let produced = run_blocking(move || unzip_to_temps(&src)).await?;

        // Import each temp file into the BlobStore; all temps are cleaned up
        // at the end, whether or not there was an error partway through
        let mut entries = Vec::with_capacity(produced.len());
        let mut outcome = Ok(());
        for (name, temp) in &produced {
            match ctx.blobs().import_file(temp, Some(name.clone())).await {
                Ok(blob) => entries.push(json!({
                    "name": name,
                    "file": serde_json::to_value(&blob).unwrap_or(Value::Null),
                })),
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
        for (_, temp) in &produced {
            let _ = tokio::fs::remove_file(temp).await;
        }
        outcome?;
        Ok(WorkflowData(json!({ "entries": entries })))
    }
}

// ---------------------------------------------------------------------------
// Support
// ---------------------------------------------------------------------------

/// Runs blocking work (compression/IO) off the async runtime,
/// flattening tokio's `JoinError` into a `WorkflowError`.
async fn run_blocking<T, F>(f: F) -> Result<T, WorkflowError>
where
    F: FnOnce() -> Result<T, WorkflowError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| compress_error(format!("operation was interrupted: {e}")))?
}
