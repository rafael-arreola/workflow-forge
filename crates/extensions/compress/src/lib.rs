//! Extensión `compress` de workflow-forge: gzip y zip sobre la convención
//! `$blob`. El caso típico de integración por archivos: los partners mandan
//! `.csv.gz` o `.zip` por SFTP y hay que abrirlos (o producirlos) en el flujo.
//!
//! | Tarea | Contrato |
//! |-------|----------|
//! | `compress.gzip` | `{ file: $blob, name? }` → `{ file: $blob }` (comprime un blob) |
//! | `compress.gunzip` | `{ file: $blob, name? }` → `{ file: $blob }` (descomprime un `.gz`) |
//! | `compress.zip` | `{ entries: [{ name, file: $blob }], name? }` → `{ file: $blob }` |
//! | `compress.unzip` | `{ file: $blob }` → `{ entries: [{ name, file: $blob }] }` |
//!
//! Todo se hace por streaming a través de archivos temporales (memoria
//! acotada aunque el archivo sea grande); la compresión usa deflate puro en
//! Rust (sin dependencias C, igual que el resto del workspace con rustls).

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

/// Registra todas las tareas de la extensión en el registry
pub fn register(registry: &TaskRegistry) {
    registry.register(GzipTask::default());
    registry.register(GunzipTask::default());
    registry.register(ZipTask::default());
    registry.register(UnzipTask::default());
}

/// Códigos de error que esta extensión puede emitir. Mismo contrato que
/// [`workflow_forge_core::error::codes`]: constantes estables, nunca cambian
/// de valor. Los errores de blobs reusan los códigos del core.
pub mod codes {
    /// El input de una tarea `compress.*` no deserializa contra su contrato.
    pub const COMPRESS_INPUT_INVALID: &str = "COMPRESS_INPUT_INVALID";
    /// Fallo de compresión/descompresión o de I/O del archivo temporal.
    pub const COMPRESS_ERROR: &str = "COMPRESS_ERROR";
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("schema estático válido")
}

fn compress_error(e: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::new(codes::COMPRESS_ERROR, e.to_string())
}

fn input_invalid(e: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::new(
        codes::COMPRESS_INPUT_INVALID,
        format!("input inválido: {e}"),
    )
}

/// Ruta temporal única para el resultado intermedio de una operación
fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("wf-{tag}-{}", uuid::Uuid::now_v7()))
}

/// Schema JSON reutilizable de un blob de entrada/salida
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
            Some("Comprime un blob con gzip y devuelve la referencia al `.gz`".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": {
                "file": blob_schema(),
                "name": { "type": "string", "description": "Nombre del blob resultante (default: nombre original + .gz)" }
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
            Some("Descomprime un blob gzip (`.gz`) y devuelve la referencia al original".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": {
                "file": blob_schema(),
                "name": { "type": "string", "description": "Nombre del blob resultante (default: nombre original sin .gz)" }
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
    /// Ruta/nombre de la entrada dentro del archivo
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
        manifest.description = Some(
            "Empaqueta varios blobs en un archivo zip (deflate) y devuelve su referencia".into(),
        );
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
                            "name": { "type": "string", "description": "Ruta de la entrada dentro del zip" },
                            "file": blob_schema()
                        }
                    }
                },
                "name": { "type": "string", "description": "Nombre del zip resultante (default: archive.zip)" }
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
        // Las rutas locales se resuelven aquí (síncrono y barato); el trabajo
        // pesado de comprimir va a spawn_blocking
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
            "Extrae las entradas de un archivo zip; cada una se registra como un \
             blob y se devuelve `{ name, file }`"
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

/// Extrae cada entrada (archivo, no directorio) a un temporal y devuelve los
/// pares `(nombre dentro del zip, ruta temporal)`.
fn unzip_to_temps(src: &Path) -> Result<Vec<(String, PathBuf)>, WorkflowError> {
    let file = File::open(src).map_err(compress_error)?;
    let mut archive = zip::ZipArchive::new(file).map_err(compress_error)?;
    let mut produced = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).map_err(compress_error)?;
        if entry.is_dir() {
            continue;
        }
        // `name()` puede traer rutas con `..`; se usa solo como etiqueta, no
        // como ruta de escritura (el destino es un temporal con id propio)
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

        // Importar cada temporal al BlobStore; los temporales se limpian todos
        // al final, haya o no error a mitad
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
// Soporte
// ---------------------------------------------------------------------------

/// Corre trabajo bloqueante (compresión/IO) fuera del runtime async,
/// aplanando el `JoinError` de tokio a un `WorkflowError`.
async fn run_blocking<T, F>(f: F) -> Result<T, WorkflowError>
where
    F: FnOnce() -> Result<T, WorkflowError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| compress_error(format!("la operación se interrumpió: {e}")))?
}
