//! Extensión `http` de workflow-forge: cliente HTTP/S declarativo.
//!
//! `http.request` — input (los campos de cuerpo son mutuamente excluyentes):
//! ```json
//! {
//!   "url": "https://api.example.com/users",
//!   "method": "POST",
//!   "headers": { "x-api-key": "..." },
//!   "query": { "page": "1" },
//!   "body": { "name": "ada" },
//!   "form": { "grant_type": "client_credentials" },
//!   "text": "<xml>...</xml>",
//!   "body_blob": { "$blob": "01J…" },
//!   "multipart": {
//!     "metadata": { "json": { "orden": 123 } },
//!     "nota": "texto plano",
//!     "archivo": { "blob": { "$blob": "01J…" }, "filename": "ventas.csv", "content_type": "text/csv" }
//!   },
//!   "auth": { "type": "bearer", "token": "..." },
//!   "response_body": "auto",
//!   "fail_on_error_status": false
//! }
//! ```
//! Output: `{ "status": 200, "headers": {...}, "body": <json|string|{"$blob"...}|null> }`.
//!
//! Los blobs (subida y descarga) se transfieren por streaming, nunca se
//! cargan completos al contexto JSON. Con `response_body: "blob"` el cuerpo
//! de la respuesta se guarda en el BlobStore de la ejecución.
//!
//! Un status 4xx/5xx no es error por default: el status es dato y se rutea
//! con gateways. Con `fail_on_error_status: true` la tarea falla y aplican
//! `retry`/`on_error` del nodo.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::io::blob::BlobRef;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Registra todas las tareas de la extensión en el registry.
///
/// La tarea por defecto reutiliza un único `reqwest::Client`, que ya hace
/// **pooling de conexiones** (keep-alive por host). Para afinar el pool
/// (tamaño, timeouts, proxy, TLS) a volumen, usa [`register_with_client`].
pub fn register(registry: &TaskRegistry) {
    registry.register(HttpRequestTask::default());
}

/// Como [`register`], pero con un `reqwest::Client` provisto por el host: la
/// vía recomendada para producción a volumen, donde quieres controlar el pool
/// (`pool_max_idle_per_host`, `timeout`, `connect_timeout`, proxy…).
pub fn register_with_client(registry: &TaskRegistry, client: reqwest::Client) {
    registry.register(HttpRequestTask::with_client(client));
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("schema estático válido")
}

#[derive(Deserialize)]
struct RequestInput {
    url: String,
    #[serde(default = "default_method")]
    method: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    query: HashMap<String, String>,
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    form: Option<BTreeMap<String, String>>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    body_blob: Option<BlobRef>,
    #[serde(default)]
    multipart: Option<BTreeMap<String, MultipartPart>>,
    #[serde(default)]
    auth: Option<Auth>,
    #[serde(default)]
    response_body: ResponseBodyMode,
    #[serde(default)]
    fail_on_error_status: bool,
}

fn default_method() -> String {
    "GET".to_string()
}

#[derive(Deserialize)]
#[serde(untagged)]
enum MultipartPart {
    Plain(String),
    Text {
        text: String,
        #[serde(default)]
        content_type: Option<String>,
    },
    Json {
        json: Value,
    },
    Blob {
        blob: BlobRef,
        #[serde(default)]
        filename: Option<String>,
        #[serde(default)]
        content_type: Option<String>,
    },
}

#[derive(Deserialize, Default, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum ResponseBodyMode {
    /// JSON si el content-type es json; string en otro caso (comportamiento
    /// histórico)
    #[default]
    Auto,
    /// Fuerza string aunque el content-type sea json
    Text,
    /// Guarda el cuerpo en el BlobStore y devuelve la referencia `$blob`
    Blob,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Auth {
    Basic {
        username: String,
        #[serde(default)]
        password: Option<String>,
    },
    Bearer {
        token: String,
    },
}

pub struct HttpRequestTask {
    manifest: TaskManifest,
    client: reqwest::Client,
}

impl Default for HttpRequestTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("http.request");
        manifest.description = Some(
            "Ejecuta una petición HTTP/S y devuelve {status, headers, body}. \
             Cuerpos: body (JSON), form (urlencoded), text (raw), body_blob \
             (binario streamed) o multipart (form-data con blobs) — \
             mutuamente excluyentes. Con response_body: blob el cuerpo de la \
             respuesta se guarda en el BlobStore. Con fail_on_error_status, \
             un status >= 400 falla la tarea"
                .into(),
        );
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": { "type": "string", "format": "uri" },
                "method": {
                    "enum": ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
                    "default": "GET"
                },
                "headers": { "type": "object", "additionalProperties": { "type": "string" } },
                "query": { "type": "object", "additionalProperties": { "type": "string" } },
                "body": { "description": "Cuerpo JSON de la petición" },
                "form": {
                    "description": "Cuerpo application/x-www-form-urlencoded",
                    "type": "object",
                    "additionalProperties": { "type": "string" }
                },
                "text": {
                    "description": "Cuerpo raw; content-type vía headers (default text/plain)",
                    "type": "string"
                },
                "body_blob": {
                    "description": "Cuerpo binario streamed desde un blob; content-type vía headers (default application/octet-stream)",
                    "type": "object",
                    "required": ["$blob"]
                },
                "multipart": {
                    "description": "Cuerpo multipart/form-data: cada propiedad es una parte",
                    "type": "object",
                    "additionalProperties": {
                        "oneOf": [
                            { "type": "string", "description": "Parte de texto plano" },
                            {
                                "type": "object",
                                "required": ["text"],
                                "properties": {
                                    "text": { "type": "string" },
                                    "content_type": { "type": "string" }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["json"],
                                "properties": { "json": {} },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["blob"],
                                "properties": {
                                    "blob": { "type": "object", "required": ["$blob"] },
                                    "filename": { "type": "string" },
                                    "content_type": { "type": "string" }
                                },
                                "additionalProperties": false
                            }
                        ]
                    }
                },
                "auth": {
                    "type": "object",
                    "required": ["type"],
                    "properties": {
                        "type": { "enum": ["basic", "bearer"] },
                        "username": { "type": "string" },
                        "password": { "type": "string" },
                        "token": { "type": "string" }
                    }
                },
                "response_body": {
                    "description": "Cómo interpretar el cuerpo de la respuesta",
                    "enum": ["auto", "text", "blob"],
                    "default": "auto"
                },
                "fail_on_error_status": { "type": "boolean", "default": false }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["status", "headers"],
            "properties": {
                "status": { "type": "integer" },
                "headers": { "type": "object" },
                "body": {
                    "description": "JSON, string, null o {\"$blob\"} con response_body: blob"
                }
            }
        })));
        Self {
            manifest,
            client: reqwest::Client::new(),
        }
    }
}

impl HttpRequestTask {
    /// Construye la tarea reutilizando un `reqwest::Client` provisto por el
    /// host. Un único client comparte y reutiliza conexiones (pool keep-alive)
    /// entre todas las peticiones; inyéctalo afinado para uso a volumen.
    pub fn with_client(client: reqwest::Client) -> Self {
        Self {
            client,
            ..Self::default()
        }
    }
}

fn input_error(message: impl Into<String>) -> WorkflowError {
    WorkflowError::new("HTTP_INPUT_INVALID", message)
}

fn has_header(headers: &HashMap<String, String>, name: &str) -> bool {
    headers.keys().any(|k| k.eq_ignore_ascii_case(name))
}

/// Body streamed desde un blob file-backed, con su longitud
async fn blob_stream(
    ctx: &WorkflowContext,
    blob: &BlobRef,
) -> Result<(reqwest::Body, u64), WorkflowError> {
    let path = ctx.blobs().local_path(blob)?;
    let file = tokio::fs::File::open(&path).await.map_err(|e| {
        WorkflowError::new(
            "BLOB_IO_ERROR",
            format!("No se pudo abrir el blob '{}': {e}", blob.id),
        )
    })?;
    let len = file
        .metadata()
        .await
        .map_err(|e| {
            WorkflowError::new(
                "BLOB_IO_ERROR",
                format!("No se pudo leer el tamaño del blob '{}': {e}", blob.id),
            )
        })?
        .len();
    Ok((reqwest::Body::wrap_stream(ReaderStream::new(file)), len))
}

fn part_with_mime(
    part: reqwest::multipart::Part,
    content_type: &str,
) -> Result<reqwest::multipart::Part, WorkflowError> {
    part.mime_str(content_type)
        .map_err(|e| input_error(format!("content_type '{content_type}' inválido: {e}")))
}

/// Extrae `filename="..."` de un header content-disposition
fn content_disposition_filename(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let value = headers
        .get(reqwest::header::CONTENT_DISPOSITION)?
        .to_str()
        .ok()?;
    let after = value.split("filename=").nth(1)?;
    let name = after.split(';').next()?.trim().trim_matches('"');
    (!name.is_empty()).then(|| name.to_string())
}

#[async_trait]
impl Task for HttpRequestTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let request: RequestInput = serde_json::from_value(input.0)
            .map_err(|e| input_error(format!("Input inválido: {e}")))?;

        let method: reqwest::Method = request
            .method
            .parse()
            .map_err(|_| input_error(format!("Método HTTP '{}' no válido", request.method)))?;

        let bodies_present = [
            request.body.is_some(),
            request.form.is_some(),
            request.text.is_some(),
            request.body_blob.is_some(),
            request.multipart.is_some(),
        ]
        .iter()
        .filter(|present| **present)
        .count();
        if bodies_present > 1 {
            return Err(input_error(
                "body, form, text, body_blob y multipart son mutuamente excluyentes",
            ));
        }

        let mut builder = self.client.request(method, &request.url);
        for (key, value) in &request.headers {
            builder = builder.header(key, value);
        }
        if !request.query.is_empty() {
            builder = builder.query(&request.query);
        }

        if let Some(body) = &request.body {
            builder = builder.json(body);
        } else if let Some(form) = &request.form {
            builder = builder.form(form);
        } else if let Some(text) = &request.text {
            if !has_header(&request.headers, "content-type") {
                builder = builder.header(reqwest::header::CONTENT_TYPE, "text/plain");
            }
            builder = builder.body(text.clone());
        } else if let Some(blob) = &request.body_blob {
            let (body, len) = blob_stream(ctx, blob).await?;
            if !has_header(&request.headers, "content-type") {
                builder = builder.header(reqwest::header::CONTENT_TYPE, "application/octet-stream");
            }
            builder = builder
                .header(reqwest::header::CONTENT_LENGTH, len)
                .body(body);
        } else if let Some(parts) = &request.multipart {
            let mut form = reqwest::multipart::Form::new();
            for (name, part) in parts {
                let built = match part {
                    MultipartPart::Plain(text) => reqwest::multipart::Part::text(text.clone()),
                    MultipartPart::Text { text, content_type } => {
                        let built = reqwest::multipart::Part::text(text.clone());
                        match content_type {
                            Some(ct) => part_with_mime(built, ct)?,
                            None => built,
                        }
                    }
                    MultipartPart::Json { json } => {
                        let serialized = serde_json::to_string(json).map_err(|e| {
                            input_error(format!("La parte '{name}' no serializa a JSON: {e}"))
                        })?;
                        part_with_mime(
                            reqwest::multipart::Part::text(serialized),
                            "application/json",
                        )?
                    }
                    MultipartPart::Blob {
                        blob,
                        filename,
                        content_type,
                    } => {
                        let (body, len) = blob_stream(ctx, blob).await?;
                        let filename = filename
                            .clone()
                            .or_else(|| blob.name.clone())
                            .unwrap_or_else(|| blob.id.clone());
                        let built = reqwest::multipart::Part::stream_with_length(body, len)
                            .file_name(filename);
                        part_with_mime(
                            built,
                            content_type
                                .as_deref()
                                .unwrap_or("application/octet-stream"),
                        )?
                    }
                };
                form = form.part(name.clone(), built);
            }
            builder = builder.multipart(form);
        }

        builder = match &request.auth {
            Some(Auth::Basic { username, password }) => {
                builder.basic_auth(username, password.as_ref())
            }
            Some(Auth::Bearer { token }) => builder.bearer_auth(token),
            None => builder,
        };

        let response = builder.send().await.map_err(|e| {
            let mut err = WorkflowError::new(
                "HTTP_REQUEST_FAILED",
                format!("La petición a '{}' falló: {e}", request.url),
            );
            err.source = Some(Box::new(e));
            err
        })?;

        let status = response.status().as_u16();
        let headers: serde_json::Map<String, Value> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    Value::String(String::from_utf8_lossy(value.as_bytes()).into_owned()),
                )
            })
            .collect();

        let is_json = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.contains("json"));

        let body = match request.response_body {
            ResponseBodyMode::Blob => {
                let name = content_disposition_filename(response.headers());
                let blob = download_to_blob(ctx, response, name).await?;
                serde_json::to_value(&blob).unwrap_or(Value::Null)
            }
            mode => {
                let raw = response.text().await.map_err(|e| {
                    WorkflowError::new(
                        "HTTP_REQUEST_FAILED",
                        format!("No se pudo leer el cuerpo de la respuesta: {e}"),
                    )
                })?;
                if raw.is_empty() {
                    Value::Null
                } else if mode == ResponseBodyMode::Auto && is_json {
                    serde_json::from_str(&raw).unwrap_or(Value::String(raw))
                } else {
                    Value::String(raw)
                }
            }
        };

        let output = json!({ "status": status, "headers": headers, "body": body });

        if request.fail_on_error_status && status >= 400 {
            let mut err = WorkflowError::new(
                "HTTP_STATUS_ERROR",
                format!("La petición a '{}' devolvió status {status}", request.url),
            );
            err.response = Some(Box::new(WorkflowData(output)));
            return Err(err);
        }

        Ok(WorkflowData(output))
    }
}

/// Baja el cuerpo de la respuesta por streaming a un archivo temporal y lo
/// importa al BlobStore de la ejecución (mismo patrón que la extensión sftp).
async fn download_to_blob(
    ctx: &WorkflowContext,
    response: reqwest::Response,
    name: Option<String>,
) -> Result<BlobRef, WorkflowError> {
    let temp: PathBuf = std::env::temp_dir().join(format!("wf-http-{}", uuid::Uuid::now_v7()));
    let result = async {
        let mut file = tokio::fs::File::create(&temp).await.map_err(|e| {
            WorkflowError::new(
                "BLOB_IO_ERROR",
                format!("No se pudo crear el archivo temporal de descarga: {e}"),
            )
        })?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                WorkflowError::new(
                    "HTTP_REQUEST_FAILED",
                    format!("No se pudo leer el cuerpo de la respuesta: {e}"),
                )
            })?;
            file.write_all(&chunk).await.map_err(|e| {
                WorkflowError::new(
                    "BLOB_IO_ERROR",
                    format!("No se pudo escribir la descarga a disco: {e}"),
                )
            })?;
        }
        file.flush().await.map_err(|e| {
            WorkflowError::new(
                "BLOB_IO_ERROR",
                format!("No se pudo escribir la descarga a disco: {e}"),
            )
        })?;
        ctx.blobs().import_file(&temp, name).await
    }
    .await;
    let _ = tokio::fs::remove_file(&temp).await;
    result
}
