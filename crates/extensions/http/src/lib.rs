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
//! `retry`/`on_error` del nodo. Con `retry_on_status: [429, 503]` solo esos
//! status fallan (los transitorios), sin gastar reintentos en errores
//! permanentes; en cualquier fallo por status se adjunta el header
//! `Retry-After` como pista, y la política de retry del nodo espera al menos
//! ese tiempo antes del siguiente intento.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::error::codes as core_codes;
use workflow_forge_core::io::blob::BlobRef;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Códigos de error que esta extensión puede emitir. Mismo contrato que
/// [`workflow_forge_core::error::codes`]: constantes estables, nunca cambian
/// de valor. Los errores de blobs reusan los códigos del core
/// (`BLOB_IO_ERROR`).
pub mod codes {
    /// El input de `http.request` no cumple el contrato: no deserializa,
    /// el método no es válido o combina cuerpos mutuamente excluyentes.
    pub const HTTP_INPUT_INVALID: &str = "HTTP_INPUT_INVALID";
    /// La petición no se pudo completar (DNS, conexión, TLS, lectura del
    /// cuerpo). Suele ser transitorio: candidato natural a `retry`.
    pub const HTTP_REQUEST_FAILED: &str = "HTTP_REQUEST_FAILED";
    /// La respuesta tuvo status >= 400 y el input pedía
    /// `fail_on_error_status: true`. El output `{status, headers, body}`
    /// completo viaja en `error.response`.
    pub const HTTP_STATUS_ERROR: &str = "HTTP_STATUS_ERROR";
}

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
    /// Status que se tratan como fallo **reintentable**: la tarea falla con
    /// `HTTP_STATUS_ERROR` (y, si el nodo tiene `retry`, se reintenta) solo si
    /// el status está en esta lista. Pensado para los transitorios (429, 503)
    /// sin gastar reintentos en errores permanentes (400, 404).
    #[serde(default)]
    retry_on_status: Vec<u16>,
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
                "fail_on_error_status": { "type": "boolean", "default": false },
                "retry_on_status": {
                    "description": "Status tratados como fallo reintentable (p.ej. [429, 503]); con `retry` en el nodo se reintentan honrando Retry-After, sin gastar intentos en errores permanentes",
                    "type": "array",
                    "items": { "type": "integer" }
                }
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
    WorkflowError::new(codes::HTTP_INPUT_INVALID, message)
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
            core_codes::BLOB_IO_ERROR,
            format!("No se pudo abrir el blob '{}': {e}", blob.id),
        )
    })?;
    let len = file
        .metadata()
        .await
        .map_err(|e| {
            WorkflowError::new(
                core_codes::BLOB_IO_ERROR,
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

/// Interpreta el header `Retry-After`: segundos (`"120"`) o HTTP-date
/// (`"Wed, 21 Oct 2099 07:28:00 GMT"`). Devuelve la espera en ms, o `None` si
/// el header falta o no parsea. Una fecha en el pasado da `Some(0)`.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(secs.saturating_mul(1000));
    }
    // HTTP-date (IMF-fixdate): "Wed, 21 Oct 2099 07:28:00 GMT". El día de la
    // semana se ignora (es derivable de la fecha; no dependemos de que el
    // servidor lo mande consistente). chrono solo parsea; el "ahora" sale de
    // SystemTime (no requiere la feature `clock`)
    let date = raw.split_once(", ").map(|(_, rest)| rest).unwrap_or(raw);
    let when_ms = chrono::NaiveDateTime::parse_from_str(date, "%d %b %Y %H:%M:%S GMT")
        .ok()?
        .and_utc()
        .timestamp_millis();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Some((when_ms - now_ms).max(0) as u64)
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
                codes::HTTP_REQUEST_FAILED,
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

        // Se lee antes de consumir el cuerpo (la rama Blob mueve `response`)
        let retry_after_ms = parse_retry_after(response.headers());

        let body = match request.response_body {
            ResponseBodyMode::Blob => {
                let name = content_disposition_filename(response.headers());
                let blob = download_to_blob(ctx, response, name).await?;
                serde_json::to_value(&blob).unwrap_or(Value::Null)
            }
            mode => {
                let raw = response.text().await.map_err(|e| {
                    WorkflowError::new(
                        codes::HTTP_REQUEST_FAILED,
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

        // Un status falla la tarea si está marcado como reintentable o si
        // `fail_on_error_status` cubre todos los >= 400. En ambos casos se
        // adjunta `Retry-After` como pista para la política de retry del nodo.
        let should_fail = request.retry_on_status.contains(&status)
            || (request.fail_on_error_status && status >= 400);
        if should_fail {
            let mut err = WorkflowError::new(
                codes::HTTP_STATUS_ERROR,
                format!("La petición a '{}' devolvió status {status}", request.url),
            );
            err.response = Some(Box::new(WorkflowData(output)));
            err.retry_after_ms = retry_after_ms;
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
                core_codes::BLOB_IO_ERROR,
                format!("No se pudo crear el archivo temporal de descarga: {e}"),
            )
        })?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                WorkflowError::new(
                    codes::HTTP_REQUEST_FAILED,
                    format!("No se pudo leer el cuerpo de la respuesta: {e}"),
                )
            })?;
            file.write_all(&chunk).await.map_err(|e| {
                WorkflowError::new(
                    core_codes::BLOB_IO_ERROR,
                    format!("No se pudo escribir la descarga a disco: {e}"),
                )
            })?;
        }
        file.flush().await.map_err(|e| {
            WorkflowError::new(
                core_codes::BLOB_IO_ERROR,
                format!("No se pudo escribir la descarga a disco: {e}"),
            )
        })?;
        ctx.blobs().import_file(&temp, name).await
    }
    .await;
    let _ = tokio::fs::remove_file(&temp).await;
    result
}

#[cfg(test)]
mod tests {
    use super::parse_retry_after;
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};

    fn headers(retry_after: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(RETRY_AFTER, HeaderValue::from_str(retry_after).unwrap());
        h
    }

    #[test]
    fn retry_after_en_segundos() {
        assert_eq!(parse_retry_after(&headers("120")), Some(120_000));
        assert_eq!(parse_retry_after(&headers("0")), Some(0));
    }

    #[test]
    fn retry_after_ausente_o_invalido_es_none() {
        assert_eq!(parse_retry_after(&HeaderMap::new()), None);
        assert_eq!(parse_retry_after(&headers("ya mismo")), None);
    }

    #[test]
    fn retry_after_como_http_date_futura() {
        // Una fecha muy futura debe dar una espera positiva y grande
        let ms = parse_retry_after(&headers("Wed, 21 Oct 2099 07:28:00 GMT"))
            .expect("una fecha válida produce ms");
        assert!(ms > 0);
    }

    #[test]
    fn retry_after_como_http_date_pasada_es_cero() {
        assert_eq!(
            parse_retry_after(&headers("Wed, 21 Oct 1999 07:28:00 GMT")),
            Some(0)
        );
    }
}
