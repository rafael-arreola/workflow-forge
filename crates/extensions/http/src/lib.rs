//! workflow-forge `http` extension: declarative HTTP/S client.
//!
//! `http.request` — input (body fields are mutually exclusive):
//! ```json
//! {
//!   "url": "https://api.example.com/users",
//!   "method": "POST",
//!   "headers": { "x-api-key": "..." },
//!   "query": { "page": "1" },
//!   "body": { "name": "ada" },
//!   "form": { "grant_type": "client_credentials" },
//!   "text": "<xml>...</xml>",
//!   "body_blob": { "$blob": "01J..." },
//!   "multipart": {
//!     "metadata": { "json": { "order": 123 } },
//!     "note": "plain text",
//!     "file": { "blob": { "$blob": "01J..." }, "filename": "sales.csv", "content_type": "text/csv" }
//!   },
//!   "auth": { "type": "bearer", "token": "..." },
//!   "response_body": "auto",
//!   "fail_on_error_status": false
//! }
//! ```
//! Output: `{ "status": 200, "headers": {...}, "body": <json|string|{"$blob"...}|null> }`.
//!
//! Blobs (upload and download) are transferred via streaming, never
//! fully loaded into the JSON context. With `response_body: "blob"` the
//! response body is saved to the execution's BlobStore.
//!
//! A 4xx/5xx status is not an error by default: the status is data and is
//! routed with gateways. With `fail_on_error_status: true` the task fails
//! and the node's `retry`/`on_error` apply. With `retry_on_status: [429, 503]`
//! only those statuses fail (the transient ones), without wasting retries on
//! permanent errors; on any status-based failure the `Retry-After` header is
//! attached as a hint, and the node's retry policy waits at least that long
//! before the next attempt.

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

/// Error codes this extension can emit. Same contract as
/// [`workflow_forge_core::error::codes`]: stable constants, never change
/// value. Blob errors reuse the core codes
/// (`BLOB_IO_ERROR`).
pub mod codes {
    /// The `http.request` input does not satisfy its contract: cannot
    /// deserialize, the method is invalid, or combines mutually exclusive bodies.
    pub const HTTP_INPUT_INVALID: &str = "HTTP_INPUT_INVALID";
    /// The request could not be completed (DNS, connection, TLS, body read).
    /// Usually transient: a natural candidate for `retry`.
    pub const HTTP_REQUEST_FAILED: &str = "HTTP_REQUEST_FAILED";
    /// The response had status >= 400 and the input requested
    /// `fail_on_error_status: true`. The full `{status, headers, body}`
    /// output travels in `error.response`.
    pub const HTTP_STATUS_ERROR: &str = "HTTP_STATUS_ERROR";
}

/// Registers all extension tasks in the registry.
///
/// The default task reuses a single `reqwest::Client`, which already performs
/// **connection pooling** (keep-alive per host). To tune the pool
/// (size, timeouts, proxy, TLS) at volume, use [`register_with_client`].
pub fn register(registry: &TaskRegistry) {
    registry.register(HttpRequestTask::default());
}

/// Like [`register`], but with a `reqwest::Client` provided by the host: the
/// recommended approach for production at volume, where you want to control the
/// pool (`pool_max_idle_per_host`, `timeout`, `connect_timeout`, proxy, etc.).
pub fn register_with_client(registry: &TaskRegistry, client: reqwest::Client) {
    registry.register(HttpRequestTask::with_client(client));
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("valid static schema")
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
    /// Statuses treated as **retryable** failures: the task fails with
    /// `HTTP_STATUS_ERROR` (and, if the node has `retry`, retries) only if the
    /// status is in this list. Designed for transient statuses (429, 503)
    /// without wasting retries on permanent errors (400, 404).
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
    /// JSON if the content-type is json; string otherwise (historical behavior)
    #[default]
    Auto,
    /// Forces string even if the content-type is json
    Text,
    /// Saves the body to the BlobStore and returns the `$blob` reference
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
            "Executes an HTTP/S request and returns {status, headers, body}. \
             Bodies: body (JSON), form (urlencoded), text (raw), body_blob \
             (streamed binary) or multipart (form-data with blobs) — \
             mutually exclusive. With response_body: blob the response body \
             is saved to the BlobStore. With fail_on_error_status, \
             a status >= 400 fails the task"
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
                "body": { "description": "JSON request body" },
                "form": {
                    "description": "application/x-www-form-urlencoded body",
                    "type": "object",
                    "additionalProperties": { "type": "string" }
                },
                "text": {
                    "description": "Raw body; content-type via headers (default text/plain)",
                    "type": "string"
                },
                "body_blob": {
                    "description": "Streamed binary body from a blob; content-type via headers (default application/octet-stream)",
                    "type": "object",
                    "required": ["$blob"]
                },
                "multipart": {
                    "description": "multipart/form-data body: each property is a part",
                    "type": "object",
                    "additionalProperties": {
                        "oneOf": [
                            { "type": "string", "description": "Plain text part" },
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
                    "description": "How to interpret the response body",
                    "enum": ["auto", "text", "blob"],
                    "default": "auto"
                },
                "fail_on_error_status": { "type": "boolean", "default": false },
                "retry_on_status": {
                    "description": "Statuses treated as retryable failures (e.g. [429, 503]); with `retry` on the node they are retried honoring Retry-After, without wasting attempts on permanent errors",
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
                    "description": "JSON, string, null or {\"$blob\"} with response_body: blob"
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
    /// Builds the task reusing a `reqwest::Client` provided by the
    /// host. A single client shares and reuses connections (keep-alive pool)
    /// across all requests; inject it tuned for volume use.
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

/// Streamed body from a file-backed blob, with its length
async fn blob_stream(
    ctx: &WorkflowContext,
    blob: &BlobRef,
) -> Result<(reqwest::Body, u64), WorkflowError> {
    let path = ctx.blobs().local_path(blob)?;
    let file = tokio::fs::File::open(&path).await.map_err(|e| {
        WorkflowError::new(
            core_codes::BLOB_IO_ERROR,
            format!("Could not open blob '{}': {e}", blob.id),
        )
    })?;
    let len = file
        .metadata()
        .await
        .map_err(|e| {
            WorkflowError::new(
                core_codes::BLOB_IO_ERROR,
                format!("Could not read size of blob '{}': {e}", blob.id),
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
        .map_err(|e| input_error(format!("invalid content_type '{content_type}': {e}")))
}

/// Interprets the `Retry-After` header: seconds (`"120"`) or HTTP-date
/// (`"Wed, 21 Oct 2099 07:28:00 GMT"`). Returns the wait in ms, or `None` if
/// the header is missing or cannot be parsed. A date in the past yields `Some(0)`.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(secs.saturating_mul(1000));
    }
    // HTTP-date (IMF-fixdate): "Wed, 21 Oct 2099 07:28:00 GMT". The day of the
    // week is ignored (it is derivable from the date; we do not depend on the
    // server sending it consistently). chrono only parses; "now" comes from
    // SystemTime (does not require the `clock` feature)
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

/// Extracts `filename="..."` from a content-disposition header
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
            .map_err(|e| input_error(format!("Invalid input: {e}")))?;

        let method: reqwest::Method = request
            .method
            .parse()
            .map_err(|_| input_error(format!("Invalid HTTP method '{}'", request.method)))?;

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
                "body, form, text, body_blob and multipart are mutually exclusive",
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
                            input_error(format!("Part '{name}' cannot be serialized to JSON: {e}"))
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
                format!("Request to '{}' failed: {e}", request.url),
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

        // Read before consuming the body (the Blob branch moves `response`)
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
                        format!("Could not read response body: {e}"),
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

        // A status fails the task if it is marked as retryable or if
        // `fail_on_error_status` covers all >= 400. In both cases
        // `Retry-After` is attached as a hint for the node's retry policy.
        let should_fail = request.retry_on_status.contains(&status)
            || (request.fail_on_error_status && status >= 400);
        if should_fail {
            let mut err = WorkflowError::new(
                codes::HTTP_STATUS_ERROR,
                format!("Request to '{}' returned status {status}", request.url),
            );
            err.response = Some(Box::new(WorkflowData(output)));
            err.retry_after_ms = retry_after_ms;
            return Err(err);
        }

        Ok(WorkflowData(output))
    }
}

/// Downloads the response body via streaming to a temporary file and imports
/// it to the execution's BlobStore (same pattern as the sftp extension).
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
                format!("Could not create temporary download file: {e}"),
            )
        })?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                WorkflowError::new(
                    codes::HTTP_REQUEST_FAILED,
                    format!("Could not read response body: {e}"),
                )
            })?;
            file.write_all(&chunk).await.map_err(|e| {
                WorkflowError::new(
                    core_codes::BLOB_IO_ERROR,
                    format!("Could not write download to disk: {e}"),
                )
            })?;
        }
        file.flush().await.map_err(|e| {
            WorkflowError::new(
                core_codes::BLOB_IO_ERROR,
                format!("Could not write download to disk: {e}"),
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
    fn retry_after_in_seconds() {
        assert_eq!(parse_retry_after(&headers("120")), Some(120_000));
        assert_eq!(parse_retry_after(&headers("0")), Some(0));
    }

    #[test]
    fn retry_after_missing_or_invalid_is_none() {
        assert_eq!(parse_retry_after(&HeaderMap::new()), None);
        assert_eq!(parse_retry_after(&headers("right now")), None);
    }

    #[test]
    fn retry_after_as_future_http_date() {
        // A date far in the future should yield a large positive wait
        let ms = parse_retry_after(&headers("Wed, 21 Oct 2099 07:28:00 GMT"))
            .expect("a valid date produces ms");
        assert!(ms > 0);
    }

    #[test]
    fn retry_after_as_past_http_date_is_zero() {
        assert_eq!(
            parse_retry_after(&headers("Wed, 21 Oct 1999 07:28:00 GMT")),
            Some(0)
        );
    }
}
