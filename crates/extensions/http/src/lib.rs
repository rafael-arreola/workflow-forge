//! Extensión `http` de workflow-forge: cliente HTTP/S declarativo.
//!
//! `http.request` — input:
//! ```json
//! {
//!   "url": "https://api.example.com/users",
//!   "method": "POST",
//!   "headers": { "x-api-key": "..." },
//!   "query": { "page": "1" },
//!   "body": { "name": "ada" },
//!   "auth": { "type": "bearer", "token": "..." },
//!   "fail_on_error_status": false
//! }
//! ```
//! Output: `{ "status": 200, "headers": {...}, "body": <json|string|null> }`.
//!
//! Un status 4xx/5xx no es error por default: el status es dato y se rutea
//! con gateways. Con `fail_on_error_status: true` la tarea falla y aplican
//! `retry`/`on_error` del nodo.

use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use workflow_forge_core::context::WorkflowContext;
use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::types::{WorkflowData, WorkflowResult};

/// Registra todas las tareas de la extensión en el registry
pub fn register(registry: &TaskRegistry) {
    registry.register(HttpRequestTask::default());
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
    auth: Option<Auth>,
    #[serde(default)]
    fail_on_error_status: bool,
}

fn default_method() -> String {
    "GET".to_string()
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
             Con fail_on_error_status, un status >= 400 falla la tarea"
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
                "fail_on_error_status": { "type": "boolean", "default": false }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["status", "headers"],
            "properties": {
                "status": { "type": "integer" },
                "headers": { "type": "object" },
                "body": {}
            }
        })));
        Self {
            manifest,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Task for HttpRequestTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let request: RequestInput = serde_json::from_value(input.0).map_err(|e| {
            WorkflowError::new("HTTP_INPUT_INVALID", format!("Input inválido: {e}"))
        })?;

        let method: reqwest::Method = request.method.parse().map_err(|_| {
            WorkflowError::new(
                "HTTP_INPUT_INVALID",
                format!("Método HTTP '{}' no válido", request.method),
            )
        })?;

        let mut builder = self.client.request(method, &request.url);
        for (key, value) in &request.headers {
            builder = builder.header(key, value);
        }
        if !request.query.is_empty() {
            builder = builder.query(&request.query);
        }
        if let Some(body) = &request.body {
            builder = builder.json(body);
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

        let raw = response.text().await.map_err(|e| {
            WorkflowError::new(
                "HTTP_REQUEST_FAILED",
                format!("No se pudo leer el cuerpo de la respuesta: {e}"),
            )
        })?;

        let body = if raw.is_empty() {
            Value::Null
        } else if is_json {
            serde_json::from_str(&raw).unwrap_or(Value::String(raw))
        } else {
            Value::String(raw)
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
