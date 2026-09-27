use axum::{
    body::Body,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::io::{self, Write};
use workflow_forge::v2::ForgeError;

#[derive(Debug)]
pub(crate) struct ApiError(pub ForgeError);
impl From<ForgeError> for ApiError {
    fn from(value: ForgeError) -> Self {
        Self(value)
    }
}
impl ApiError {
    pub fn new(code: &str, message: &str) -> Self {
        let mut error = ForgeError::new(code, message);
        error.diagnostics[0].phase = "transport".into();
        Self(error)
    }
}

fn status(code: &str) -> StatusCode {
    match code {
        "http.unauthenticated" => StatusCode::UNAUTHORIZED,
        "access.denied" => StatusCode::FORBIDDEN,
        "not_found" | "resource.not_found" | "resource.missing" | "wait.not_found" => {
            StatusCode::NOT_FOUND
        }
        "state.conflict" | "not_ready" | "effect.unknown" => StatusCode::CONFLICT,
        "http.body_too_large" | "http.response_too_large" => StatusCode::PAYLOAD_TOO_LARGE,
        "http.content_type" => StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "http.invalid_request" | "http.cursor_invalid" => StatusCode::BAD_REQUEST,
        "http.method_not_allowed" => StatusCode::METHOD_NOT_ALLOWED,
        "http.busy" | "admission.full" => StatusCode::TOO_MANY_REQUESTS,
        "http.timeout" => StatusCode::SERVICE_UNAVAILABLE,
        "internal" | "http.internal" => StatusCode::INTERNAL_SERVER_ERROR,
        c if c.starts_with("runtime.") || c.starts_with("store.") => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

fn message(code: &str) -> &'static str {
    match code {
        "http.unauthenticated" => "A valid bearer credential is required",
        "access.denied" => "Command is not permitted",
        "not_found" | "resource.not_found" | "resource.missing" | "wait.not_found" => {
            "Requested resource is unavailable"
        }
        "state.conflict" => "Command or cursor conflicts with the current revision",
        "not_ready" => "A confirmed result is not available yet",
        "effect.unknown" => "The effect requires an explicit resolution",
        "http.body_too_large" => "Request body exceeds its byte limit",
        "http.response_too_large" => "Response exceeds its byte limit; request a smaller page",
        "http.content_type" => "This command requires a JSON content type",
        "http.invalid_request" => "Request does not match the transport contract",
        "http.cursor_invalid" => "Cursor is invalid for this collection and caller",
        "http.method_not_allowed" => "Method is not available for this route",
        "http.busy" | "admission.full" => "Admission capacity is exhausted",
        "http.timeout" => {
            "Request deadline exceeded; inspect or deduplicate a possibly committed command"
        }
        "internal" | "http.internal" => "The transport could not complete this request",
        c if c.starts_with("runtime.") => "Runtime is unavailable for this command",
        c if c.starts_with("store.") => "Storage could not complete this command",
        c if c.starts_with("schema.") => "Data does not satisfy its schema contract",
        c if c.starts_with("definition.") => "Workflow definition is invalid",
        c if c.starts_with("mapping.") => "Data binding cannot be evaluated",
        c if c.starts_with("capability.") => "The composition does not support this capability",
        c if c.starts_with("resource.") => "A required resource or resource budget is unavailable",
        _ => "Workflow command failed; inspect its code, location and classification",
    }
}

/// Keep structured diagnoses, but never echo arbitrary adapter/parser messages.
pub(crate) fn sanitize(mut error: ForgeError) -> ForgeError {
    if error.diagnostics.is_empty() {
        return ForgeError::new("internal", message("internal"));
    }
    if error.diagnostics.len() > 256
        || error.diagnostics.iter().any(|d| {
            d.location.field.len() > 2048
                || d.location.node.as_ref().is_some_and(|v| v.len() > 256)
                || d.location.data.as_ref().is_some_and(|v| v.len() > 2048)
        })
    {
        return ForgeError::new(
            "http.response_too_large",
            message("http.response_too_large"),
        );
    }
    for d in &mut error.diagnostics {
        if !valid_code(&d.code) {
            d.code = "operation.failed".into();
        }
        d.message = message(&d.code).into();
        if !valid_code(&d.phase) {
            d.phase = "execution".into();
        }
        if let Some(operation) = &mut d.operation_error {
            if !valid_code(&operation.code) {
                operation.code = "operation.failed".into();
            }
            operation.message = "Operation failed".into();
        }
    }
    error
}
fn valid_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let error = sanitize(self.0);
        let status = status(error.code());
        let mut response = Response::new(Body::from(
            serde_json::to_vec(&error).expect("diagnostics serialize"),
        ));
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        response.headers_mut().insert(
            header::X_CONTENT_TYPE_OPTIONS,
            header::HeaderValue::from_static("nosniff"),
        );
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                header::HeaderValue::from_static("Bearer"),
            );
        }
        response
    }
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("response byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn json_response(
    status: StatusCode,
    value: &impl Serialize,
    limit: usize,
) -> Result<Response, ApiError> {
    let mut buffer = LimitedBuffer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut buffer, value)
        .map_err(|_| ApiError::new("http.response_too_large", "Response budget exceeded"))?;
    let mut response = Response::new(Body::from(buffer.bytes));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}
