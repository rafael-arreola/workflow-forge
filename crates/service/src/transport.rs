use crate::{auth::valid_identity, error::ApiError, runtime::Shared};
use axum::{
    body::{Body, HttpBody},
    extract::{Request, State},
    http::{Method, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures::{FutureExt, StreamExt};
use serde::de::DeserializeOwned;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, atomic::Ordering},
};
use tokio::{sync::OwnedSemaphorePermit, time::Instant};
use tokio_util::sync::CancellationToken;
use workflow_forge::v2::{AccessContext, ByteStream, ForgeError};

#[derive(Clone)]
pub(crate) struct RequestBudget {
    pub deadline: Instant,
    // A transfer worker retains the permit if its HTTP caller disappears.
    pub permit: Arc<OwnedSemaphorePermit>,
}

pub(crate) async fn guard(
    State(shared): State<Arc<Shared>>,
    mut request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if request.method() == Method::GET && matches!(path, "/health/live" | "/health/ready") {
        return next.run(request).await;
    }
    if !shared.ready.load(Ordering::Acquire) {
        return ApiError::new("runtime.unavailable", "Service is draining").into_response();
    }
    let permit = match shared.requests.clone().try_acquire_owned() {
        Ok(value) => Arc::new(value),
        Err(_) => return ApiError::new("http.busy", "Request capacity exhausted").into_response(),
    };
    let deadline = Instant::now() + shared.options.request_timeout;
    request.extensions_mut().insert(RequestBudget {
        deadline,
        permit: permit.clone(),
    });
    let work = async {
        if request.uri().to_string().len() > 4096 {
            return Err(ApiError::new("http.invalid_request", "URI exceeds limit"));
        }
        let access = shared
            .authenticator
            .authenticate(request.headers())
            .await
            .map_err(|_| ApiError::new("http.unauthenticated", "Credential rejected"))?;
        if !valid_identity(&access) || access.scope != shared.options.scope {
            return Err(ApiError::new(
                "access.denied",
                "Caller scope is not served here",
            ));
        }
        request.extensions_mut().insert(access);
        Ok::<_, ApiError>(next.run(request).await)
    };
    let response = tokio::select! {
        biased;
        _ = shared.force.cancelled() => Err(ApiError::new("runtime.unavailable", "Service stopped")),
        _ = tokio::time::sleep_until(deadline) => Err(ApiError::new("http.timeout", "Request deadline exceeded")),
        result = AssertUnwindSafe(work).catch_unwind() => result.unwrap_or_else(|_| Err(ApiError::new("http.internal", "Handler failed"))),
    };
    // Permit a small timeout diagnostic to reach the caller after the command
    // deadline. This grace does not extend execution or transfer deadlines.
    let deadline = if response.is_err() {
        deadline.max(Instant::now() + std::time::Duration::from_secs(1))
    } else {
        deadline
    };
    let mut response = response.unwrap_or_else(IntoResponse::into_response);
    if response
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|v| v == "application/json")
        && response
            .body()
            .size_hint()
            .upper()
            .is_some_and(|n| n > shared.options.max_response_bytes as u64)
    {
        response =
            ApiError::new("http.response_too_large", "Response budget exceeded").into_response();
    }
    let (parts, body) = response.into_parts();
    // Retain the request slot through the last body chunk, including downloads.
    let stream = futures::stream::unfold(
        (body.into_data_stream(), permit, shared.force.clone(), false),
        move |(mut body, permit, cancel, done)| async move {
            if done {
                return None;
            }
            let item = tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(std::io::Error::other("service stopped")),
                _ = tokio::time::sleep_until(deadline) => Err(std::io::Error::other("response deadline")),
                item = body.next() => match item {
                    Some(Ok(bytes)) => Ok(bytes),
                    Some(Err(_)) => Err(std::io::Error::other("response stream failed")),
                    None => return None,
                }
            };
            let done = item.is_err();
            Some((item, (body, permit, cancel, done)))
        },
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

pub(crate) async fn json<T: DeserializeOwned>(
    request: Request,
    limit: usize,
) -> Result<T, ApiError> {
    let mut values = request.headers().get_all(header::CONTENT_TYPE).iter();
    let content_type = values.next().and_then(|v| v.to_str().ok());
    let valid = content_type.is_some_and(|v| {
        let media = v
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        media == "application/json"
            || (media.starts_with("application/") && media.ends_with("+json"))
    });
    if !valid || values.next().is_some() {
        return Err(ApiError::new(
            "http.content_type",
            "JSON content type required",
        ));
    }
    // Count actual chunks, independent of Content-Length and extractor defaults.
    let mut stream = request.into_body().into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|_| ApiError::new("http.invalid_request", "Request body failed"))?;
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err(ApiError::new("http.body_too_large", "Request body limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::new("http.invalid_request", "Malformed command JSON"))
}

pub(crate) fn access(request: &Request) -> AccessContext {
    request
        .extensions()
        .get::<AccessContext>()
        .expect("authenticated router")
        .clone()
}

/// Failure is delivered to ArtifactStore so it can await staging cleanup.
pub(crate) fn upload_stream(
    body: Body,
    max: usize,
    deadline: Instant,
    request_cancel: CancellationToken,
    service_cancel: CancellationToken,
) -> ByteStream {
    Box::pin(futures::stream::unfold(
        (body.into_data_stream(), max, false),
        move |(mut source, remaining, done)| {
            let caller = request_cancel.clone();
            let service = service_cancel.clone();
            async move {
                if done {
                    return None;
                }
                let item = tokio::select! {
                    biased;
                    _ = caller.cancelled() => Err(ForgeError::new("http.invalid_request", "Upload caller left")),
                    _ = service.cancelled() => Err(ForgeError::new("runtime.unavailable", "Service stopped")),
                    _ = tokio::time::sleep_until(deadline) => Err(ForgeError::new("http.timeout", "Upload deadline exceeded")),
                    next = source.next() => match next {
                        Some(Ok(bytes)) if bytes.len() <= remaining => Ok(bytes.to_vec()),
                        Some(Ok(_)) => Err(ForgeError::new("http.body_too_large", "Upload byte limit")),
                        Some(Err(_)) => Err(ForgeError::new("http.invalid_request", "Upload stream failed")),
                        None => return None,
                    }
                };
                let done = item.is_err();
                let remaining = remaining.saturating_sub(item.as_ref().map_or(0, Vec::len));
                Some((item, (source, remaining, done)))
            }
        },
    ))
}
