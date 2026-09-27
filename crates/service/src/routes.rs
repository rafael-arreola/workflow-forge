use crate::{
    dto::*,
    error::{ApiError, json_response, sanitize},
    pagination::{self, PageQuery},
    runtime::Shared,
    transport::{self, RequestBudget},
};
use axum::{
    Router,
    body::Body,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::StreamExt;
use serde::Serialize;
use serde_json::json;
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use workflow_forge::v2::*;

pub(crate) fn router(shared: Arc<Shared>) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v2/catalog", get(catalog))
        .route("/v2/workflows/prepare", post(prepare))
        .route("/v2/runs", post(start))
        .route("/v2/runs/{id}", get(status))
        .route("/v2/runs/{id}/result", get(result))
        .route("/v2/runs/{id}/invocations", get(invocations))
        .route("/v2/runs/{id}/waits", get(waits))
        .route("/v2/runs/{id}/audit", get(audit))
        .route("/v2/runs/{id}/cancel", post(cancel))
        .route("/v2/runs/{id}/signals", post(signal))
        .route("/v2/runs/{id}/effects/inspect", post(inspect))
        .route("/v2/runs/{id}/effects/reconcile", post(reconcile))
        .route("/v2/artifacts", post(upload))
        .route("/v2/artifacts/read", post(download))
        .fallback(|| async { ApiError::new("not_found", "No such route") })
        .method_not_allowed_fallback(|| async {
            ApiError::new("http.method_not_allowed", "No such method")
        })
        .layer(middleware::from_fn_with_state(
            shared.clone(),
            transport::guard,
        ))
        .with_state(shared)
}

fn respond(
    shared: &Shared,
    status: StatusCode,
    value: &impl Serialize,
) -> Result<Response, ApiError> {
    json_response(status, value, shared.options.max_response_bytes)
}
fn query(request: &Request) -> Result<PageQuery, ApiError> {
    Query::<PageQuery>::try_from_uri(request.uri())
        .map(|q| q.0)
        .map_err(|_| ApiError::new("http.invalid_request", "Invalid page query"))
}
// Extract raw paths ourselves so framework rejections cannot echo user input.
async fn run_id(request: &mut Request) -> Result<RunId, ApiError> {
    use axum::extract::FromRequestParts;
    let (mut parts, body) = std::mem::replace(request, Request::new(Body::empty())).into_parts();
    let id = Path::<String>::from_request_parts(&mut parts, &())
        .await
        .map_err(|_| ApiError::new("http.invalid_request", "Invalid run path"));
    *request = Request::from_parts(parts, body);
    let Path(id) = id?;
    if id.is_empty()
        || id.len() > 256
        || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(ApiError::new(
            "http.invalid_request",
            "Invalid run identifier",
        ));
    }
    Ok(RunId(id))
}
fn same_run(route: &RunId, body: &RunId) -> Result<(), ApiError> {
    if route != body {
        return Err(ApiError::new(
            "http.invalid_request",
            "Run identifiers disagree",
        ));
    }
    Ok(())
}

async fn live() -> Response {
    json_response(StatusCode::OK, &json!({"live": true}), 1024).expect("small health response")
}
async fn ready(State(shared): State<Arc<Shared>>) -> Response {
    let ready = shared.ready.load(Ordering::Acquire) && shared.app.is_ready();
    json_response(
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        &json!({"ready": ready}),
        1024,
    )
    .expect("small health response")
}
async fn catalog(
    State(shared): State<Arc<Shared>>,
    request: Request,
) -> Result<Response, ApiError> {
    let access = transport::access(&request);
    let mut items = shared.app.catalog(&access)?;
    items.sort_by(|a, b| a.revision.cmp(&b.revision));
    let page = pagination::page(
        items,
        query(&request)?,
        &shared.instance,
        &access,
        "catalog",
        None,
    )?;
    let response = CatalogResponse {
        capabilities: shared.app.capabilities(&access)?,
        items: page.items,
        next_cursor: page.next_cursor,
    };
    respond(&shared, StatusCode::OK, &response)
}
async fn prepare(
    State(shared): State<Arc<Shared>>,
    request: Request,
) -> Result<Response, ApiError> {
    let access = transport::access(&request);
    let body: PrepareRequest = transport::json(request, shared.options.max_json_bytes).await?;
    let key = (body.definition.id.clone(), body.definition.revision.clone());
    let mut plans = shared.plans.lock().await;
    if plans.len() >= shared.options.max_prepared && !plans.contains_key(&key) {
        return Err(ApiError::new(
            "resource.exhausted",
            "Prepared plan capacity exhausted",
        ));
    }
    let plan = shared.app.prepare(access, body.definition).await?;
    let diagnostics = if plan.diagnostics().is_empty() {
        Vec::new()
    } else {
        sanitize(ForgeError {
            diagnostics: plan.diagnostics().to_vec(),
        })
        .diagnostics
    };
    plans.insert(key.clone(), plan);
    respond(
        &shared,
        StatusCode::OK,
        &PrepareResponse {
            workflow: WorkflowRevision {
                id: key.0,
                revision: key.1,
            },
            diagnostics,
        },
    )
}
async fn start(State(shared): State<Arc<Shared>>, request: Request) -> Result<Response, ApiError> {
    let access = transport::access(&request);
    let body: StartRequest = transport::json(request, shared.options.max_json_bytes).await?;
    let plan = shared
        .plans
        .lock()
        .await
        .get(&(body.workflow.id, body.workflow.revision))
        .cloned()
        .ok_or_else(|| ApiError::new("not_found", "Workflow revision is not prepared"))?;
    let receipt = shared
        .app
        .start(
            access,
            StartRunRequest {
                plan,
                input: body.input,
                options: body.options.into(),
            },
        )
        .await?;
    let mut response = respond(&shared, StatusCode::ACCEPTED, &receipt)?;
    response.headers_mut().insert(
        header::LOCATION,
        format!("/v2/runs/{}", receipt.run_id.0)
            .parse()
            .map_err(|_| ApiError::new("http.internal", "Invalid run location"))?,
    );
    Ok(response)
}
async fn status(
    State(shared): State<Arc<Shared>>,
    mut request: Request,
) -> Result<Response, ApiError> {
    let id = run_id(&mut request).await?;
    let run = shared.app.status(transport::access(&request), id).await?;
    respond(&shared, StatusCode::OK, &RunStatus::from(run))
}
async fn result(
    State(shared): State<Arc<Shared>>,
    mut request: Request,
) -> Result<Response, ApiError> {
    let id = run_id(&mut request).await?;
    let output = shared
        .app
        .result(transport::access(&request), id.clone())
        .await?;
    respond(
        &shared,
        StatusCode::OK,
        &json!({"run_id":id,"output":output}),
    )
}
async fn invocations(
    State(shared): State<Arc<Shared>>,
    request: Request,
) -> Result<Response, ApiError> {
    run_page(shared, request, "invocations").await
}
async fn waits(State(shared): State<Arc<Shared>>, request: Request) -> Result<Response, ApiError> {
    run_page(shared, request, "waits").await
}
async fn audit(State(shared): State<Arc<Shared>>, request: Request) -> Result<Response, ApiError> {
    run_page(shared, request, "audit").await
}
async fn run_page(
    shared: Arc<Shared>,
    mut request: Request,
    collection: &str,
) -> Result<Response, ApiError> {
    let id = run_id(&mut request).await?;
    let access = transport::access(&request);
    let query = query(&request)?;
    let run = shared.app.status(access.clone(), id.clone()).await?;
    let collection_key = format!("runs/{}/{}", id.0, collection);
    match collection {
        "invocations" => respond(
            &shared,
            StatusCode::OK,
            &pagination::page(
                run.invocations
                    .into_iter()
                    .map(InvocationStatus::from)
                    .collect(),
                query,
                &shared.instance,
                &access,
                &collection_key,
                Some(run.revision),
            )?,
        ),
        "waits" => respond(
            &shared,
            StatusCode::OK,
            &pagination::page(
                run.waits.into_values().map(WaitStatus::from).collect(),
                query,
                &shared.instance,
                &access,
                &collection_key,
                Some(run.revision),
            )?,
        ),
        _ => respond(
            &shared,
            StatusCode::OK,
            &pagination::page(
                run.audit.into_iter().map(AuditStatus::from).collect(),
                query,
                &shared.instance,
                &access,
                &collection_key,
                Some(run.revision),
            )?,
        ),
    }
}
async fn cancel(
    State(shared): State<Arc<Shared>>,
    mut request: Request,
) -> Result<Response, ApiError> {
    let id = run_id(&mut request).await?;
    let access = transport::access(&request);
    if request
        .into_body()
        .into_data_stream()
        .any(|item| async move { item.map_or(true, |bytes| !bytes.is_empty()) })
        .await
    {
        return Err(ApiError::new("http.invalid_request", "Cancel has no body"));
    }
    shared.app.cancel(access, id.clone()).await?;
    respond(
        &shared,
        StatusCode::ACCEPTED,
        &json!({"run_id":id,"cancellation_requested":true}),
    )
}
async fn signal(
    State(shared): State<Arc<Shared>>,
    mut request: Request,
) -> Result<Response, ApiError> {
    let id = run_id(&mut request).await?;
    let access = transport::access(&request);
    let command: SignalCommand = transport::json(request, shared.options.max_json_bytes).await?;
    same_run(&id, &command.run_id)?;
    let receipt = shared.app.signal(access, command).await?;
    respond(&shared, StatusCode::OK, &receipt)
}
async fn inspect(
    State(shared): State<Arc<Shared>>,
    mut request: Request,
) -> Result<Response, ApiError> {
    let id = run_id(&mut request).await?;
    let access = transport::access(&request);
    let command: InspectRequest = transport::json(request, shared.options.max_json_bytes).await?;
    let finding = shared
        .app
        .inspect_effect(access, id, command.invocation_id)
        .await?;
    respond(&shared, StatusCode::OK, &finding)
}
async fn reconcile(
    State(shared): State<Arc<Shared>>,
    mut request: Request,
) -> Result<Response, ApiError> {
    let id = run_id(&mut request).await?;
    let access = transport::access(&request);
    let command: ReconcileCommand = transport::json(request, shared.options.max_json_bytes).await?;
    same_run(&id, &command.run_id)?;
    let mut receipt = shared.app.reconcile(access, command).await?;
    receipt.diagnostic = receipt.diagnostic.map(sanitize);
    respond(&shared, StatusCode::OK, &receipt)
}

async fn upload(State(shared): State<Arc<Shared>>, request: Request) -> Result<Response, ApiError> {
    let access = transport::access(&request);
    let budget = request
        .extensions()
        .get::<RequestBudget>()
        .expect("request budget")
        .clone();
    let mut media_values = request.headers().get_all(header::CONTENT_TYPE).iter();
    let media_type = media_values
        .next()
        .map(|v| v.to_str())
        .transpose()
        .map_err(|_| ApiError::new("http.invalid_request", "Invalid media type"))?
        .unwrap_or("application/octet-stream")
        .to_owned();
    if media_type.len() > 256 || media_values.next().is_some() {
        return Err(ApiError::new("http.invalid_request", "Invalid media type"));
    }
    let cancel = CancellationToken::new();
    let _cancel_on_drop = cancel.clone().drop_guard();
    let stream = transport::upload_stream(
        request.into_body(),
        shared.options.max_artifact_bytes,
        budget.deadline,
        cancel,
        shared.force.clone(),
    );
    let app = shared.app.clone();
    let (send, receive) = oneshot::channel();
    if !shared.jobs.spawn(async move {
        let _permit = budget.permit;
        let result = app.write_artifact(access, stream, &media_type).await;
        let _ = send.send(result);
    }) {
        return Err(ApiError::new(
            "runtime.unavailable",
            "Transfer workers stopped",
        ));
    }
    let reference = receive
        .await
        .map_err(|_| ApiError::new("http.internal", "Transfer worker failed"))??;
    respond(&shared, StatusCode::CREATED, &reference)
}
async fn download(
    State(shared): State<Arc<Shared>>,
    request: Request,
) -> Result<Response, ApiError> {
    let access = transport::access(&request);
    let command: ReadArtifactRequest =
        transport::json(request, shared.options.max_json_bytes).await?;
    let reference = command.artifact;
    if reference.bytes > shared.options.max_artifact_bytes as u64 {
        return Err(ApiError::new(
            "http.response_too_large",
            "Artifact exceeds transfer budget",
        ));
    }
    let media_type = reference
        .media_type
        .parse()
        .map_err(|_| ApiError::new("http.internal", "Invalid artifact media type"))?;
    let stream = shared.app.read_artifact(access, &reference).await?;
    let stream = futures::stream::unfold(
        (stream, reference.bytes, false),
        |(mut stream, remaining, done)| async move {
            if done {
                return None;
            }
            let item = match stream.next().await {
                Some(Ok(bytes)) if bytes.len() as u64 <= remaining => Ok(bytes),
                Some(Ok(_)) | Some(Err(_)) => Err(std::io::Error::other("artifact stream failed")),
                None if remaining == 0 => return None,
                None => Err(std::io::Error::other("artifact stream incomplete")),
            };
            let done = item.is_err();
            let remaining =
                remaining.saturating_sub(item.as_ref().map_or(0, |bytes| bytes.len() as u64));
            Some((item, (stream, remaining, done)))
        },
    );
    let mut response = Body::from_stream(stream).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, media_type);
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, reference.bytes.into());
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        header::HeaderValue::from_static("attachment"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}
