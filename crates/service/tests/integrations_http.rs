mod support;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use support::*;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use workflow_forge::v2::*;
use workflow_forge_service::ServiceRuntime;

#[derive(Clone)]
struct RequestRecord {
    path: String,
    method: Method,
    query: String,
    authorization: Option<String>,
    body: Vec<u8>,
}
#[derive(Default)]
struct Endpoint {
    calls: Mutex<Vec<RequestRecord>>,
}
struct Upstream {
    base: String,
    state: Arc<Endpoint>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}
impl Upstream {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Endpoint::default());
        let stop = CancellationToken::new();
        let router = Router::new()
            .route("/{path}", any(endpoint))
            .with_state(state.clone());
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            base,
            state,
            stop,
            task,
        }
    }
    async fn shutdown(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
async fn endpoint(
    State(state): State<Arc<Endpoint>>,
    uri: Uri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.calls.lock().unwrap().push(RequestRecord {
        path: uri.path().into(),
        method,
        query: uri.query().unwrap_or_default().into(),
        authorization: headers
            .get("authorization")
            .map(|h| h.to_str().unwrap().into()),
        body: body.to_vec(),
    });
    match uri.path() {
        "/redirect" => (StatusCode::FOUND, [("location", "/lookup")], "redirect").into_response(),
        "/lost" => (
            [("content-type", "application/json")],
            Body::from_stream(futures::stream::once(async {
                Err::<Vec<u8>, _>(std::io::Error::other("lost reply"))
            })),
        )
            .into_response(),
        "/large" => (
            [("content-type", "application/json")],
            Body::from_stream(futures::stream::iter([
                Ok::<_, std::io::Error>(vec![b'a'; 512]),
                Ok(vec![b'b'; 513]),
            ])),
        )
            .into_response(),
        "/status" => (StatusCode::SERVICE_UNAVAILABLE, "private upstream body").into_response(),
        "/slow" => {
            tokio::time::sleep(Duration::from_millis(100)).await;
            axum::Json(json!({"applied":true})).into_response()
        }
        "/invalid" => (
            [("content-type", "application/json")],
            "private invalid JSON",
        )
            .into_response(),
        "/empty" => StatusCode::NO_CONTENT.into_response(),
        _ => axum::Json(json!({"active":true})).into_response(),
    }
}
fn profile(
    upstream: &Upstream,
    name: &str,
    method: modules::HttpMethod,
) -> modules::HttpJsonProfile {
    modules::HttpJsonProfile {
        name: name.into(),
        url: format!("{}/{}", upstream.base, name),
        method,
        ..Default::default()
    }
}
fn operation_definition(revision: OperationRevision) -> WorkflowDefinition {
    single(
        &revision.id,
        json!({"id":"call","kind":"operation","operation":revision,"config":{},"input":{"select":{"source":"input","pointer":""}},"retry":{"max_attempts":2,"initial_delay_ms":0,"max_delay_ms":0,"jitter":false}}),
    )
}
struct Observed {
    inner: Arc<dyn Operation>,
    calls: Arc<Mutex<Vec<(String, String, Value)>>>,
}
impl Operation for Observed {
    fn descriptor(&self) -> &OperationDescriptor {
        self.inner.descriptor()
    }
    fn execute<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> OperationFuture<'a> {
        self.calls.lock().unwrap().push((
            invocation.id.clone(),
            invocation.attempt_id.clone(),
            invocation.input.clone(),
        ));
        self.inner.execute(context, invocation)
    }
}

#[tokio::test]
async fn shared_http_client_and_decorator_keep_contract_and_invocations_independent() {
    let upstream = Upstream::boot().await;
    let mut profile = profile(&upstream, "lookup", modules::HttpMethod::Get);
    profile.bearer_secret = Some("crm".into());
    let mut bundle = modules::http_json_operations(vec![profile.clone()]).unwrap();
    let revision = bundle.operations[0].descriptor().revision.clone();
    assert_eq!(
        revision,
        modules::http_json_operations(vec![profile.clone()])
            .unwrap()
            .operations[0]
            .descriptor()
            .revision
    );
    let mut changed = profile;
    changed.url.push_str("/v2");
    assert_ne!(
        revision,
        modules::http_json_operations(vec![changed])
            .unwrap()
            .operations[0]
            .descriptor()
            .revision
    );
    let observed = Arc::new(Mutex::new(Vec::new()));
    bundle.operations[0] = Arc::new(Observed {
        inner: bundle.operations[0].clone(),
        calls: observed.clone(),
    });
    let mut secrets = modules::MemorySecrets::default();
    secrets.insert("default", "crm", "fixture-bearer-value".into());
    let mut builder = WorkflowBuilder::standard().secret_provider(Arc::new(secrets));
    builder.register_bundle(bundle).unwrap();
    let definition = operation_definition(revision.clone());
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    assert!(upstream.state.calls.lock().unwrap().is_empty());
    let starts = (0..16).map(|index| {
        let mut body = start_body(
            &definition,
            json!({"query":{"customer":format!("C-{index}")}}),
            false,
        );
        body["options"]["receipt_key"] = json!(format!("request-{index}"));
        let service = &service;
        async move {
            serde_json::from_value::<StartReceipt>(post(service, "/v2/runs", body, 202).await)
                .unwrap()
        }
    });
    for receipt in futures::future::join_all(starts).await {
        assert_eq!(
            wait(&service, receipt.run_id).await.output,
            Some(json!({"status":200,"body":{"active":true}}))
        );
    }
    {
        let calls = observed.lock().unwrap();
        assert_eq!(calls.len(), 16);
        assert_eq!(
            calls
                .iter()
                .map(|c| &c.0)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            16
        );
        assert_eq!(
            calls
                .iter()
                .map(|c| &c.1)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            16
        );
    }
    {
        let requests = upstream.state.calls.lock().unwrap();
        assert_eq!(requests.len(), 16);
        assert!(requests.iter().all(|r| r.method == Method::GET
            && r.authorization.as_deref() == Some("Bearer fixture-bearer-value")
            && r.body.is_empty()));
        assert_eq!(
            requests
                .iter()
                .map(|r| &r.query)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            16
        );
    }
    let mut invalid = start_body(
        &definition,
        json!({"url":"http://unapproved.example","query":{}}),
        false,
    );
    invalid["options"]["receipt_key"] = json!("invalid-target-override");
    // This workflow deliberately has input_schema=true. The operation's stricter
    // schema rejects the override at invocation time, before any HTTP dispatch.
    let invalid: StartReceipt =
        serde_json::from_value(post(&service, "/v2/runs", invalid, 202).await).unwrap();
    let rejected = wait(&service, invalid.run_id).await;
    assert_eq!(rejected.state, RunState::Failed);
    assert_eq!(rejected.error.unwrap().code(), "data.invalid");
    assert_eq!(upstream.state.calls.lock().unwrap().len(), 16);
    service.shutdown().await.unwrap();
    upstream.shutdown().await;
}

#[tokio::test]
async fn redirects_and_chunked_response_budgets_are_enforced_without_transport_retries() {
    let upstream = Upstream::boot().await;
    let profiles = ["redirect", "large", "status", "invalid", "empty"]
        .into_iter()
        .map(|name| {
            let mut profile = profile(&upstream, name, modules::HttpMethod::Get);
            profile.max_response_bytes = 1024;
            profile
        })
        .collect();
    let bundle = modules::http_json_operations(profiles).unwrap();
    let definitions: Vec<_> = bundle
        .operations
        .iter()
        .map(|o| operation_definition(o.descriptor().revision.clone()))
        .collect();
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(bundle).unwrap();
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(definitions.clone()),
    )
    .await
    .unwrap();
    for (index, definition) in definitions.iter().enumerate() {
        let mut body = start_body(definition, json!({}), false);
        body["options"]["receipt_key"] = json!(definition.id);
        let receipt: StartReceipt =
            serde_json::from_value(post(&service, "/v2/runs", body, 202).await).unwrap();
        let run = wait(&service, receipt.run_id).await;
        if index == 4 {
            assert_eq!(run.output, Some(json!({"status":204,"body":null})));
        } else {
            assert_eq!(run.state, RunState::Failed);
            assert!(
                !serde_json::to_string(&run.error)
                    .unwrap()
                    .contains("private upstream")
            );
            let wanted = [
                "http.status",
                "resource.limit",
                "http.status",
                "http.invalid_json",
            ][index];
            assert_eq!(
                run.error.unwrap().diagnostics[0]
                    .operation_error
                    .as_ref()
                    .unwrap()
                    .code,
                wanted
            );
        }
    }
    {
        let requests = upstream.state.calls.lock().unwrap();
        assert!(!requests.iter().any(|r| r.path == "/lookup"));
        assert_eq!(requests.iter().filter(|r| r.path == "/redirect").count(), 1);
        // GET 503 can be retried by the engine's explicit policy: exactly two
        // attempts, with no extra retries hidden inside the client.
        assert_eq!(requests.iter().filter(|r| r.path == "/status").count(), 2);
    }
    service.shutdown().await.unwrap();
    upstream.shutdown().await;
}

#[tokio::test]
async fn uncertain_http_writes_never_repeat_automatically_after_dispatch() {
    let upstream = Upstream::boot().await;
    let profiles = ["lost", "slow", "status"]
        .into_iter()
        .map(|name| {
            let mut profile = profile(&upstream, name, modules::HttpMethod::Post);
            if name == "slow" {
                profile.timeout_ms = 25;
            }
            profile
        })
        .collect();
    let bundle = modules::http_json_operations(profiles).unwrap();
    let definitions: Vec<_> = bundle
        .operations
        .iter()
        .map(|o| operation_definition(o.descriptor().revision.clone()))
        .collect();
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(bundle).unwrap();
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(definitions.clone()),
    )
    .await
    .unwrap();
    for definition in definitions {
        let mut body = start_body(&definition, json!({"body":{"value":1}}), false);
        body["options"]["receipt_key"] = json!(definition.id);
        let receipt: StartReceipt =
            serde_json::from_value(post(&service, "/v2/runs", body, 202).await).unwrap();
        let blocked = state(&service, &receipt.run_id, RunState::Blocked).await;
        let invocation = blocked.invocations.values().next().unwrap();
        assert_eq!(invocation.attempts, 1);
        assert_eq!(invocation.certainty, EffectCertainty::Unknown);
        service
            .application()
            .reconcile(
                access(),
                ReconcileCommand {
                    command_id: "stop".into(),
                    run_id: receipt.run_id.clone(),
                    invocation_id: invocation.id.clone(),
                    expected_revision: blocked.revision,
                    observed_attempt: invocation.attempt_id.clone(),
                    resolution: EffectResolution::StopTracking {
                        reason: "Fixture observes an uncertain HTTP write".into(),
                        evidence: None,
                    },
                },
            )
            .await
            .unwrap();
        wait(&service, receipt.run_id).await;
    }
    {
        let requests = upstream.state.calls.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|r| r.method == Method::POST
            && serde_json::from_slice::<Value>(&r.body).unwrap() == json!({"value":1})));
    }
    service.shutdown().await.unwrap();
    upstream.shutdown().await;
}
