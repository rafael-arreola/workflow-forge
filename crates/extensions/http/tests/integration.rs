use std::sync::Arc;

use httpmock::prelude::*;
use serde_json::{Value, json};
use workflow_forge_core::executor::WorkflowExecutor;
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::task::WorkflowData;
use workflow_forge_core::workflow::WorkflowDefinition;

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    workflow_forge_ext_http::register(&registry);
    registry
}

async fn run(
    workflow: Value,
    trigger: Value,
) -> Result<WorkflowData, workflow_forge_core::error::WorkflowError> {
    let workflow: WorkflowDefinition = serde_json::from_value(workflow).unwrap();
    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    executor.run(WorkflowData(trigger)).await
}

fn single_request_workflow(input: Value) -> Value {
    json!({
        "name": "http", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "req", "kind": "task", "task": "http.request", "input": input },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "req" },
            { "from": "req", "to": "end" }
        ]
    })
}

#[tokio::test]
async fn post_json_con_auth_y_query() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/users")
                .query_param("page", "1")
                .header("authorization", "Bearer tok-123")
                .json_body(json!({ "name": "ada" }));
            then.status(201)
                .header("content-type", "application/json")
                .json_body(json!({ "id": 7, "name": "ada" }));
        })
        .await;

    let result = run(
        single_request_workflow(json!({
            "url": format!("{}/users", server.base_url()),
            "method": "POST",
            "query": { "page": "1" },
            "body": { "name": "$.trigger.nombre" },
            "auth": { "type": "bearer", "token": "tok-123" }
        })),
        json!({ "nombre": "ada" }),
    )
    .await
    .unwrap();

    mock.assert_async().await;
    assert_eq!(result.0["status"], 201);
    assert_eq!(result.0["body"], json!({ "id": 7, "name": "ada" }));
}

#[tokio::test]
async fn status_de_error_es_dato_por_default() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/falla");
            then.status(503).body("mantenimiento");
        })
        .await;

    let result = run(
        single_request_workflow(json!({ "url": format!("{}/falla", server.base_url()) })),
        json!({}),
    )
    .await
    .unwrap();

    assert_eq!(result.0["status"], 503);
    assert_eq!(result.0["body"], json!("mantenimiento"));
}

#[tokio::test]
async fn fail_on_error_status_permite_retry() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/inestable");
            then.status(500);
        })
        .await;

    let workflow = json!({
        "name": "retry", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "req", "kind": "task", "task": "http.request",
              "input": {
                  "url": format!("{}/inestable", server.base_url()),
                  "fail_on_error_status": true
              },
              "retry": { "max": 2, "backoff": "fixed", "initial_ms": 1 } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "req" },
            { "from": "req", "to": "end" }
        ]
    });

    let err = run(workflow, json!({})).await.unwrap_err();
    assert_eq!(err.code, "HTTP_STATUS_ERROR");
    // 1 intento + 2 reintentos
    assert_eq!(mock.hits_async().await, 3);
}
