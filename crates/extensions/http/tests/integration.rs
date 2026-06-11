use std::sync::Arc;

use async_trait::async_trait;
use httpmock::prelude::*;
use serde_json::{Value, json};
use workflow_forge_core::io::blob::BlobRef;
use workflow_forge_core::runtime::{WorkflowContext, WorkflowExecutor};
use workflow_forge_core::spec::WorkflowDefinition;
use workflow_forge_core::task::{Task, TaskManifest, TaskRegistry, WorkflowData, WorkflowResult};

/// Crea un blob en el store de la ejecución a partir de bytes JSON
/// (`{ "content": [u8...] | "string", "name": "..." }` → `{ "file": BlobRef }`)
struct MakeBlobTask(TaskManifest);

#[async_trait]
impl Task for MakeBlobTask {
    fn manifest(&self) -> &TaskManifest {
        &self.0
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let bytes = match &input.0["content"] {
            Value::String(s) => s.clone().into_bytes(),
            Value::Array(nums) => nums
                .iter()
                .map(|n| n.as_u64().unwrap() as u8)
                .collect::<Vec<u8>>(),
            other => panic!("content inesperado: {other}"),
        };
        let name = input.0["name"].as_str().map(str::to_string);
        let blob = ctx.blobs().put(bytes, name).await?;
        Ok(WorkflowData(
            json!({ "file": serde_json::to_value(&blob).unwrap() }),
        ))
    }
}

/// Lee un blob del store (`{ "file": BlobRef }` → `{ "bytes": [u8...], "name": ... }`)
struct ReadBlobTask(TaskManifest);

#[async_trait]
impl Task for ReadBlobTask {
    fn manifest(&self) -> &TaskManifest {
        &self.0
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let blob = BlobRef::from_value(&input.0["file"]).expect("BlobRef en input.file");
        let bytes = ctx.blobs().get(&blob).await?;
        Ok(WorkflowData(json!({ "bytes": bytes, "name": blob.name })))
    }
}

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    workflow_forge_ext_http::register(&registry);
    registry.register(MakeBlobTask(TaskManifest::new("test.make_blob")));
    registry.register(ReadBlobTask(TaskManifest::new("test.read_blob")));
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
    assert_eq!(mock.calls_async().await, 3);
}

// ---------------------------------------------------------------------------
// Cuerpos: form, text, body_blob, multipart
// ---------------------------------------------------------------------------

#[tokio::test]
async fn form_urlencoded() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/token")
                .header("content-type", "application/x-www-form-urlencoded")
                .form_urlencoded_tuple("grant_type", "client_credentials")
                .form_urlencoded_tuple("scope", "envios");
            then.status(200);
        })
        .await;

    run(
        single_request_workflow(json!({
            "url": format!("{}/token", server.base_url()),
            "method": "POST",
            "form": { "grant_type": "client_credentials", "scope": "envios" }
        })),
        json!({}),
    )
    .await
    .unwrap();
    mock.assert_async().await;
}

#[tokio::test]
async fn text_raw_con_content_type_custom() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/xml")
                .header("content-type", "application/xml")
                .body("<orden id=\"7\"/>");
            then.status(200);
        })
        .await;

    run(
        single_request_workflow(json!({
            "url": format!("{}/xml", server.base_url()),
            "method": "POST",
            "headers": { "content-type": "application/xml" },
            "text": "<orden id=\"7\"/>"
        })),
        json!({}),
    )
    .await
    .unwrap();
    mock.assert_async().await;
}

#[tokio::test]
async fn body_blob_sube_el_binario_streamed() {
    let server = MockServer::start_async().await;
    let contenido = "col1,col2\nñandú,2\n";
    let mock = server
        .mock_async(|when, then| {
            when.method(PUT)
                .path("/archivo")
                .header("content-type", "application/octet-stream")
                .body(contenido);
            then.status(201);
        })
        .await;

    let workflow = json!({
        "name": "sube-blob", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "haz_blob", "kind": "task", "task": "test.make_blob",
              "input": { "content": "$.trigger.contenido", "name": "datos.csv" } },
            { "id": "req", "kind": "task", "task": "http.request",
              "input": {
                  "url": format!("{}/archivo", server.base_url()),
                  "method": "PUT",
                  "body_blob": "$.nodes.haz_blob.output.file"
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "haz_blob" },
            { "from": "haz_blob", "to": "req" },
            { "from": "req", "to": "end" }
        ]
    });
    let result = run(workflow, json!({ "contenido": contenido }))
        .await
        .unwrap();
    mock.assert_async().await;
    assert_eq!(result.0["status"], 201);
}

#[tokio::test]
async fn multipart_con_texto_json_y_blob() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/upload")
                .header_includes("content-type", "multipart/form-data")
                .body_includes("name=\"nota\"")
                .body_includes("texto plano")
                .body_includes("name=\"meta\"")
                .body_includes("{\"orden\":123}")
                .body_includes("name=\"archivo\"")
                .body_includes("filename=\"ventas.csv\"")
                .body_includes("Content-Type: text/csv")
                .body_includes("sku,qty\nA1,2");
            then.status(201);
        })
        .await;

    let workflow = json!({
        "name": "multipart", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "haz_blob", "kind": "task", "task": "test.make_blob",
              "input": { "content": "sku,qty\nA1,2", "name": "ventas.csv" } },
            { "id": "req", "kind": "task", "task": "http.request",
              "input": {
                  "url": format!("{}/upload", server.base_url()),
                  "method": "POST",
                  "multipart": {
                      "nota": "texto plano",
                      "meta": { "json": { "orden": 123 } },
                      "archivo": {
                          "blob": "$.nodes.haz_blob.output.file",
                          "filename": "ventas.csv",
                          "content_type": "text/csv"
                      }
                  }
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "haz_blob" },
            { "from": "haz_blob", "to": "req" },
            { "from": "req", "to": "end" }
        ]
    });
    let result = run(workflow, json!({})).await.unwrap();
    mock.assert_async().await;
    assert_eq!(result.0["status"], 201);
}

// ---------------------------------------------------------------------------
// Respuesta a blob
// ---------------------------------------------------------------------------

#[tokio::test]
async fn response_body_blob_descarga_el_binario_sin_corromperlo() {
    let server = MockServer::start_async().await;
    let contenido: Vec<u8> = vec![0x25, 0x50, 0x44, 0x46, 0x00, 0xFE, 0xFF, 0x07];
    {
        let contenido = contenido.clone();
        server
            .mock_async(move |when, then| {
                when.method(GET).path("/reporte");
                then.status(200)
                    .header("content-type", "application/pdf")
                    .header(
                        "content-disposition",
                        "attachment; filename=\"reporte.pdf\"",
                    )
                    .body(contenido.clone());
            })
            .await;
    }

    let workflow = json!({
        "name": "descarga", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "req", "kind": "task", "task": "http.request",
              "input": {
                  "url": format!("{}/reporte", server.base_url()),
                  "response_body": "blob"
              } },
            { "id": "lee", "kind": "task", "task": "test.read_blob",
              "input": { "file": "$.nodes.req.output.body" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "req" },
            { "from": "req", "to": "lee" },
            { "from": "lee", "to": "end" }
        ]
    });
    let result = run(workflow, json!({})).await.unwrap();
    assert_eq!(result.0["bytes"], json!(contenido));
    assert_eq!(result.0["name"], json!("reporte.pdf"));
}

#[tokio::test]
async fn response_body_text_fuerza_string_aunque_sea_json() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/raw");
            then.status(200)
                .header("content-type", "application/json")
                .body("{\"a\":1}");
        })
        .await;

    let result = run(
        single_request_workflow(json!({
            "url": format!("{}/raw", server.base_url()),
            "response_body": "text"
        })),
        json!({}),
    )
    .await
    .unwrap();
    assert_eq!(result.0["body"], json!("{\"a\":1}"));
}

// ---------------------------------------------------------------------------
// Exclusión mutua
// ---------------------------------------------------------------------------

#[tokio::test]
async fn los_cuerpos_son_mutuamente_excluyentes() {
    let err = run(
        single_request_workflow(json!({
            "url": "http://localhost/imposible",
            "body": { "a": 1 },
            "text": "hola"
        })),
        json!({}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "HTTP_INPUT_INVALID");
}
