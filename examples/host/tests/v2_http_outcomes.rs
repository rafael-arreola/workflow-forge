#![cfg(feature = "integrations")]
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use workflow_forge::prelude::*;

#[tokio::test]
async fn http_statuses_are_workflow_data_and_invalid_json_is_a_typed_error() {
    for (status, body, content_type, expected_error) in [
        (200, "{\"ok\":true}", "application/json", None),
        (404, "{\"reason\":\"missing\"}", "application/json", None),
        (429, "{\"retry\":true}", "application/json", None),
        (500, "{\"reason\":\"failure\"}", "application/json", None),
        (204, "", "", None),
        (
            200,
            "invalid",
            "application/json",
            Some("http.invalid_json"),
        ),
        (404, "missing", "text/plain", Some("http.content_type")),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            let mut received = 0;
            while !bytes[..received].windows(4).any(|w| w == b"\r\n\r\n") {
                let count = socket.read(&mut bytes[received..]).await.unwrap();
                assert!(
                    count > 0,
                    "fixture expected complete bounded request headers"
                );
                received += count;
            }
            let response = format!(
                "HTTP/1.1 {status} Response\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let bundle = modules::http_json_operations(vec![modules::HttpJsonProfile {
            name: "fixture".into(),
            url: format!("http://{address}/"),
            ..Default::default()
        }])
        .unwrap();
        let revision = bundle.operations[0].descriptor().revision.clone();
        assert_eq!(revision.contract, "2");
        let mut builder = WorkflowBuilder::standard();
        builder.register_bundle(bundle).unwrap();
        let runtime = EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
            .await
            .unwrap();
        let app = runtime.application();
        let access = AccessContext::trusted("default");
        let definition = serde_json::from_value(json!({
            "format":WORKFLOW_FORMAT,"id":"http","revision":"r1","schema_dialect":SCHEMA_DIALECT,
            "input_schema":true,"output_schema":true,"entry":"request","edges":[],
            "nodes":[{"id":"request","kind":"operation","operation":revision,"config":{},"input":{"literal":{}}}],
            "output":{"select":{"source":"node","node":"request","pointer":""}}
        })).unwrap();
        let plan = app.prepare(access.clone(), definition).await.unwrap();
        let result = app
            .execute(
                access,
                StartRunRequest::new(plan, json!(null)),
                CancellationToken::new(),
            )
            .await;
        if let Some(code) = expected_error {
            assert_eq!(
                result.unwrap_err().diagnostics[0]
                    .operation_error
                    .as_ref()
                    .unwrap()
                    .code,
                code
            );
        } else {
            let value = result.unwrap();
            assert_eq!(value["status"], status);
            if status == 204 {
                assert!(value["body"].is_null());
            }
        }
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
        peer.await.unwrap();
    }
}
