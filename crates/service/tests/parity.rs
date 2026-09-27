mod support;
use serde_json::{Value, json};
use std::sync::Arc;
use support::*;
use workflow_forge::v2::*;
use workflow_forge_reference_module::inventory::{MemoryInventory, inventory_operations};
use workflow_forge_service::{ServiceRuntime, dto::*};

#[tokio::test]
async fn c01_has_same_results_diagnostics_and_receipts_over_rust_and_http() {
    for durable in [false, true] {
        let dir = Directory::new();
        let mut builder = customer_builder();
        if durable {
            let store = dir.store(Default::default());
            builder = builder.execution_store(store.clone()).artifact_store(store);
        }
        let definition = customer();
        let service = ServiceRuntime::boot(
            builder.build().unwrap(),
            auth(),
            options(vec![definition.clone()]),
        )
        .await
        .unwrap();
        assert!(service.is_ready());
        let ready = value(
            client()
                .get(url(&service, "/health/ready"))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(ready, json!({"ready":true}));
        let catalog: CatalogResponse =
            serde_json::from_value(get(&service, "/v2/catalog", 200).await).unwrap();
        assert_eq!(catalog.capabilities.durable, durable);
        assert!(catalog.capabilities.artifact_transfers);
        let plan = service
            .application()
            .prepare(access(), definition.clone())
            .await
            .unwrap();
        let prepared: PrepareResponse = serde_json::from_value(
            post(
                &service,
                "/v2/workflows/prepare",
                json!({"definition":definition}),
                200,
            )
            .await,
        )
        .unwrap();
        assert_eq!(prepared.diagnostics.len(), plan.diagnostics().len());
        assert_eq!(prepared.diagnostics[0].code, plan.diagnostics()[0].code);
        let body = start_body(&definition, input(), durable);
        if !durable {
            let rejected = post(
                &service,
                "/v2/runs",
                json!({"workflow":body["workflow"],"input":body["input"]}),
                422,
            )
            .await;
            assert_eq!(rejected["diagnostics"][0]["code"], "capability.unsupported");
        }
        let receipt: StartReceipt =
            serde_json::from_value(post(&service, "/v2/runs", body.clone(), 202).await).unwrap();
        assert_eq!(receipt.durable, durable);
        let done = wait(&service, receipt.run_id.clone()).await;
        assert_eq!(done.state, RunState::Succeeded);
        let http = get(
            &service,
            &format!("/v2/runs/{}/result", receipt.run_id.0),
            200,
        )
        .await;
        assert_eq!(Some(http["output"].clone()), done.output);
        let duplicate: StartReceipt =
            serde_json::from_value(post(&service, "/v2/runs", body.clone(), 202).await).unwrap();
        assert_eq!(duplicate.run_id, receipt.run_id);
        assert!(duplicate.duplicate);
        let mut changed = body.clone();
        changed["input"]["customer"] = json!("C-10");
        assert_eq!(
            post(&service, "/v2/runs", changed, 409).await["diagnostics"][0]["code"],
            "state.conflict"
        );
        let metadata = get(&service, &format!("/v2/runs/{}", receipt.run_id.0), 200).await;
        let parsed: RunStatus = serde_json::from_value(metadata.clone()).unwrap();
        assert_eq!(parsed.state, done.state);
        assert_eq!(parsed.invocation_count, 2);
        assert!(
            metadata.get("input").is_none()
                && metadata.get("output").is_none()
                && metadata.get("package").is_none()
        );
        let mut invalid = input();
        invalid["items"][0]["quantity"] = json!(0);
        let rust_error = service
            .application()
            .start(access(), StartRunRequest::new(plan, invalid.clone()))
            .await
            .unwrap_err();
        let http_error: ForgeError = serde_json::from_value(
            post(
                &service,
                "/v2/runs",
                start_body(&definition, invalid, durable),
                422,
            )
            .await,
        )
        .unwrap();
        assert_eq!(http_error.code(), rust_error.code());
        assert_eq!(
            http_error.diagnostics[0].location,
            rust_error.diagnostics[0].location
        );
        let report = service.shutdown().await.unwrap();
        assert!(!report.http_forced && !report.engine.forced);
    }
}

#[tokio::test]
async fn c02_upload_execute_download_matches_rust_on_memory_and_sqlite() {
    for durable in [false, true] {
        let dir = Directory::new();
        let destination = Arc::new(MemoryInventory::default());
        let mut builder = WorkflowBuilder::standard();
        if durable {
            let store = dir.store(Default::default());
            builder = builder.execution_store(store.clone()).artifact_store(store);
        }
        builder
            .register_bundle(inventory_operations(destination.clone()))
            .unwrap();
        let definition: WorkflowDefinition = serde_json::from_str(include_str!(
            "../../../examples/workflows/inventory_import.v2.json"
        ))
        .unwrap();
        let service = ServiceRuntime::boot(
            builder.build().unwrap(),
            auth(),
            options(vec![definition.clone()]),
        )
        .await
        .unwrap();
        let csv = "sku,quantity\nA-1,2\nB-2,error\n\"C,3\",4\n";
        let reference: ArtifactRef = serde_json::from_value(
            value(
                client()
                    .post(url(&service, "/v2/artifacts"))
                    .bearer_auth(TOKEN)
                    .header("content-type", "text/csv")
                    .body(csv)
                    .send()
                    .await
                    .unwrap(),
                201,
            )
            .await,
        )
        .unwrap();
        let mut body = start_body(&definition, json!({"source":reference}), durable);
        body["options"]["artifacts"] = json!([reference]);
        let receipt: StartReceipt =
            serde_json::from_value(post(&service, "/v2/runs", body, 202).await).unwrap();
        let run = wait(&service, receipt.run_id.clone()).await;
        assert_eq!(run.state, RunState::Succeeded, "{:?}", run.error);
        let output = get(
            &service,
            &format!("/v2/runs/{}/result", receipt.run_id.0),
            200,
        )
        .await["output"]
            .clone();
        assert_eq!(output["rows"], 3);
        assert_eq!(output["succeeded"], 2);
        assert_eq!(output["failed"], 1);
        let response = client()
            .post(url(&service, "/v2/artifacts/read"))
            .bearer_auth(TOKEN)
            .json(&json!({"artifact":output["report"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-disposition"], "attachment");
        let text = response.text().await.unwrap();
        let rows: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1]["status"], "failed");
        assert_eq!(rows[2]["output"]["sku"], "C,3");
        // Execute the same definition through the public Rust facade, using the
        // same provider bundle, and compare the business report by row.
        let app = service.application();
        let plan = app.prepare(access(), definition).await.unwrap();
        let mut command = StartRunRequest::new(plan, json!({"source":reference}));
        command.options.artifacts.push(reference);
        command.options.require_durable = durable;
        let rust = app.start(access(), command).await.unwrap();
        let rust = wait(&service, rust.run_id).await;
        let report: ArtifactRef =
            serde_json::from_value(rust.output.unwrap()["report"].clone()).unwrap();
        use futures::StreamExt;
        let chunks = app
            .read_artifact(access(), &report)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let bytes: Vec<u8> = chunks.into_iter().flat_map(Result::unwrap).collect();
        assert_eq!(String::from_utf8(bytes).unwrap(), text);
        assert_eq!(destination.snapshot().unwrap().effects.len(), 4);
        service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn signal_receipts_and_wait_views_use_the_public_contract() {
    let definition = single(
        "signal",
        json!({"id":"callback","kind":"await_signal","input":{"literal":null},"correlation":{"literal":"job-1"},"timeout_ms":60000,"payload_schema":{"type":"integer"}}),
    );
    let service = ServiceRuntime::boot(
        WorkflowBuilder::standard().build().unwrap(),
        auth(),
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    let receipt: StartReceipt = serde_json::from_value(
        post(
            &service,
            "/v2/runs",
            start_body(&definition, json!(null), false),
            202,
        )
        .await,
    )
    .unwrap();
    state(&service, &receipt.run_id, RunState::Waiting).await;
    let path = format!("/v2/runs/{}/waits", receipt.run_id.0);
    let waits: Page<WaitStatus> = serde_json::from_value(get(&service, &path, 200).await).unwrap();
    assert_eq!(waits.items.len(), 1);
    let command = json!({"run_id":receipt.run_id,"wait_id":waits.items[0].id,"message_id":"callback-1","correlation":"job-1","payload":7});
    let path = format!("/v2/runs/{}/signals", receipt.run_id.0);
    let result = post(&service, &path, command.clone(), 200).await;
    assert_eq!(
        wait(&service, receipt.run_id.clone()).await.output,
        Some(json!({"signal":7,"start":null}))
    );
    let mut duplicate = post(&service, &path, command.clone(), 200).await;
    assert_eq!(duplicate["duplicate"], true);
    duplicate["duplicate"] = json!(false);
    assert_eq!(duplicate, result);
    let mut wrong = command;
    wrong["run_id"] = json!("another");
    assert_eq!(
        post(&service, &path, wrong, 400).await["diagnostics"][0]["code"],
        "http.invalid_request"
    );
    service.shutdown().await.unwrap();
}
