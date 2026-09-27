mod support;
use serde_json::{Value, json};
use std::{process::Stdio, sync::atomic::Ordering, time::Duration};
use support::*;
use tokio::io::AsyncWriteExt;
use workflow_forge::v2::*;

#[tokio::test]
async fn paginated_catalog_and_validation_preserve_contracts_without_executing() {
    let operations: Vec<_> = (0..65)
        .map(|i| Probe::new(&format!("test.op{i}"), Outcome::Echo))
        .collect();
    let service = boot(builder(operations.clone()), vec![]).await;
    let server = url(&service);
    let directory = Directory::new();
    let catalog = success(execute(&server, &["catalog"]).await);
    let items = catalog["items"].as_array().unwrap();
    assert!(items.len() > 65);
    for op in &operations {
        assert_eq!(
            items
                .iter()
                .filter(|item| item["revision"] == json!(op.descriptor.revision))
                .count(),
            1
        );
    }
    assert!(catalog["next_cursor"].is_null());
    let definition = echo("test.op0");
    let path = directory.json("workflow.json", &definition);
    let prepared = success(execute(&server, &["validate", &path]).await);
    assert_eq!(
        prepared["workflow"],
        json!({"id":definition.id,"revision":"r1"})
    );
    assert!(
        operations
            .iter()
            .all(|op| op.calls.load(Ordering::SeqCst) == 0)
    );
    let invalid = directory.json("invalid.json", &echo("missing.operation"));
    let output = execute(&server, &["validate", &invalid]).await;
    assert_eq!(output.status.code(), Some(1));
    let error: ForgeError = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error.diagnostics[0].location.node.as_deref(), Some("echo"));
    assert_eq!(error.diagnostics[0].location.field, "/operation");
    let mut denied = command(&server, &["catalog"]);
    denied.env("FORGE_CLI_TEST_TOKEN", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    failure(denied.output().await.unwrap(), 1, "http.unauthenticated");
    assert!(
        operations
            .iter()
            .all(|op| op.calls.load(Ordering::SeqCst) == 0)
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_receipts_survive_clients_and_service_restart_without_repeating_work() {
    let directory = Directory::new();
    let store = directory.sqlite();
    let probe = Probe::new("test.echo", Outcome::Echo);
    let definition = echo("test.echo");
    let path = directory.json("workflow.json", &definition);
    let service = boot(
        builder(vec![probe.clone()])
            .execution_store(store.clone())
            .artifact_store(store.clone()),
        vec![],
    )
    .await;
    let server = url(&service);
    let input = json!({"customer":"C-9","nested":[null,1,"UTF-8 á"]});
    let mut process = command(&server, &["run", &path, "--receipt-key", "receive-1"]);
    process.stdin(Stdio::piped());
    let mut child = process.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .await
        .unwrap();
    let output = child.wait_with_output().await.unwrap();
    let receipt: StartReceipt = serde_json::from_value(
        serde_json::from_slice::<Value>(&output.stderr).unwrap()["accepted"].clone(),
    )
    .unwrap();
    assert_eq!(success(output), input);
    assert!(receipt.durable);
    assert!(!receipt.duplicate);
    service.shutdown().await.unwrap();

    let service = boot(
        builder(vec![probe.clone()])
            .execution_store(store.clone())
            .artifact_store(store),
        vec![definition.clone()],
    )
    .await;
    let server = url(&service);
    let input_path = format!("@{}", directory.json("input.json", &input));
    let duplicate: StartReceipt = serde_json::from_value(success(
        execute(
            &server,
            &[
                "start",
                &definition.id,
                "r1",
                "--input",
                &input_path,
                "--receipt-key",
                "receive-1",
            ],
        )
        .await,
    ))
    .unwrap();
    assert_eq!(duplicate.run_id, receipt.run_id);
    assert!(duplicate.duplicate && duplicate.durable);
    assert_eq!(
        success(execute(&server, &["result", &receipt.run_id.0]).await)["output"],
        input
    );
    assert_eq!(
        success(execute(&server, &["status", &receipt.run_id.0]).await)["state"],
        "succeeded"
    );
    assert_eq!(
        success(execute(&server, &["wait", &receipt.run_id.0]).await),
        input
    );
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn ephemeral_opt_in_wait_timeout_signal_and_explicit_cancel_are_independent() {
    let definition = approval();
    let service = boot(WorkflowBuilder::standard(), vec![definition.clone()]).await;
    let server = url(&service);
    let args = [
        "start",
        &definition.id,
        "r1",
        "--input",
        "{\"order_id\":\"A-1\"}",
    ];
    let rejected = execute(&server, &args).await;
    assert_eq!(rejected.status.code(), Some(1));
    let error: ForgeError = serde_json::from_slice(&rejected.stderr).unwrap();
    assert_eq!(error.code(), "capability.unsupported");
    let mut args = args.to_vec();
    args.push("--ephemeral");
    let receipt: StartReceipt =
        serde_json::from_value(success(execute(&server, &args).await)).unwrap();
    assert!(!receipt.durable);
    state(&service, &receipt.run_id, RunState::Waiting).await;
    let waiting = success(execute(&server, &["waits", &receipt.run_id.0, "--limit", "1"]).await);
    assert_eq!(waiting["items"].as_array().unwrap().len(), 1);
    failure(
        execute(
            &server,
            &[
                "wait",
                &receipt.run_id.0,
                "--wait-timeout-ms",
                "20",
                "--poll-ms",
                "2",
            ],
        )
        .await,
        3,
        "cli.wait_expired",
    );
    assert!(
        !state(&service, &receipt.run_id, RunState::Waiting)
            .await
            .cancel_requested
    );
    let directory = Directory::new();
    let path = directory.json(
        "signal.json",
        &SignalCommand {
            run_id: receipt.run_id.clone(),
            wait_id: waiting["items"][0]["id"].as_str().unwrap().into(),
            message_id: "approval-1".into(),
            correlation: "A-1".into(),
            payload: json!({"approved":true}),
            artifacts: vec![],
        },
    );
    let signal = success(execute(&server, &["signal", &path]).await);
    assert_eq!(signal["message_id"], "approval-1");
    assert_eq!(
        success(execute(&server, &["wait", &receipt.run_id.0]).await),
        json!({"signal":{"approved":true},"start":null})
    );
    assert_eq!(
        success(execute(&server, &["signal", &path]).await)["duplicate"],
        true
    );

    let other: StartReceipt =
        serde_json::from_value(success(execute(&server, &args).await)).unwrap();
    state(&service, &other.run_id, RunState::Waiting).await;
    success(execute(&server, &["cancel", &other.run_id.0]).await);
    let cancelled = state(&service, &other.run_id, RunState::Cancelled).await;
    assert!(cancelled.cancel_requested);
    assert_eq!(
        execute(&server, &["wait", &other.run_id.0])
            .await
            .status
            .code(),
        Some(1)
    );
    service.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn interrupting_an_accepted_client_does_not_cancel_the_remote_run() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
    let directory = Directory::new();
    let path = directory.json("approval.json", &approval());
    let service = boot(WorkflowBuilder::standard(), vec![]).await;
    let mut child = command(
        &url(&service),
        &[
            "run",
            &path,
            "--ephemeral",
            "--input",
            "{\"order_id\":\"INT-1\"}",
        ],
    )
    .spawn()
    .unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), stderr.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let receipt: StartReceipt =
        serde_json::from_value(serde_json::from_str::<Value>(&line).unwrap()["accepted"].clone())
            .unwrap();
    assert!(
        tokio::process::Command::new("/bin/kill")
            .args(["-s", "INT", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    let mut errors = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stderr.read_to_end(&mut errors))
        .await
        .unwrap()
        .unwrap();
    let mut output = child.wait_with_output().await.unwrap();
    output.stderr = errors;
    failure(output, 3, "cli.interrupted");
    assert!(
        !state(&service, &receipt.run_id, RunState::Waiting)
            .await
            .cancel_requested
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn operation_failure_and_effect_reconciliation_preserve_the_service_decision() {
    let failed = Probe::new("test.failed", Outcome::Fail);
    let uncertain = Probe::new("test.uncertain", Outcome::Uncertain);
    let service = boot(builder(vec![failed.clone(), uncertain.clone()]), vec![]).await;
    let directory = Directory::new();
    let server = url(&service);
    let path = directory.json("failed.json", &echo("test.failed"));
    let output = execute(&server, &["run", &path, "--ephemeral", "--input", "{}"]).await;
    assert_eq!(output.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private adapter payload"));
    assert_eq!(failed.calls.load(Ordering::SeqCst), 1);
    let mut definition = echo("test.uncertain");
    definition.id = "cli.uncertain".into();
    let path = directory.json("uncertain.json", &definition);
    let receipt: StartReceipt = serde_json::from_value(success(
        execute(
            &server,
            &[
                "run",
                &path,
                "--ephemeral",
                "--input",
                "{\"output\":42}",
                "--no-wait",
            ],
        )
        .await,
    ))
    .unwrap();
    let blocked = state(&service, &receipt.run_id, RunState::Blocked).await;
    failure(
        execute(&server, &["wait", &receipt.run_id.0]).await,
        3,
        "cli.run_blocked",
    );
    let invocations = success(execute(&server, &["invocations", &receipt.run_id.0]).await);
    let record: workflow_forge_service::dto::InvocationStatus =
        serde_json::from_value(invocations["items"][0].clone()).unwrap();
    let inspected: EffectInspection = serde_json::from_value(success(
        execute(&server, &["inspect", &receipt.run_id.0, &record.id]).await,
    ))
    .unwrap();
    let EffectInspection::Applied { output, evidence } = inspected else {
        panic!("Expected ledger evidence");
    };
    let resolution = ReconcileCommand {
        command_id: "resolve-1".into(),
        run_id: receipt.run_id.clone(),
        invocation_id: record.id.clone(),
        expected_revision: blocked.revision,
        observed_attempt: record.attempt_id.clone(),
        resolution: EffectResolution::ConfirmApplied { output, evidence },
    };
    let path = directory.json("resolution.json", &resolution);
    let applied = success(execute(&server, &["reconcile", &path]).await);
    assert_eq!(
        success(execute(&server, &["reconcile", &path]).await),
        applied
    );
    assert_eq!(
        success(execute(&server, &["wait", &receipt.run_id.0]).await),
        json!({"output":42})
    );
    assert_eq!(uncertain.calls.load(Ordering::SeqCst), 1);
    let audit = success(execute(&server, &["audit", &receipt.run_id.0]).await);
    assert_eq!(audit["items"].as_array().unwrap().len(), 1);
    assert_eq!(audit["items"][0]["command_id"], "resolve-1");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn artifacts_stream_round_trip_with_explicit_references_and_no_overwrite() {
    let directory = Directory::new();
    let service = boot(
        WorkflowBuilder::standard(),
        vec![echo("forge.data.identity")],
    )
    .await;
    let server = url(&service);
    let source = directory.0.join("source.bin");
    let bytes: Vec<_> = (0..200_003).map(|i| (i % 251) as u8).collect();
    std::fs::write(&source, &bytes).unwrap();
    failure(
        execute(
            &server,
            &[
                "upload",
                source.to_str().unwrap(),
                "--max-artifact-bytes",
                "100",
            ],
        )
        .await,
        1,
        "cli.body_too_large",
    );
    let artifact: ArtifactRef = serde_json::from_value(success(
        execute(&server, &["upload", source.to_str().unwrap()]).await,
    ))
    .unwrap();
    assert_eq!(artifact.bytes, bytes.len() as u64);
    let reference = directory.json("reference.json", &artifact);
    let receipt: StartReceipt = serde_json::from_value(success(
        execute(
            &server,
            &[
                "start",
                "cli.echo",
                "r1",
                "--ephemeral",
                "--input",
                &json!(artifact).to_string(),
                "--artifact-ref",
                &reference,
            ],
        )
        .await,
    ))
    .unwrap();
    assert_eq!(
        success(execute(&server, &["wait", &receipt.run_id.0]).await),
        json!(artifact)
    );
    let output = directory.0.join("download.bin");
    let args = ["download", &reference, "--output", output.to_str().unwrap()];
    assert_eq!(success(execute(&server, &args).await)["downloaded"], true);
    assert_eq!(std::fs::read(&output).unwrap(), bytes);
    std::fs::write(&output, b"keep existing file").unwrap();
    failure(execute(&server, &args).await, 1, "cli.file");
    assert_eq!(std::fs::read(&output).unwrap(), b"keep existing file");
    assert!(!std::fs::read_dir(&directory.0).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".part")
    }));
    service.shutdown().await.unwrap();
}
