use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use workflow_forge::v2::*;

#[path = "support/wait_store.rs"]
mod support;
use support::GateStore;
#[cfg(feature = "sqlite")]
#[path = "support/directory.rs"]
mod test_directory;

struct Provider {
    state: Arc<dyn ExecutionStore>,
    artifacts: Arc<dyn ArtifactStore>,
    path: Option<std::path::PathBuf>,
}
impl Drop for Provider {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}
fn providers() -> Vec<Provider> {
    let list = vec![Provider {
        state: Arc::new(modules::MemoryExecutionStore::default()),
        artifacts: Arc::new(modules::MemoryArtifacts::default()),
        path: None,
    }];
    #[cfg(feature = "sqlite")]
    let list = {
        let mut list = list;
        list.push(sqlite());
        list
    };
    list
}
#[cfg(feature = "sqlite")]
fn sqlite() -> Provider {
    let path = test_directory::create("workflow-forge-wait-races");
    let store = Arc::new(
        modules::SqliteExecutionStore::open(
            path.join("state.sqlite"),
            modules::SqliteOptions::default(),
        )
        .unwrap(),
    );
    Provider {
        state: store.clone(),
        artifacts: store,
        path: Some(path),
    }
}
fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn definition(timeout: u64) -> WorkflowDefinition {
    serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":"race","revision":"r1","input_schema":true,"output_schema":true,"entry":"callback",
        "nodes":[{"id":"callback","kind":"await_signal","input":{"literal":null},"correlation":{"literal":"job"},"timeout_ms":timeout,"payload_schema":true}],"edges":[],"output":{"select":{"source":"node","node":"callback","pointer":""}}
    })).unwrap()
}
async fn boot(
    provider: &Provider,
    predicate: fn(&RunSnapshot) -> bool,
) -> (EngineRuntime, Arc<GateStore>) {
    let gate = Arc::new(GateStore::new(provider.state.clone(), predicate));
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(gate.clone())
            .artifact_store(provider.artifacts.clone())
            .build()
            .unwrap(),
        BootOptions {
            recovery: RecoveryPolicy::Resume,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    (runtime, gate)
}
async fn start(app: &WorkflowApplication, timeout: u64) -> RunId {
    let plan = app.prepare(access(), definition(timeout)).await.unwrap();
    app.start(access(), StartRunRequest::new(plan, Value::Null))
        .await
        .unwrap()
        .run_id
}
async fn waiting(app: &WorkflowApplication, id: &RunId) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = app.status(access(), id.clone()).await.unwrap();
            if snapshot.state == RunState::Waiting {
                return snapshot;
            }
            assert!(!snapshot.state.is_terminal(), "{:?}", snapshot.error);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap()
}
fn command(run: &RunSnapshot) -> SignalCommand {
    SignalCommand {
        run_id: run.id.clone(),
        wait_id: run.waits.keys().next().unwrap().clone(),
        message_id: "one".into(),
        correlation: "job".into(),
        payload: json!(7),
        artifacts: vec![],
    }
}
async fn done(app: &WorkflowApplication, id: RunId) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(5), app.wait(access(), id))
        .await
        .unwrap()
        .unwrap()
}
fn delivered(run: &RunSnapshot) -> bool {
    run.waits.values().any(|w| w.delivery.is_some())
}

#[tokio::test]
async fn collecting_one_run_preserves_the_package_of_another_and_a_later_acceptance() {
    for provider in providers() {
        let (runtime, gate) = boot(&provider, |_| false).await;
        let app = runtime.application();
        let first = start(&app, 60_000).await;
        let second = start(&app, 60_000).await;
        let first_run = waiting(&app, &first).await;
        let second_run = waiting(&app, &second).await;
        let package = first_run.package.clone();
        assert_eq!(second_run.package, package);
        app.signal(access(), command(&first_run)).await.unwrap();
        let done_first = done(&app, first.clone()).await;
        let retention = Limits {
            terminal_runs: 0,
            ..Default::default()
        };
        provider
            .state
            .collect(
                &gate.owner(),
                done_first.finished_at_ms.unwrap(),
                &retention,
            )
            .await
            .unwrap();
        assert_eq!(
            app.status(access(), first).await.unwrap_err().code(),
            "not_found"
        );
        assert_eq!(
            app.status(access(), second.clone()).await.unwrap().package,
            package
        );
        // A public snapshot is independently owned; replacing its package under
        // the same revision must still be rejected by the provider.
        let mut changed = second_run.clone();
        changed.package.definitions[0].revision = "altered".into();
        changed.revision += 1;
        assert_eq!(
            provider
                .state
                .commit(&gate.owner(), second_run.revision, changed)
                .await
                .unwrap_err()
                .code(),
            "state.conflict"
        );
        assert_eq!(
            app.status(access(), second.clone()).await.unwrap().package,
            package
        );
        app.signal(access(), command(&second_run)).await.unwrap();
        let done_second = done(&app, second.clone()).await;
        provider
            .state
            .collect(
                &gate.owner(),
                done_second.finished_at_ms.unwrap(),
                &retention,
            )
            .await
            .unwrap();
        assert!(provider.state.get(&second).await.unwrap().is_none());
        let third = start(&app, 60_000).await;
        let third_run = waiting(&app, &third).await;
        assert_eq!(third_run.package, package);
        app.signal(access(), command(&third_run)).await.unwrap();
        assert_eq!(done(&app, third).await.state, RunState::Succeeded);
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    }
}

#[tokio::test]
async fn a_signal_between_suspension_and_parking_cannot_lose_the_wakeup() {
    for provider in providers() {
        let (runtime, gate) = boot(&provider, |r| r.state == RunState::Waiting).await;
        let app = runtime.application();
        let id = start(&app, 60_000).await;
        gate.entered().await;
        let before = app.status(access(), id.clone()).await.unwrap();
        assert_eq!(before.state, RunState::Running);
        let cmd = command(&before);
        let receipt = app.signal(access(), cmd.clone()).await.unwrap();
        assert_eq!(receipt.durable, provider.state.capabilities().durable);
        gate.release();
        let run = done(&app, id).await;
        assert_eq!(run.state, RunState::Succeeded, "{:?}", run.error);
        assert_eq!(run.output, Some(json!({"start":null,"signal":7})));
        assert_eq!(run.waits[&cmd.wait_id].state, WaitState::Consumed);
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    }
}

#[tokio::test]
async fn competing_signals_have_one_delivery_and_only_exact_duplicates_share_its_receipt() {
    for identical in [true, false] {
        for provider in providers() {
            let (runtime, gate) = boot(&provider, delivered).await;
            let app = runtime.application();
            let id = start(&app, 60_000).await;
            let cmd = command(&waiting(&app, &id).await);
            let pending = {
                let app = app.clone();
                let cmd = cmd.clone();
                tokio::spawn(async move { app.signal(access(), cmd).await })
            };
            gate.entered().await;
            let mut winner = cmd.clone();
            if !identical {
                winner.message_id = "two".into();
                winner.payload = json!(8);
            }
            let receipt = app.signal(access(), winner.clone()).await.unwrap();
            assert!(!receipt.duplicate);
            gate.release();
            let result = pending.await.unwrap();
            if identical {
                let mut duplicate = receipt.clone();
                duplicate.duplicate = true;
                assert_eq!(result.unwrap(), duplicate);
            } else {
                assert_eq!(result.unwrap_err().code(), "state.conflict");
            }
            let run = done(&app, id).await;
            assert_eq!(run.state, RunState::Succeeded, "{:?}", run.error);
            assert_eq!(
                run.waits[&cmd.wait_id].delivery.as_ref().unwrap().receipt,
                receipt
            );
            assert_eq!(
                run.output,
                Some(json!({"start":null,"signal":winner.payload}))
            );
            runtime.shutdown(ShutdownOptions::default()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn stale_signal_cannot_overwrite_committed_cancellation_or_expiry() {
    for cancel in [true, false] {
        for provider in providers() {
            let (runtime, gate) = boot(&provider, delivered).await;
            let app = runtime.application();
            let id = start(&app, if cancel { 60_000 } else { 300 }).await;
            let cmd = command(&waiting(&app, &id).await);
            let pending = {
                let app = app.clone();
                let cmd = cmd.clone();
                tokio::spawn(async move { app.signal(access(), cmd).await })
            };
            gate.entered().await;
            if cancel {
                app.cancel(access(), id.clone()).await.unwrap();
            }
            let terminal = done(&app, id.clone()).await;
            assert_eq!(
                terminal.state,
                if cancel {
                    RunState::Cancelled
                } else {
                    RunState::Failed
                }
            );
            assert_eq!(
                terminal.waits[&cmd.wait_id].state,
                if cancel {
                    WaitState::Closed
                } else {
                    WaitState::Expired
                }
            );
            gate.release();
            let error = pending.await.unwrap().unwrap_err();
            assert_eq!(
                error.code(),
                if cancel {
                    "state.conflict"
                } else {
                    "wait.expired"
                }
            );
            assert_eq!(app.status(access(), id).await.unwrap(), terminal);
            assert!(terminal.waits[&cmd.wait_id].delivery.is_none());
            runtime.shutdown(ShutdownOptions::default()).await.unwrap();
        }
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn failed_signal_attachment_rolls_back_the_receipt_and_every_new_artifact_pin() {
    use futures::stream;
    let provider = sqlite();
    let (runtime, gate) = boot(&provider, |_| false).await;
    let app = runtime.application();
    let id = start(&app, 60_000).await;
    let mut cmd = command(&waiting(&app, &id).await);
    let reference = provider
        .artifacts
        .write(
            "default",
            Box::pin(stream::once(async { Ok(vec![1, 2, 3]) })),
            "test/bytes",
        )
        .await
        .unwrap();
    let mut missing = reference.clone();
    missing.id = "zzzz-missing".into();
    cmd.artifacts = vec![reference.clone(), missing];
    assert_eq!(
        app.signal(access(), cmd.clone()).await.unwrap_err().code(),
        "not_found"
    );
    assert!(
        app.status(access(), id.clone()).await.unwrap().waits[&cmd.wait_id]
            .delivery
            .is_none()
    );
    let run_access = ArtifactAccess {
        runtime_owner: gate.owner(),
        run_id: id.clone(),
    };
    assert_eq!(
        provider
            .artifacts
            .read_for_run(&run_access, &reference)
            .await
            .err()
            .unwrap()
            .code(),
        "access.denied"
    );
    cmd.artifacts = vec![reference.clone()];
    let receipt = app.signal(access(), cmd.clone()).await.unwrap();
    assert!(receipt.durable && !receipt.duplicate);
    let terminal = done(&app, id.clone()).await;
    assert_eq!(terminal.state, RunState::Succeeded);
    assert!(
        provider
            .artifacts
            .read_for_run(&run_access, &reference)
            .await
            .is_ok()
    );
    assert!(app.signal(access(), cmd.clone()).await.unwrap().duplicate);
    provider
        .state
        .collect(
            &gate.owner(),
            terminal.finished_at_ms.unwrap(),
            &Limits {
                terminal_runs: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        app.status(access(), id).await.unwrap_err().code(),
        "not_found"
    );
    assert_eq!(
        app.signal(access(), cmd).await.unwrap_err().code(),
        "not_found"
    );
    assert_eq!(
        provider
            .artifacts
            .read(&reference)
            .await
            .err()
            .unwrap()
            .code(),
        "not_found"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
