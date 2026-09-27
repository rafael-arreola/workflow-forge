#![cfg(feature = "sqlite")]
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use workflow_forge::v2::*;

#[path = "support/sqlite_resolution.rs"]
mod resolution;
#[path = "support/directory.rs"]
mod test_directory;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        Self(test_directory::create("workflow-forge-process"))
    }
    fn db(&self) -> PathBuf {
        self.0.join("state.sqlite")
    }
    fn effects(&self) -> PathBuf {
        self.0.join("effects.jsonl")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn definition() -> WorkflowDefinition {
    serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":"sqlite.reference","revision":"r1","entry":"write","input_schema":true,"output_schema":true,
        "nodes":[
            {"id":"write","kind":"operation","operation":{"id":"test.sqlite.write","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}},"retry":{"max_attempts":2,"initial_delay_ms":0,"max_delay_ms":0,"jitter":false}},
            {"id":"return","kind":"operation","operation":{"id":"forge.data.identity","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"node","node":"write","pointer":""}}}
        ],"edges":[{"from":"write","to":"return"}],"output":{"select":{"source":"node","node":"return","pointer":""}}
    })).unwrap()
}
fn records(path: &Path) -> Vec<Value> {
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => vec![],
        Err(error) => panic!("ledger unavailable: {error}"),
    }
}
struct Destination {
    descriptor: OperationDescriptor,
    path: PathBuf,
    crash_after_effect: bool,
    quiescent: bool,
    pause: Option<Arc<tokio::sync::Notify>>,
}
impl Operation for Destination {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            use std::io::Write;
            if let Some(ready) = &self.pause {
                ready.notify_one();
                std::future::pending::<()>().await;
            }
            let row = json!({"key":invocation.effect_key,"output":invocation.input});
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .unwrap();
            writeln!(file, "{row}").unwrap();
            file.sync_all().unwrap();
            if self.crash_after_effect {
                std::process::exit(73);
            }
            Ok(OperationOutput::json(invocation.input))
        })
    }
}
impl EffectInspector for Destination {
    fn operation(&self) -> &OperationRevision {
        &self.descriptor.revision
    }
    fn inspect<'a>(
        &'a self,
        _: OperationContext,
        invocation: Invocation,
    ) -> PortFuture<'a, EffectInspection> {
        Box::pin(async move {
            let evidence = EffectEvidence {
                authority: "test.process.ledger".into(),
                reference: invocation.effect_key.clone().unwrap(),
                note: "Parent has observed termination of the original writer process".into(),
            };
            if let Some(row) = records(&self.path)
                .iter()
                .find(|r| r["key"] == json!(invocation.effect_key))
            {
                Ok(EffectInspection::Applied {
                    output: row["output"].clone(),
                    evidence,
                })
            } else {
                Ok(EffectInspection::NotApplied {
                    evidence,
                    quiescent: self.quiescent,
                })
            }
        })
    }
}
fn builder(store: Arc<dyn ExecutionStore>, path: PathBuf, mode: &str) -> WorkflowBuilder {
    let mut d = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    d.revision = OperationRevision::new("test.sqlite.write", "1", "r1");
    d.effect = EffectKind::Write;
    d.repetition = Repetition::Unsafe;
    d.reconciliation = true;
    d.output_schema = json!({"$schema":SCHEMA_DIALECT,"type":"integer"});
    if mode == "incompatible_revision" {
        d.output_schema = json!(false);
    }
    let destination = Arc::new(Destination {
        descriptor: d.clone(),
        path,
        crash_after_effect: mode == "effect",
        quiescent: mode == "parent",
        pause: None,
    });
    let mut b = WorkflowBuilder::standard()
        .execution_store(store)
        .limits(Limits {
            run_timeout_ms: 60000,
            ..Default::default()
        });
    b.register_bundle(OperationBundle {
        module: ModuleDescriptor {
            id: "test.sqlite".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![d.revision],
        },
        operations: vec![destination.clone()],
        inspectors: vec![destination],
    })
    .unwrap();
    b
}
fn store(path: &Path) -> Arc<modules::SqliteExecutionStore> {
    Arc::new(modules::SqliteExecutionStore::open(path, modules::SqliteOptions::default()).unwrap())
}
async fn boot(path: &Path, effects: PathBuf) -> EngineRuntime {
    EngineRuntime::boot(
        builder(store(path), effects, "parent").build().unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap()
}
async fn request(app: &WorkflowApplication, value: Value) -> Result<StartReceipt, ForgeError> {
    let plan = app.prepare(access(), definition()).await?;
    let mut request = StartRunRequest::new(plan, value);
    request.options.require_durable = true;
    request.options.receipt_key = Some("request-1".into());
    app.start(access(), request).await
}
async fn wait(app: &WorkflowApplication, id: RunId) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(5), app.wait(access(), id))
        .await
        .unwrap()
        .unwrap()
}

/// Test-only Decorator: commit first, then terminate the whole process without
/// unwinding Rust. It never turns a noncommitted transition into a successful one.
struct CrashStore {
    inner: Arc<modules::SqliteExecutionStore>,
    mode: String,
    boot_claimed: Option<Arc<tokio::sync::Notify>>,
}
impl ExecutionStore for CrashStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn claim<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        self.inner.claim(owner)
    }
    fn release<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        self.inner.release(owner)
    }
    fn create<'a>(
        &'a self,
        owner: &'a str,
        run: RunSnapshot,
        receipt: Option<ReceiptReservation>,
        limits: &'a Limits,
    ) -> PortFuture<'a, CreateOutcome> {
        Box::pin(async move {
            let result = self.inner.create(owner, run, receipt, limits).await?;
            if self.mode == "accepted" {
                std::process::exit(73);
            }
            Ok(result)
        })
    }
    fn get<'a>(&'a self, id: &'a RunId) -> PortFuture<'a, Option<RunSnapshot>> {
        self.inner.get(id)
    }
    fn view<'a>(
        &'a self,
        id: &'a RunId,
        node: Option<&'a str>,
    ) -> PortFuture<'a, Option<ExecutionView>> {
        self.inner.view(id, node)
    }
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>> {
        if let Some(notify) = &self.boot_claimed {
            return Box::pin(async move {
                notify.notify_one();
                std::future::pending().await
            });
        }
        self.inner.unfinished()
    }
    fn unfinished_heads(&self) -> PortFuture<'_, Vec<RunHead>> {
        // Keep the scheduler before dispatch for the lost-acceptance-ack case.
        if self.mode == "accepted" {
            Box::pin(async { Ok(vec![]) })
        } else {
            self.inner.unfinished_heads()
        }
    }
    fn commit<'a>(
        &'a self,
        owner: &'a str,
        expected: u64,
        next: RunSnapshot,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let resolution = self.mode.starts_with("resolution:")
                && next.audit.iter().any(|entry| matches!(entry, AuditEntry::Resolution(a) if a.command.command_id == "crash-decision"));
            if resolution && self.mode == "resolution:before_applied" {
                std::process::exit(73);
            }
            self.inner.commit(owner, expected, next).await?;
            if resolution {
                std::process::exit(73);
            }
            Ok(())
        })
    }
    fn commit_invocation<'a>(
        &'a self,
        owner: &'a str,
        id: &'a RunId,
        expected: u64,
        node: &'a str,
        record: InvocationRecord,
    ) -> PortFuture<'a, ExecutionView> {
        Box::pin(async move {
            let crash = node == "/nodes/write"
                && ((self.mode == "intent" && record.state == InvocationState::Running)
                    || (self.mode == "result" && record.state == InvocationState::Succeeded));
            let value = self
                .inner
                .commit_invocation(owner, id, expected, node, record)
                .await?;
            if crash {
                std::process::exit(73);
            }
            Ok(value)
        })
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        self.inner.collect(owner, now, limits)
    }
}

#[tokio::test]
async fn process_child() {
    let Ok(mode) = std::env::var("WF_SQLITE_CRASH_CASE") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("WF_SQLITE_CRASH_PATH").unwrap());
    let effects = path.parent().unwrap().join("effects.jsonl");
    let wrapped = Arc::new(CrashStore {
        inner: store(&path),
        mode: mode.clone(),
        boot_claimed: None,
    });
    let runtime = EngineRuntime::boot(
        builder(wrapped, effects, &mode).build().unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let accepted = request(&app, json!(7)).await.unwrap();
    let run = wait(&app, accepted.run_id).await;
    if mode.starts_with("resolution:") {
        resolution::resolve_for_crash(&app, run, &mode).await;
    }
    panic!("Requested crash boundary was not reached");
}

async fn crash_at(dir: &Directory, mode: &str) {
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "process_child", "--nocapture"])
        .env("WF_SQLITE_CRASH_CASE", mode)
        .env("WF_SQLITE_CRASH_PATH", dir.db())
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(15), command.output())
        .await
        .expect("child timeout")
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "{mode}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn process_crashes_preserve_acceptance_intent_effect_and_confirmed_result() {
    for mode in ["accepted", "intent", "effect", "result"] {
        let dir = Directory::new();
        crash_at(&dir, mode).await;

        let runtime = boot(&dir.db(), dir.effects()).await;
        let app = runtime.application();
        let receipt = request(&app, json!(7)).await.unwrap();
        assert!(
            receipt.durable && receipt.duplicate,
            "{mode}: committed acceptance must survive its lost ack"
        );
        let mut snapshot = wait(&app, receipt.run_id.clone()).await;
        let mut resolution = None;
        if mode == "intent" || mode == "effect" {
            assert_eq!(
                snapshot.state,
                RunState::Blocked,
                "{mode}: {:?}",
                snapshot.error
            );
            assert_eq!(records(&dir.effects()).len(), usize::from(mode == "effect"));
            let record = &snapshot.invocations["/nodes/write"];
            assert_eq!(record.state, InvocationState::Unknown);
            let finding = app
                .inspect_effect(access(), snapshot.id.clone(), record.id.clone())
                .await
                .unwrap();
            let decision = match finding {
                EffectInspection::Applied { output, evidence } => {
                    EffectResolution::ConfirmApplied { output, evidence }
                }
                EffectInspection::NotApplied {
                    evidence,
                    quiescent,
                } => {
                    assert!(quiescent);
                    EffectResolution::ConfirmNotApplied {
                        evidence,
                        quiescent,
                        retry: true,
                    }
                }
                _ => panic!("fixture must provide decisive evidence"),
            };
            let command = ReconcileCommand {
                command_id: "recover-effect".into(),
                run_id: snapshot.id.clone(),
                invocation_id: record.id.clone(),
                expected_revision: snapshot.revision,
                observed_attempt: record.attempt_id.clone(),
                resolution: decision,
            };
            let answer = app.reconcile(access(), command.clone()).await.unwrap();
            resolution = Some((command, answer));
            snapshot = wait(&app, receipt.run_id.clone()).await;
        }
        assert_eq!(
            snapshot.state,
            RunState::Succeeded,
            "{mode}: {:?}",
            snapshot.error
        );
        assert_eq!(snapshot.output, Some(json!(7)));
        assert_eq!(
            records(&dir.effects()).len(),
            1,
            "{mode}: external effect repeated"
        );
        let record = &snapshot.invocations["/nodes/write"];
        assert_eq!(record.attempts, if mode == "intent" { 2 } else { 1 });
        assert_eq!(records(&dir.effects())[0]["key"], json!(record.effect_key));
        assert_eq!(
            request(&app, json!(8)).await.unwrap_err().code(),
            "state.conflict"
        );
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();

        let runtime = boot(&dir.db(), dir.effects()).await;
        let app = runtime.application();
        assert_eq!(
            app.status(access(), receipt.run_id).await.unwrap(),
            snapshot
        );
        if let Some((command, answer)) = resolution {
            assert_eq!(
                app.reconcile(access(), command).await.unwrap(),
                answer,
                "audit and resolution receipt must survive restart"
            );
        }
        assert_eq!(records(&dir.effects()).len(), 1);
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    }
}

#[tokio::test]
async fn missing_or_changed_plugin_blocks_sqlite_recovery_until_exact_reinstallation() {
    let dir = Directory::new();
    crash_at(&dir, "result").await;
    assert_eq!(records(&dir.effects()).len(), 1);

    let provider = store(&dir.db());
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(provider.clone())
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let pending = provider.unfinished().await.unwrap();
    assert_eq!(pending.len(), 1);
    let id = pending[0].id.clone();
    let blocked = wait(&runtime.application(), id.clone()).await;
    assert_eq!(blocked.state, RunState::Blocked);
    assert_eq!(
        blocked.error.as_ref().unwrap().code(),
        "recovery.unavailable"
    );
    let confirmed = blocked.invocations["/nodes/write"].clone();
    assert_eq!(confirmed.state, InvocationState::Succeeded);
    assert_eq!(confirmed.output, Some(json!(7)));
    assert!(!blocked.invocations.contains_key("/nodes/return"));
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    drop(provider);

    // Reusing the exact revision ID with another contract is not a replacement.
    let runtime = EngineRuntime::boot(
        builder(store(&dir.db()), dir.effects(), "incompatible_revision")
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let incompatible = wait(&runtime.application(), id.clone()).await;
    assert_eq!(incompatible.state, RunState::Blocked);
    assert_eq!(
        incompatible.error.as_ref().unwrap().code(),
        "recovery.unavailable"
    );
    assert_eq!(incompatible.package, blocked.package);
    assert_eq!(incompatible.invocations, blocked.invocations);
    assert_eq!(records(&dir.effects()).len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();

    let runtime = boot(&dir.db(), dir.effects()).await;
    let app = runtime.application();
    let receipt = request(&app, json!(7)).await.unwrap();
    assert!(receipt.duplicate && receipt.durable);
    assert_eq!(receipt.run_id, id);
    let done = wait(&app, id.clone()).await;
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.error);
    assert_eq!(done.output, Some(json!(7)));
    assert_eq!(done.package, blocked.package);
    assert_eq!(done.invocations["/nodes/write"], confirmed);
    assert_eq!(records(&dir.effects()).len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();

    let runtime = boot(&dir.db(), dir.effects()).await;
    assert_eq!(
        runtime.application().status(access(), id).await.unwrap(),
        done
    );
    assert_eq!(records(&dir.effects()).len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn durable_profile_rejects_ephemeral_artifact_dependencies_before_acceptance() {
    let dir = Directory::new();
    let mut b = WorkflowBuilder::standard().execution_store(store(&dir.db()));
    b.register_bundle(
        workflow_forge_reference_module::inventory::inventory_operations(Arc::new(
            workflow_forge_reference_module::inventory::MemoryInventory::default(),
        )),
    )
    .unwrap();
    let runtime = EngineRuntime::boot(b.build().unwrap(), BootOptions::default())
        .await
        .unwrap();
    let error = runtime
        .application()
        .prepare_json(
            access(),
            include_bytes!("../../../examples/workflows/inventory_import.v2.json"),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), "capability.unsupported");
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn cancelled_boot_releases_a_durable_claim_while_its_provider_stays_alive() {
    let dir = Directory::new();
    let notify = Arc::new(tokio::sync::Notify::new());
    let wrapped = Arc::new(CrashStore {
        inner: store(&dir.db()),
        mode: "boot".into(),
        boot_claimed: Some(notify.clone()),
    });
    let assembly = builder(wrapped.clone(), dir.effects(), "parent")
        .build()
        .unwrap();
    let task = tokio::spawn(EngineRuntime::boot(assembly, BootOptions::default()));
    tokio::time::timeout(Duration::from_secs(2), notify.notified())
        .await
        .unwrap();
    assert!(!task.is_finished());
    task.abort();
    assert!(matches!(task.await,Err(error) if error.is_cancelled()));
    let replacement = store(&dir.db());
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match replacement.claim("replacement").await {
                Ok(()) => break,
                Err(error) if error.code() == "state.conflict" => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(error) => panic!("replacement claim failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    replacement.release("replacement").await.unwrap();
}

#[tokio::test]
async fn dropping_runtime_releases_ownership_even_with_an_old_application_handle() {
    let dir = Directory::new();
    let runtime = boot(&dir.db(), dir.effects()).await;
    let old_application = runtime.application();
    drop(runtime);
    assert!(!old_application.is_ready());
    let replacement = store(&dir.db());
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match replacement.claim("new-owner").await {
                Ok(()) => break,
                Err(error) if error.code() == "state.conflict" => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(error) => panic!("replacement claim failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    replacement.release("new-owner").await.unwrap();
}

#[tokio::test]
async fn abandoning_shutdown_releases_the_store_after_supervisor_join_was_taken() {
    let dir = Directory::new();
    let provider = store(&dir.db());
    let started = Arc::new(tokio::sync::Notify::new());
    let mut descriptor = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    descriptor.revision = OperationRevision::new("test.sqlite.write", "1", "r1");
    descriptor.effect = EffectKind::Write;
    descriptor.repetition = Repetition::Unsafe;
    let mut builder = WorkflowBuilder::standard().execution_store(provider.clone());
    builder
        .register_bundle(OperationBundle {
            module: ModuleDescriptor {
                id: "test.pause".into(),
                version: "1".into(),
                protocol_version: PROTOCOL_VERSION,
                exports: vec![descriptor.revision.clone()],
            },
            operations: vec![Arc::new(Destination {
                descriptor,
                path: dir.effects(),
                crash_after_effect: false,
                quiescent: false,
                pause: Some(started.clone()),
            })],
            inspectors: vec![],
        })
        .unwrap();
    let runtime = EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
        .await
        .unwrap();
    let app = runtime.application();
    request(&app, json!(7)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    let shutdown = tokio::spawn(runtime.shutdown(ShutdownOptions {
        timeout: Duration::from_secs(60),
    }));
    tokio::time::timeout(Duration::from_secs(2), async {
        while app.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!shutdown.is_finished());
    shutdown.abort();
    assert!(matches!(shutdown.await,Err(error) if error.is_cancelled()));
    let replacement = store(&dir.db());
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match replacement.claim("replacement").await {
                Ok(()) => break,
                Err(error) if error.code() == "state.conflict" => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(error) => panic!("replacement claim failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(replacement.unfinished().await.unwrap().len(), 1);
    assert!(records(&dir.effects()).is_empty());
    replacement.release("replacement").await.unwrap();
}
