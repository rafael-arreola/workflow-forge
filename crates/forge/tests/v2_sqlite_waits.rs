#![cfg(feature = "sqlite")]
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use workflow_forge::v2::*;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "workflow-forge-waits-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn write(path: &Path, value: &Value) {
    use std::io::Write;
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(serde_json::to_string(value).unwrap().as_bytes())
        .unwrap();
    file.sync_all().unwrap();
}
fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn definition() -> WorkflowDefinition {
    serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":"durable.callback","revision":"r1","input_schema":true,"output_schema":true,"entry":"callback",
        "nodes":[
            {"id":"callback","kind":"await_signal","input":{"select":{"source":"input","pointer":""}},"correlation":{"literal":"job-1"},"timeout_ms":60000,"payload_schema":{"$ref":"https://example.test/signal"},
             "start":{"operation":{"id":"test.wait.start","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}}}},
            {"id":"finish","kind":"operation","operation":{"id":"test.wait.finish","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"node","node":"callback","pointer":"/signal"}}}
        ],"edges":[{"from":"callback","to":"finish"}],"output":{"select":{"source":"node","node":"finish","pointer":""}}
    })).unwrap()
}
struct OperationWithLedger {
    descriptor: OperationDescriptor,
    path: PathBuf,
    finish: bool,
}
impl Operation for OperationWithLedger {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> OperationFuture<'a> {
        Box::pin(async move {
            use std::io::Write;
            let value = if self.finish {
                let reference: ArtifactRef =
                    serde_json::from_value(invocation.input["artifact"].clone()).unwrap();
                let mut stream = context.read_artifact(&reference).await.unwrap();
                let mut data = vec![];
                while let Some(chunk) = stream.next().await {
                    data.extend(chunk.unwrap());
                }
                json!({"bytes":data})
            } else {
                json!({"ticket":"T-1"})
            };
            let mut ledger = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .unwrap();
            writeln!(
                ledger,
                "{}",
                json!({"key":invocation.effect_key,"output":value})
            )
            .unwrap();
            ledger.sync_all().unwrap();
            Ok(OperationOutput::json(value))
        })
    }
}
fn store(path: &Path) -> Arc<modules::SqliteExecutionStore> {
    Arc::new(
        modules::SqliteExecutionStore::open(
            path.join("state.sqlite"),
            modules::SqliteOptions::default(),
        )
        .unwrap(),
    )
}
fn builder(
    path: &Path,
    state: Arc<dyn ExecutionStore>,
    artifacts: Arc<dyn ArtifactStore>,
    authoring_schema: bool,
) -> WorkflowBuilder {
    let mut b = WorkflowBuilder::standard()
        .execution_store(state)
        .artifact_store(artifacts);
    if authoring_schema {
        b.register_schemas(vec![SchemaResource {
            uri: "https://example.test/signal".into(),
            revision: "r1".into(),
            schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["artifact"],"properties":{"artifact":{"type":"object"}},"additionalProperties":false}),
        }])
        .unwrap();
    }
    let mut operations: Vec<Arc<dyn Operation>> = vec![];
    let mut exports = vec![];
    for finish in [false, true] {
        let mut d = modules::data_operations().operations[0]
            .descriptor()
            .clone();
        d.revision = OperationRevision::new(
            if finish {
                "test.wait.finish"
            } else {
                "test.wait.start"
            },
            "1",
            "r1",
        );
        d.effect = EffectKind::Write;
        d.repetition = Repetition::Unsafe;
        if finish {
            d.required_resources.insert("artifacts".into());
        }
        exports.push(d.revision.clone());
        operations.push(Arc::new(OperationWithLedger {
            descriptor: d,
            path: path.join(if finish {
                "finish.jsonl"
            } else {
                "start.jsonl"
            }),
            finish,
        }));
    }
    b.register_bundle(OperationBundle {
        module: ModuleDescriptor {
            id: "test.wait".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports,
        },
        operations,
        inspectors: vec![],
    })
    .unwrap();
    b
}
async fn waiting(app: &WorkflowApplication, id: RunId) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let run = app.status(access(), id.clone()).await.unwrap();
            if run.state == RunState::Waiting {
                return run;
            }
            assert!(
                !run.state.is_terminal(),
                "Unexpected result: {:?}",
                run.error
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap()
}
async fn signal_command(provider: &dyn ArtifactStore, run: &RunSnapshot) -> SignalCommand {
    let reference = provider
        .write(
            "default",
            Box::pin(stream::once(async { Ok(vec![1, 2, 3]) })),
            "test/bytes",
        )
        .await
        .unwrap();
    let wait = run.waits.values().next().unwrap();
    SignalCommand {
        run_id: run.id.clone(),
        wait_id: wait.id.clone(),
        message_id: "callback-1".into(),
        correlation: "job-1".into(),
        payload: json!({"artifact":reference}),
        artifacts: vec![reference],
    }
}
fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().count())
        .unwrap_or(0)
}

struct CrashStore {
    inner: Arc<modules::SqliteExecutionStore>,
    path: PathBuf,
    mode: String,
    armed: AtomicBool,
}
impl ExecutionStore for CrashStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn artifact_domain(&self) -> Option<&str> {
        ExecutionStore::artifact_domain(&*self.inner)
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
        self.inner.create(owner, run, receipt, limits)
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
        self.inner.unfinished()
    }
    fn unfinished_heads(&self) -> PortFuture<'_, Vec<RunHead>> {
        if self.armed.load(Ordering::Acquire) && self.mode == "delivered" {
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
            let crash = next.waits.values().any(|w| match self.mode.as_str() {
                "reserved" => w.state == WaitState::Open && !w.start_confirmed,
                "delivered" => w.delivery.is_some(),
                "consumed" => w.state == WaitState::Consumed,
                _ => false,
            });
            let snapshot = if crash { Some(next.clone()) } else { None };
            self.inner.commit(owner, expected, next).await?;
            if let Some(snapshot) = snapshot {
                write(&self.path.join("boundary.json"), &json!(snapshot));
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
        self.inner
            .commit_invocation(owner, id, expected, node, record)
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        self.inner.collect(owner, now, limits)
    }
}

#[tokio::test]
async fn signal_process_child() {
    let Ok(mode) = std::env::var("WF_SIGNAL_CRASH_CASE") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("WF_SIGNAL_CRASH_PATH").unwrap());
    let inner = store(&path);
    let wrapper = Arc::new(CrashStore {
        inner: inner.clone(),
        path: path.clone(),
        mode,
        armed: AtomicBool::new(false),
    });
    let runtime = EngineRuntime::boot(
        builder(&path, wrapper.clone(), inner.clone(), true)
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let plan = app.prepare(access(), definition()).await.unwrap();
    let mut request = StartRunRequest::new(plan, json!({"job":7}));
    request.options.receipt_key = Some("job-1".into());
    request.options.require_durable = true;
    let receipt = app.start(access(), request).await.unwrap();
    let run = waiting(&app, receipt.run_id).await;
    let command = signal_command(&*inner, &run).await;
    write(&path.join("command.json"), &json!(command));
    wrapper.armed.store(true, Ordering::Release);
    app.signal(access(), command).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), app.wait(access(), run.id))
        .await
        .unwrap()
        .unwrap();
    panic!("Crash boundary was not reached: {:?}", result.error);
}

#[tokio::test]
async fn process_recovery_preserves_reservation_signal_receipt_consumption_and_attached_bytes() {
    for mode in ["reserved", "delivered", "consumed"] {
        let dir = Directory::new();
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "signal_process_child", "--nocapture"])
            .env("WF_SIGNAL_CRASH_CASE", mode)
            .env("WF_SIGNAL_CRASH_PATH", &dir.0)
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
        let boundary: RunSnapshot =
            serde_json::from_value(read(&dir.0.join("boundary.json"))).unwrap();
        assert_eq!(
            lines(&dir.0.join("start.jsonl")),
            usize::from(mode != "reserved")
        );
        assert_eq!(lines(&dir.0.join("finish.jsonl")), 0);
        let inner = store(&dir.0);
        // The authoring schema is absent. Recovery and signal validation use
        // only the accepted package; current authoring cannot compile this flow.
        let runtime = EngineRuntime::boot(
            builder(&dir.0, inner.clone(), inner.clone(), false)
                .build()
                .unwrap(),
            BootOptions::default(),
        )
        .await
        .unwrap();
        let app = runtime.application();
        assert!(app.prepare(access(), definition()).await.is_err());
        let signal = if mode == "reserved" {
            let run = waiting(&app, boundary.id.clone()).await;
            assert_eq!(
                run.waits.values().next().unwrap().deadline_at_ms,
                boundary.waits.values().next().unwrap().deadline_at_ms
            );
            let signal = signal_command(&*inner, &run).await;
            let mut invalid = signal.clone();
            invalid.payload = json!({"wrong":true});
            assert_eq!(
                app.signal(access(), invalid).await.unwrap_err().code(),
                "data.invalid"
            );
            signal
        } else {
            serde_json::from_value(read(&dir.0.join("command.json"))).unwrap()
        };
        let receipt = app.signal(access(), signal.clone()).await.unwrap();
        assert!(receipt.durable);
        assert_eq!(receipt.duplicate, mode != "reserved");
        let completed = tokio::time::timeout(
            Duration::from_secs(5),
            app.wait(access(), boundary.id.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            completed.state,
            RunState::Succeeded,
            "{mode}: {:?}",
            completed.error
        );
        assert_eq!(completed.output, Some(json!({"bytes":[1,2,3]})));
        assert_eq!(completed.waits[&signal.wait_id].state, WaitState::Consumed);
        assert_eq!(lines(&dir.0.join("start.jsonl")), 1);
        assert_eq!(lines(&dir.0.join("finish.jsonl")), 1);
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
        let inner = store(&dir.0);
        let runtime = EngineRuntime::boot(
            builder(&dir.0, inner.clone(), inner, false)
                .build()
                .unwrap(),
            BootOptions::default(),
        )
        .await
        .unwrap();
        let app = runtime.application();
        let duplicate = app.signal(access(), signal.clone()).await.unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.accepted_at_ms, receipt.accepted_at_ms);
        assert_eq!(app.status(access(), boundary.id).await.unwrap(), completed);
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    }
}

#[tokio::test]
async fn a_timer_keeps_its_original_deadline_after_shutdown_and_boot() {
    let dir = Directory::new();
    let inner = store(&dir.0);
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(inner.clone())
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let d:WorkflowDefinition=serde_json::from_value(json!({"format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":"timer","revision":"r1","input_schema":true,"output_schema":true,"entry":"pause","nodes":[{"id":"pause","kind":"timer","duration_ms":300,"input":{"select":{"source":"input","pointer":""}}}],"edges":[],"output":{"select":{"source":"node","node":"pause","pointer":""}}})).unwrap();
    let app = runtime.application();
    let plan = app.prepare(access(), d).await.unwrap();
    let receipt = app
        .start(access(), StartRunRequest::new(plan, json!(9)))
        .await
        .unwrap();
    let before = waiting(&app, receipt.run_id.clone()).await;
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store(&dir.0))
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let completed = tokio::time::timeout(
        Duration::from_secs(3),
        runtime.application().wait(access(), receipt.run_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(completed.state, RunState::Succeeded);
    assert_eq!(completed.output, Some(json!(9)));
    let original = before.waits.values().next().unwrap();
    let current = completed.waits.values().next().unwrap();
    assert_eq!(original.id, current.id);
    assert_eq!(original.deadline_at_ms, current.deadline_at_ms);
    assert_eq!(original.created_at_ms, current.created_at_ms);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
