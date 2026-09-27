#![cfg(feature = "sqlite")]
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use workflow_forge::v2::*;
use workflow_forge_reference_module::inventory::{
    InventoryDestination, InventoryUpdate, inventory_operations,
};
#[path = "support/directory.rs"]
mod test_directory;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        Self(test_directory::create("workflow-forge-durable-csv"))
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
    let mut value: Value = serde_json::from_slice(include_bytes!(
        "../../../examples/workflows/inventory_import.v2.json"
    ))
    .unwrap();
    fn retry(value: &mut Value) {
        match value {
            Value::Object(object) => {
                if object.get("kind").and_then(Value::as_str) == Some("operation") {
                    object.insert("retry".into(),json!({"max_attempts":3,"initial_delay_ms":0,"max_delay_ms":0,"jitter":false}));
                }
                for value in object.values_mut() {
                    retry(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    retry(value);
                }
            }
            _ => (),
        }
    }
    // An interrupted attempt consumes budget, including read/report operations.
    retry(&mut value);
    serde_json::from_value(value).unwrap()
}
fn records(path: &Path) -> Vec<Value> {
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => vec![],
        Err(e) => panic!("fixture ledger: {e}"),
    }
}
fn append(path: &Path, value: &Value) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{value}").unwrap();
    file.sync_all().unwrap();
}
struct Ledger {
    directory: PathBuf,
    serial: Mutex<()>,
}
impl InventoryDestination for Ledger {
    fn apply<'a>(
        &'a self,
        key: &'a str,
        update: InventoryUpdate,
    ) -> PortFuture<'a, InventoryUpdate, OperationError> {
        Box::pin(async move {
            let _guard = self.serial.lock().unwrap();
            append(
                &self.directory.join("attempts.jsonl"),
                &json!({"key":key,"update":update}),
            );
            if let Some(row) = records(&self.directory.join("effects.jsonl"))
                .into_iter()
                .find(|r| r["key"] == key)
            {
                assert_eq!(row["update"], json!(update));
            } else {
                append(
                    &self.directory.join("effects.jsonl"),
                    &json!({"key":key,"update":update}),
                );
            }
            Ok(update)
        })
    }
    fn inspect<'a>(&'a self, key: &'a str) -> PortFuture<'a, EffectInspection> {
        Box::pin(async move {
            let _guard = self.serial.lock().unwrap();
            let evidence = EffectEvidence {
                authority: "test.durable.inventory".into(),
                reference: key.into(),
                note: "Serialized fixture ledger".into(),
            };
            Ok(
                match records(&self.directory.join("effects.jsonl"))
                    .into_iter()
                    .find(|r| r["key"] == key)
                {
                    Some(row) => EffectInspection::Applied {
                        output: row["update"].clone(),
                        evidence,
                    },
                    None => EffectInspection::NotApplied {
                        evidence,
                        quiescent: true,
                    },
                },
            )
        })
    }
}
fn sqlite(path: &Path) -> Arc<modules::SqliteExecutionStore> {
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
    store: Arc<dyn ExecutionStore>,
    artifacts: Arc<dyn ArtifactStore>,
) -> WorkflowBuilder {
    let mut builder = WorkflowBuilder::standard()
        .execution_store(store)
        .artifact_store(artifacts)
        .limits(Limits {
            run_timeout_ms: 60_000,
            ..Default::default()
        });
    builder
        .register_bundle(inventory_operations(Arc::new(Ledger {
            directory: path.into(),
            serial: Mutex::new(()),
        })))
        .unwrap();
    builder
}
async fn start(
    app: &WorkflowApplication,
    source: &ArtifactRef,
) -> Result<StartReceipt, ForgeError> {
    let plan = app.prepare(access(), definition()).await?;
    let mut request = StartRunRequest::new(plan, json!({"source":source}));
    request.options.require_durable = true;
    request.options.receipt_key = Some("import-1".into());
    request.options.artifacts = vec![source.clone()];
    app.start(access(), request).await
}
async fn wait(app: &WorkflowApplication, id: RunId) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(20), app.wait(access(), id))
        .await
        .unwrap()
        .unwrap()
}
async fn report(store: &dyn ArtifactStore, reference: &ArtifactRef) -> Vec<Value> {
    let mut stream = store.read(reference).await.unwrap();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        data.extend(chunk.unwrap());
    }
    std::str::from_utf8(&data)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// Decorates only public ports. Each crash follows an acknowledged durable
/// boundary; process::exit bypasses runtime shutdown and every Rust destructor.
struct CrashPorts {
    inner: Arc<modules::SqliteExecutionStore>,
    mode: String,
}
impl ExecutionStore for CrashPorts {
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
        self.inner.unfinished_heads()
    }
    fn commit<'a>(
        &'a self,
        owner: &'a str,
        expected: u64,
        next: RunSnapshot,
    ) -> PortFuture<'a, ()> {
        self.inner.commit(owner, expected, next)
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
            let crash = self.mode == "page"
                && node == "/nodes/import"
                && matches!(
                    record.control,
                    Some(ControlFrame::Loop { iteration: 1, .. })
                );
            let result = self
                .inner
                .commit_invocation(owner, id, expected, node, record)
                .await?;
            if crash {
                std::process::exit(73);
            }
            Ok(result)
        })
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        self.inner.collect(owner, now, limits)
    }
}
impl ArtifactStore for CrashPorts {
    fn durable(&self) -> bool {
        true
    }
    fn artifact_domain(&self) -> Option<&str> {
        ArtifactStore::artifact_domain(&*self.inner)
    }
    fn write<'a>(
        &'a self,
        scope: &'a str,
        content: ByteStream,
        media: &'a str,
    ) -> PortFuture<'a, ArtifactRef> {
        self.inner.write(scope, content, media)
    }
    fn read<'a>(&'a self, reference: &'a ArtifactRef) -> PortFuture<'a, ByteStream> {
        self.inner.read(reference)
    }
    fn read_for_run<'a>(
        &'a self,
        access: &'a ArtifactAccess,
        reference: &'a ArtifactRef,
    ) -> PortFuture<'a, ByteStream> {
        self.inner.read_for_run(access, reference)
    }
    fn write_for_run<'a>(
        &'a self,
        access: &'a ArtifactAccess,
        scope: &'a str,
        content: ByteStream,
        media: &'a str,
    ) -> PortFuture<'a, ArtifactRef> {
        Box::pin(async move {
            let content: ByteStream = if self.mode == "staging" && media == "application/x-ndjson" {
                // The provider polls the next chunk only after persisting the
                // first. This exit leaves one real staging write on disk.
                Box::pin(
                    content
                        .take(1)
                        .chain(stream::once(async { std::process::exit(73) })),
                )
            } else {
                content
            };
            let result = self
                .inner
                .write_for_run(access, scope, content, media)
                .await?;
            if self.mode == "published" && media == "application/x-ndjson" {
                std::process::exit(73);
            }
            Ok(result)
        })
    }
}

#[tokio::test]
async fn inventory_process_child() {
    let Ok(mode) = std::env::var("WF_DURABLE_CSV_CASE") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("WF_DURABLE_CSV_PATH").unwrap());
    let ports = Arc::new(CrashPorts {
        inner: sqlite(&path),
        mode,
    });
    let runtime = EngineRuntime::boot(
        builder(&path, ports.clone(), ports.clone())
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let mut csv = "sku,quantity\n".to_owned();
    for index in 0..201 {
        csv.push_str(&format!("SKU-{index},{}\n", index + 1));
    }
    let source = ports
        .write(
            "default",
            Box::pin(stream::once(async move { Ok(csv.into_bytes()) })),
            "text/csv",
        )
        .await
        .unwrap();
    append(&path.join("source.jsonl"), &json!(source));
    let receipt = start(&runtime.application(), &source).await.unwrap();
    let completed = wait(&runtime.application(), receipt.run_id).await;
    panic!("Crash boundary was not reached: {:?}", completed.error);
}

#[tokio::test]
async fn c02_recovers_between_pages_during_streaming_and_after_publication_without_repeating_rows()
{
    for mode in ["page", "staging", "published"] {
        let dir = Directory::new();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "inventory_process_child", "--nocapture"])
            .env("WF_DURABLE_CSV_CASE", mode)
            .env("WF_DURABLE_CSV_PATH", &dir.0)
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(30), child.output())
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
        let before = records(&dir.0.join("attempts.jsonl"));
        assert_eq!(before.len(), if mode == "page" { 100 } else { 201 });
        let source: ArtifactRef =
            serde_json::from_value(records(&dir.0.join("source.jsonl"))[0].clone()).unwrap();
        let store = Arc::new(
            modules::SqliteExecutionStore::open(
                dir.0.join("state.sqlite"),
                modules::SqliteOptions {
                    // Exactly source + three batch reports + one final report.
                    // Recovery cannot publish if it forgets to remove old staging.
                    max_artifacts: if mode == "staging" { 5 } else { 6 },
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let runtime = EngineRuntime::boot(
            builder(&dir.0, store.clone(), store.clone())
                .build()
                .unwrap(),
            BootOptions::default(),
        )
        .await
        .unwrap();
        let app = runtime.application();
        let receipt = start(&app, &source).await.unwrap();
        assert!(receipt.durable && receipt.duplicate);
        let completed = wait(&app, receipt.run_id.clone()).await;
        assert_eq!(
            completed.state,
            RunState::Succeeded,
            "{mode}: {:?}",
            completed.error
        );
        let value = completed.output.as_ref().unwrap();
        assert_eq!(value["rows"], 201);
        assert_eq!(value["succeeded"], 201);
        assert_eq!(value["failed"], 0);
        let reference: ArtifactRef = serde_json::from_value(value["report"].clone()).unwrap();
        if mode == "published" {
            // The published artifact whose acknowledgement was lost remains
            // pinned, even though its reference never reached the checkpoint.
            assert_eq!(
                store
                    .write("default", Box::pin(stream::empty()), "test/extra")
                    .await
                    .unwrap_err()
                    .code(),
                "resource.limit"
            );
        }
        let rows = report(&*store, &reference).await;
        assert_eq!(rows.len(), 201);
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row["index"], index);
            assert_eq!(row["output"]["quantity"], index + 1);
        }
        let attempts = records(&dir.0.join("attempts.jsonl"));
        assert_eq!(attempts.len(), 201, "{mode}: confirmed rows were repeated");
        assert_eq!(records(&dir.0.join("effects.jsonl")).len(), 201);
        let by_index: BTreeMap<_, _> = attempts
            .iter()
            .map(|r| (r["update"]["index"].as_u64().unwrap(), r["key"].clone()))
            .collect();
        assert_eq!(by_index.len(), 201);
        assert_eq!(&attempts[..before.len()], before);
        assert_eq!(
            completed.invocations["/nodes/publish"].attempts,
            if mode == "page" { 1 } else { 2 }
        );
        // A previously declared source is part of the receipt identity, even
        // when the visible workflow input has not changed.
        let plan = app.prepare(access(), definition()).await.unwrap();
        let mut changed = StartRunRequest::new(plan, json!({"source":source}));
        changed.options.require_durable = true;
        changed.options.receipt_key = Some("import-1".into());
        assert_eq!(
            app.start(access(), changed).await.unwrap_err().code(),
            "state.conflict"
        );
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
        let reopened = sqlite(&dir.0);
        let runtime = EngineRuntime::boot(
            builder(&dir.0, reopened.clone(), reopened.clone())
                .build()
                .unwrap(),
            BootOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            runtime
                .application()
                .status(access(), receipt.run_id)
                .await
                .unwrap(),
            completed
        );
        assert_eq!(report(&*reopened, &reference).await, rows);
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
        reopened.claim("retention-check").await.unwrap();
        reopened
            .collect("retention-check", i64::MAX as u64, &Limits::default())
            .await
            .unwrap();
        assert_eq!(
            reopened.read(&reference).await.err().unwrap().code(),
            "not_found"
        );
        reopened.release("retention-check").await.unwrap();
    }
}

#[tokio::test]
async fn durable_flags_without_a_shared_coordinator_are_rejected_before_dispatch() {
    let dir = Directory::new();
    let state = sqlite(&dir.0);
    // Even the same path through two independently constructed actors does not
    // form an atomic transaction domain. Clones of one provider do.
    let other = sqlite(&dir.0);
    let runtime = EngineRuntime::boot(
        builder(&dir.0, state, other).build().unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        runtime
            .application()
            .prepare(access(), definition())
            .await
            .err()
            .unwrap()
            .code(),
        "capability.unsupported"
    );
    assert!(records(&dir.0.join("effects.jsonl")).is_empty());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
