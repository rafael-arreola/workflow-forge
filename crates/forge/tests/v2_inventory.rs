use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use workflow_forge::v2::*;
use workflow_forge_reference_module::inventory::{
    InventoryDestination, InventoryUpdate, MemoryInventory, inventory_operations,
};

fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn definition() -> WorkflowDefinition {
    let value: Value = serde_json::from_str(include_str!(
        "../../../examples/workflows/inventory_import.v2.json"
    ))
    .unwrap();
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/2/workflow.schema.json")).unwrap();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&value));
    serde_json::from_value(value).unwrap()
}
async fn artifact(store: &modules::MemoryArtifacts, bytes: Vec<u8>) -> ArtifactRef {
    store
        .write(
            "default",
            Box::pin(stream::once(async { Ok(bytes) })),
            "text/csv",
        )
        .await
        .unwrap()
}
async fn read(store: &modules::MemoryArtifacts, reference: &ArtifactRef) -> Vec<u8> {
    let mut stream = store.read(reference).await.unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend(chunk.unwrap());
    }
    bytes
}
async fn boot(
    destination: Arc<dyn InventoryDestination>,
    artifacts: Arc<modules::MemoryArtifacts>,
) -> EngineRuntime {
    let mut builder = WorkflowBuilder::standard().artifact_store(artifacts);
    builder
        .register_bundle(inventory_operations(destination))
        .unwrap();
    EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
        .await
        .unwrap()
}
async fn run(
    app: &WorkflowApplication,
    definition: WorkflowDefinition,
    source: ArtifactRef,
) -> RunSnapshot {
    let plan = app.prepare(access(), definition).await.unwrap();
    let receipt = app
        .start(
            access(),
            StartRunRequest::new(plan, json!({"source":source})),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), app.wait(access(), receipt.run_id))
        .await
        .unwrap()
        .unwrap()
}
async fn report(store: &modules::MemoryArtifacts, run: &RunSnapshot) -> Vec<Value> {
    let reference: ArtifactRef =
        serde_json::from_value(run.output.as_ref().unwrap()["report"].clone()).unwrap();
    assert_eq!(reference.media_type, "application/x-ndjson");
    String::from_utf8(read(store, &reference).await)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn c02_csv_validation_collects_rows_and_publishes_correlated_report() {
    let artifacts = Arc::new(modules::MemoryArtifacts::default());
    let destination = Arc::new(MemoryInventory::default());
    let source = artifact(
        &artifacts,
        b"sku,quantity\nA-1,2\nB-2,error\nC-3,4\n".to_vec(),
    )
    .await;
    let runtime = boot(destination.clone(), artifacts.clone()).await;
    let run = run(&runtime.application(), definition(), source).await;
    assert_eq!(run.state, RunState::Succeeded, "{:?}", run.error);
    let summary = run.output.as_ref().unwrap();
    assert_eq!(summary["rows"], 3);
    assert_eq!(summary["succeeded"], 2);
    assert_eq!(summary["failed"], 1);
    let rows = report(&artifacts, &run).await;
    assert_eq!(rows.len(), 3);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row["index"], i);
        assert_eq!(row["line"], i + 2);
    }
    assert_eq!(rows[0]["status"], "succeeded");
    assert_eq!(rows[1]["status"], "failed");
    assert_eq!(rows[1]["error"]["diagnostics"][0]["code"], "data.invalid");
    assert_eq!(rows[2]["output"]["sku"], "C-3");
    let effects = destination.snapshot().unwrap();
    assert_eq!(effects.attempts, 2);
    assert_eq!(effects.effects.len(), 2);
    assert!(effects.effects.values().all(|row| row.sku != "B-2"));
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn csv_header_encoding_and_global_row_limit_fail_before_destination_effects() {
    let artifacts = Arc::new(modules::MemoryArtifacts::default());
    let destination = Arc::new(MemoryInventory::default());
    let runtime = boot(destination.clone(), artifacts.clone()).await;
    let excessive = format!("sku,quantity\n{}", "A-1,1\n".repeat(10_001));
    for bytes in [
        b"quantity,sku\n1,A-1\n".to_vec(),
        vec![],
        b"sku,quantity\n\xff,1\n".to_vec(),
        excessive.into_bytes(),
        vec![b'x'; 4 * 1024 * 1024 + 1],
    ] {
        let source = artifact(&artifacts, bytes).await;
        let run = run(&runtime.application(), definition(), source).await;
        assert_eq!(run.state, RunState::Failed, "{:?}", run.error);
        assert!(run.output.is_none());
    }
    assert_eq!(destination.snapshot().unwrap().attempts, 0);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn empty_csv_and_quoted_records_keep_their_source_line_and_distinct_repeated_skus() {
    let artifacts = Arc::new(modules::MemoryArtifacts::default());
    let destination = Arc::new(MemoryInventory::default());
    let runtime = boot(destination.clone(), artifacts.clone()).await;
    let empty = run(
        &runtime.application(),
        definition(),
        artifact(&artifacts, b"sku,quantity\n".to_vec()).await,
    )
    .await;
    assert_eq!(empty.output.as_ref().unwrap()["rows"], 0);
    assert!(report(&artifacts, &empty).await.is_empty());
    let source = artifact(
        &artifacts,
        b"sku,quantity\n\"A,1\",2\n\"A,1\",2\n\"multi\nline\",3\nbad,2,extra\n".to_vec(),
    )
    .await;
    let completed = run(&runtime.application(), definition(), source).await;
    assert_eq!(
        completed.state,
        RunState::Succeeded,
        "{:?}",
        completed.error
    );
    let rows = report(&artifacts, &completed).await;
    assert_eq!(rows[2]["sku"], "multi\nline");
    assert_eq!(rows[2]["line"], 4);
    assert_eq!(rows[3]["line"], 6);
    assert_eq!(rows[3]["status"], "failed");
    let effects = destination.snapshot().unwrap();
    assert_eq!(effects.effects.len(), 3);
    assert_eq!(
        effects.effects.values().filter(|r| r.sku == "A,1").count(),
        2
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

struct ObservedDestination {
    inner: Arc<MemoryInventory>,
    lost_once: Mutex<BTreeSet<String>>,
    lose_index: Option<usize>,
    delay_first: bool,
    active: AtomicUsize,
    peak: AtomicUsize,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ObservedDestination {
    fn new(lose_index: Option<usize>, delay_first: bool) -> Self {
        Self {
            inner: Arc::new(MemoryInventory::default()),
            lost_once: Mutex::new(BTreeSet::new()),
            lose_index,
            delay_first,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
}
impl InventoryDestination for ObservedDestination {
    fn apply<'a>(
        &'a self,
        key: &'a str,
        update: InventoryUpdate,
    ) -> PortFuture<'a, InventoryUpdate, OperationError> {
        Box::pin(async move {
            let count = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _active = Active(&self.active);
            self.peak.fetch_max(count, Ordering::SeqCst);
            if self.delay_first && update.index % 100 == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            let receipt = self.inner.apply(key, update).await?;
            if self.lose_index == Some(receipt.index)
                && self.lost_once.lock().unwrap().insert(key.into())
            {
                return Err(OperationError {
                    code: "reference.inventory.lost_ack".into(),
                    class: ErrorClass::Transient,
                    certainty: EffectCertainty::Unknown,
                    message: "Destination applied the row but response was lost".into(),
                });
            }
            Ok(receipt)
        })
    }
    fn inspect<'a>(&'a self, key: &'a str) -> PortFuture<'a, EffectInspection> {
        self.inner.inspect(key)
    }
}
fn csv(rows: usize) -> Vec<u8> {
    let mut csv = "sku,quantity\n".to_owned();
    for i in 0..rows {
        csv.push_str(&format!("SKU-{i},{}\n", i + 1));
    }
    csv.into_bytes()
}

#[tokio::test]
async fn multiple_pages_preserve_order_bound_concurrency_and_retry_without_duplicate_effect() {
    let artifacts = Arc::new(modules::MemoryArtifacts::default());
    let destination = Arc::new(ObservedDestination::new(Some(103), true));
    let runtime = boot(destination.clone(), artifacts.clone()).await;
    let source = artifact(&artifacts, csv(201)).await;
    let completed = run(&runtime.application(), definition(), source).await;
    assert_eq!(
        completed.state,
        RunState::Succeeded,
        "{:?}",
        completed.error
    );
    assert_eq!(completed.output.as_ref().unwrap()["rows"], 201);
    assert_eq!(completed.output.as_ref().unwrap()["succeeded"], 201);
    assert_eq!(destination.inner.snapshot().unwrap().effects.len(), 201);
    assert_eq!(destination.inner.snapshot().unwrap().attempts, 202);
    assert!((2..=4).contains(&destination.peak.load(Ordering::SeqCst)));
    let rows = report(&artifacts, &completed).await;
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["index"], index);
        assert_eq!(row["output"]["index"], index);
    }
    let pages: Vec<_> = completed
        .invocations
        .values()
        .filter(|r| {
            r.operation
                .as_ref()
                .is_some_and(|o| o.id == "reference.inventory.read_page")
        })
        .collect();
    assert_eq!(pages.len(), 3);
    assert_eq!(
        pages
            .iter()
            .map(|r| r.output.as_ref().unwrap()["rows"].as_array().unwrap().len())
            .sum::<usize>(),
        201
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn unknown_row_leaves_a_partial_report_then_resolution_resumes_without_repeating_previous_page()
 {
    let artifacts = Arc::new(modules::MemoryArtifacts::default());
    let destination = Arc::new(ObservedDestination::new(Some(100), false));
    let runtime = boot(destination.clone(), artifacts.clone()).await;
    let app = runtime.application();
    let mut d = serde_json::to_value(definition()).unwrap();
    d["nodes"][0]["body"]["nodes"][1]["body"]["nodes"][0]["retry"]["max_attempts"] = json!(1);
    let blocked = run(
        &app,
        serde_json::from_value(d).unwrap(),
        artifact(&artifacts, csv(101)).await,
    )
    .await;
    assert_eq!(blocked.state, RunState::Blocked, "{:?}", blocked.error);
    assert!(blocked.output.is_none());
    assert!(!blocked.invocations.contains_key("/nodes/publish"));
    let ControlFrame::Loop { state, iteration } = blocked.invocations["/nodes/import"]
        .control
        .as_ref()
        .unwrap()
    else {
        panic!("loop checkpoint")
    };
    assert_eq!(*iteration, 1);
    assert_eq!(state["summary"]["rows"], 100);
    let partial: ArtifactRef = serde_json::from_value(state["report"].clone()).unwrap();
    let partial: Value = serde_json::from_slice(&read(&artifacts, &partial).await).unwrap();
    assert_eq!(partial["partial"], true);
    assert_eq!(partial["summary"]["succeeded"], 100);
    let uncertain: Vec<_> = blocked
        .invocations
        .values()
        .filter(|r| r.state == InvocationState::Unknown)
        .collect();
    assert_eq!(uncertain.len(), 1);
    assert_eq!(uncertain[0].input["index"], 100);
    let EffectInspection::Applied { output, evidence } = app
        .inspect_effect(access(), blocked.id.clone(), uncertain[0].id.clone())
        .await
        .unwrap()
    else {
        panic!("destination contains the row")
    };
    app.reconcile(
        access(),
        ReconcileCommand {
            command_id: "finish-last-row".into(),
            run_id: blocked.id.clone(),
            invocation_id: uncertain[0].id.clone(),
            expected_revision: blocked.revision,
            observed_attempt: uncertain[0].attempt_id.clone(),
            resolution: EffectResolution::ConfirmApplied { output, evidence },
        },
    )
    .await
    .unwrap();
    let done = tokio::time::timeout(Duration::from_secs(15), app.wait(access(), blocked.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.error);
    assert_eq!(done.output.as_ref().unwrap()["rows"], 101);
    assert_eq!(destination.inner.snapshot().unwrap().attempts, 101);
    assert_eq!(destination.inner.snapshot().unwrap().effects.len(), 101);
    assert_eq!(report(&artifacts, &done).await.len(), 101);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
