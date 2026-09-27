use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use workflow_forge::v2::*;

fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn definition() -> WorkflowDefinition {
    serde_json::from_value(json!({"format":WORKFLOW_FORMAT,"id":"failure.boundary","revision":"1","schema_dialect":SCHEMA_DIALECT,"input_schema":true,"output_schema":true,"entry":"one","nodes":[{"id":"one","kind":"operation","operation":{"id":"forge.data.identity","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}}}],"edges":[],"output":{"select":{"source":"node","node":"one","pointer":""}}})).unwrap()
}

/// Inject faults only at public store boundaries, without engine internals.
#[derive(Default)]
struct HookStore {
    inner: modules::MemoryExecutionStore,
    hold_create: AtomicBool,
    fail_scan: AtomicBool,
    created: Notify,
    release_create: Notify,
    saved: Mutex<Option<RunId>>,
    hold_finish: AtomicBool,
    finishing: Notify,
    release_finish: Notify,
}
impl ExecutionStore for HookStore {
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
            let id = run.id.clone();
            let result = self.inner.create(owner, run, receipt, limits).await?;
            if self.hold_create.load(Ordering::SeqCst) && matches!(result, CreateOutcome::Created) {
                *self.saved.lock().unwrap() = Some(id);
                self.created.notify_one();
                self.release_create.notified().await;
            }
            Ok(result)
        })
    }
    fn get<'a>(&'a self, id: &'a RunId) -> PortFuture<'a, Option<RunSnapshot>> {
        self.inner.get(id)
    }
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>> {
        if self.fail_scan.swap(false, Ordering::SeqCst) {
            Box::pin(async {
                Err(ForgeError::new(
                    "store.injected",
                    "Injected supervisor failure",
                ))
            })
        } else {
            self.inner.unfinished()
        }
    }
    fn commit<'a>(
        &'a self,
        owner: &'a str,
        expected: u64,
        next: RunSnapshot,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if next.state == RunState::Succeeded && self.hold_finish.swap(false, Ordering::SeqCst) {
                self.finishing.notify_one();
                self.release_finish.notified().await;
            }
            self.inner.commit(owner, expected, next).await
        })
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        self.inner.collect(owner, now, limits)
    }
}

#[tokio::test]
async fn accepted_run_survives_a_caller_dropped_before_the_start_ack() {
    let store = Arc::new(HookStore::default());
    store.hold_create.store(true, Ordering::SeqCst);
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let plan = app.prepare(access(), definition()).await.unwrap();
    let caller = app.clone();
    let request = tokio::spawn(async move {
        caller
            .start(access(), StartRunRequest::new(plan, json!("accepted")))
            .await
    });
    store.created.notified().await;
    let id = store.saved.lock().unwrap().clone().unwrap();
    request.abort();
    let _ = request.await;
    let run = tokio::time::timeout(Duration::from_secs(2), app.wait(access(), id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(run.output, Some(json!("accepted")));
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn supervisor_failure_removes_readiness_and_shutdown_still_releases_ownership() {
    let store = Arc::new(HookStore::default());
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    store.fail_scan.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while app.is_ready() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        app.catalog(&access()).unwrap_err().code(),
        "runtime.unavailable"
    );
    assert_eq!(
        runtime
            .shutdown(ShutdownOptions::default())
            .await
            .unwrap_err()
            .code(),
        "store.injected"
    );
    let recovered = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store)
            .build()
            .unwrap(),
        BootOptions {
            recovery: RecoveryPolicy::Resume,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    recovered
        .shutdown(ShutdownOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn receipt_reservation_outlives_expired_results_without_reexecuting() {
    let limits = Limits {
        retention_ms: 1,
        receipt_ttl_ms: 60_000,
        receipt_count: 1,
        ..Default::default()
    };
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard().limits(limits).build().unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let plan = app.prepare(access(), definition()).await.unwrap();
    let request = || StartRunRequest {
        plan: plan.clone(),
        input: json!(1),
        options: StartOptions {
            receipt_key: Some("key".into()),
            ..Default::default()
        },
    };
    let receipt = app.start(access(), request()).await.unwrap();
    // The first run may finish and expire before a read; the retained receipt is authoritative.
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        app.status(access(), receipt.run_id.clone())
            .await
            .unwrap_err()
            .code(),
        "not_found"
    );
    let duplicate = app.start(access(), request()).await.unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.run_id, receipt.run_id);
    let mut next = request();
    next.options.receipt_key = Some("another".into());
    assert_eq!(
        app.start(access(), next).await.unwrap_err().code(),
        "admission.full"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn global_output_validation_fails_the_run_without_coercing_data() {
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard().build().unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let mut definition = definition();
    definition.output_schema = json!({"type":"integer"});
    let plan = app.prepare(access(), definition).await.unwrap();
    let receipt = app
        .start(access(), StartRunRequest::new(plan, json!("42")))
        .await
        .unwrap();
    let run = app.wait(access(), receipt.run_id).await.unwrap();
    assert_eq!(run.state, RunState::Failed);
    let error = run.error.unwrap();
    assert_eq!(error.code(), "data.invalid");
    assert_eq!(error.diagnostics[0].location.field, "/output");
    assert!(run.output.is_none());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

fn sample_snapshot() -> RunSnapshot {
    RunSnapshot {
        waits: Default::default(),
        artifacts: vec![],
        checkpoint_format: CHECKPOINT_FORMAT,
        id: RunId("conformance.run".into()),
        scope: "test".into(),
        actor: "host".into(),
        resources: Default::default(),
        revision: 0,
        definition: definition(),
        package: ResolvedPackage {
            definitions: vec![definition()],
            ..Default::default()
        },
        input: json!({"original":true}),
        state: RunState::Accepted,
        invocations: Default::default(),
        output: None,
        error: None,
        created_at_ms: 100,
        deadline_at_ms: 1000,
        finished_at_ms: None,
        cancel_requested: false,
        audit: Vec::new(),
        unresolved_effects: Vec::new(),
    }
}

#[tokio::test]
async fn public_store_conformance_runs_without_engine_internals() {
    let sample = sample_snapshot();
    workflow_forge_conformance::execution_store(
        &modules::MemoryExecutionStore::default(),
        sample.clone(),
    )
    .await
    .unwrap();
    workflow_forge_conformance::execution_store(&HookStore::default(), sample)
        .await
        .unwrap();
}

#[tokio::test]
async fn artifact_provider_bounds_storage_and_preserves_scope() {
    use futures::{StreamExt, stream};
    let provider = modules::MemoryArtifacts::new(65_537);
    let content = Box::pin(stream::iter((0..65_537).map(|_| Ok(vec![7]))));
    let reference = provider
        .write("test", content, "application/octet-stream")
        .await
        .unwrap();
    let mut content = provider.read(&reference).await.unwrap();
    assert_eq!(content.next().await.unwrap().unwrap(), vec![7; 65_536]);
    assert_eq!(content.next().await.unwrap().unwrap(), vec![7]);
    assert!(content.next().await.is_none());
    let mut forged = reference.clone();
    forged.scope = "foreign".into();
    assert!(provider.read(&forged).await.is_err());
    assert_eq!(
        provider
            .write(
                "test",
                Box::pin(stream::once(async { Ok(vec![1]) })),
                "application/octet-stream"
            )
            .await
            .unwrap_err()
            .code(),
        "resource.limit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projected_and_full_commits_share_one_cas_and_coherent_counters() {
    let store = Arc::new(modules::MemoryExecutionStore::default());
    let initial = sample_snapshot();
    let id = initial.id.clone();
    store.claim("owner").await.unwrap();
    store
        .create("owner", initial, None, &Limits::default())
        .await
        .unwrap();
    let mut running = store.get(&id).await.unwrap().unwrap();
    running.state = RunState::Running;
    running.revision = 1;
    store.commit("owner", 0, running).await.unwrap();
    let record = InvocationRecord {
        id: "logical".into(),
        attempt_id: "attempt".into(),
        attempts: 1,
        state: InvocationState::Unknown,
        input: json!({"value":1}),
        output: None,
        error: None,
        operation: Some(OperationRevision::new("test.write", "1", "r1")),
        config: json!({}),
        effect_key: Some("effect".into()),
        retry: RetryPolicy::default(),
        next_attempt_at_ms: None,
        certainty: EffectCertainty::Unknown,
        control: None,
    };
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = tokio::task::JoinSet::new();
    for key in ["/nodes/left", "/nodes/right"] {
        let store = store.clone();
        let id = id.clone();
        let record = record.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            store.commit_invocation("owner", &id, 1, key, record).await
        });
    }
    barrier.wait().await;
    let mut wins = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(_) => wins += 1,
            Err(error) => assert_eq!(error.code(), "state.conflict"),
        }
    }
    assert_eq!(wins, 1);
    let before = store.get(&id).await.unwrap().unwrap();
    assert_eq!(before.invocations.len(), 1);
    assert_eq!(
        store.view(&id, None).await.unwrap(),
        Some(before.view(None))
    );
    let (key, mut confirmed) = before
        .invocations
        .iter()
        .next()
        .map(|(k, v)| (k.clone(), v.clone()))
        .unwrap();
    confirmed.state = InvocationState::Succeeded;
    confirmed.certainty = EffectCertainty::Applied;
    confirmed.output = Some(json!("confirmed"));
    confirmed.input = Value::Null;
    let mut cancelled = before.clone();
    cancelled.revision += 1;
    cancelled.cancel_requested = true;
    cancelled.state = RunState::Cancelling;
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let full = {
        let store = store.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store.commit("owner", 2, cancelled).await
        })
    };
    let projected = {
        let store = store.clone();
        let barrier = barrier.clone();
        let id = id.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .commit_invocation("owner", &id, 2, &key, confirmed)
                .await
        })
    };
    barrier.wait().await;
    let full = full.await.unwrap();
    let projected = projected.await.unwrap();
    assert_ne!(full.is_ok(), projected.is_ok());
    let current = store.get(&id).await.unwrap().unwrap();
    assert_eq!(current.revision, 3);
    let view = store.view(&id, None).await.unwrap().unwrap();
    assert_eq!(view, current.view(None));
    assert_eq!(view.head.unresolved_invocations, usize::from(full.is_ok()));
    assert_eq!(current.cancel_requested, full.is_ok());
    store.release("owner").await.unwrap();
}

#[tokio::test]
async fn a_cancel_committed_before_final_result_is_rechecked_after_cas_conflict() {
    let store = Arc::new(HookStore::default());
    store.hold_finish.store(true, Ordering::SeqCst);
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let plan = app.prepare(access(), definition()).await.unwrap();
    let receipt = app
        .start(
            access(),
            StartRunRequest::new(plan, json!("already-computed")),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), store.finishing.notified())
        .await
        .unwrap();
    app.cancel(access(), receipt.run_id.clone()).await.unwrap();
    store.release_finish.notify_one();
    let run = tokio::time::timeout(Duration::from_secs(3), app.wait(access(), receipt.run_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.state, RunState::Cancelled);
    assert!(run.output.is_none());
    assert_eq!(
        run.invocations["/nodes/one"].state,
        InvocationState::Succeeded
    );
    assert_eq!(
        run.invocations["/nodes/one"].output,
        Some(json!("already-computed"))
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn owned_call_dropped_during_acceptance_is_cancelled_after_acceptance_settles() {
    let store = Arc::new(HookStore::default());
    store.hold_create.store(true, Ordering::SeqCst);
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let mut workflow = definition();
    workflow.nodes[0].instruction = Instruction::Timer {
        duration_ms: 60_000,
    };
    let plan = app.prepare(access(), workflow).await.unwrap();
    let caller = app.clone();
    let request = tokio::spawn(async move {
        caller
            .execute(
                access(),
                StartRunRequest::new(plan, json!(42)),
                CancellationToken::new(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), store.created.notified())
        .await
        .unwrap();
    let id = store.saved.lock().unwrap().clone().unwrap();
    request.abort();
    let _ = request.await;
    store.release_create.notify_one();
    let run = tokio::time::timeout(Duration::from_secs(2), app.wait(access(), id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.state, RunState::Cancelled);
    let report = runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    assert!(!report.forced);
    assert!(report.pending.is_empty());
}
