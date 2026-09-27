use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;
use workflow_forge::prelude::*;
use workflow_forge_reference_module::{StaticDirectory, customer_operations};

#[derive(Clone, Copy)]
enum Behavior {
    LostAck,
    LostThenNotApplied,
    UnknownApplied,
    LateApplied,
    InvalidOutput,
    BeforeSend,
    UnknownNotApplied,
}
#[derive(Default)]
struct RemoteState {
    calls: Vec<Invocation>,
    orders: BTreeMap<String, Value>,
}
struct Remote {
    state: Mutex<RemoteState>,
    release: Notify,
    started: Notify,
}
impl Default for Remote {
    fn default() -> Self {
        Self {
            state: Mutex::new(RemoteState::default()),
            release: Notify::new(),
            started: Notify::new(),
        }
    }
}
struct CreateOrder {
    descriptor: OperationDescriptor,
    remote: Arc<Remote>,
    behavior: Behavior,
}
fn operation_error(certainty: EffectCertainty) -> OperationError {
    OperationError {
        code: "reference.orders.connection".into(),
        class: ErrorClass::Transient,
        certainty,
        message: "Destination response unavailable".into(),
    }
}
impl Operation for CreateOrder {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let (ordinal, output) = {
                let mut remote = self.remote.state.lock().unwrap();
                let ordinal = remote.calls.len();
                let key = invocation
                    .effect_key
                    .clone()
                    .expect("writes have a logical effect key");
                remote.calls.push(invocation);
                if ordinal > 0 && matches!(self.behavior, Behavior::LostThenNotApplied) {
                    return Err(operation_error(EffectCertainty::NotApplied));
                }
                if ordinal == 0
                    && matches!(
                        self.behavior,
                        Behavior::BeforeSend | Behavior::UnknownNotApplied
                    )
                {
                    return Err(operation_error(
                        if matches!(self.behavior, Behavior::BeforeSend) {
                            EffectCertainty::NotApplied
                        } else {
                            EffectCertainty::Unknown
                        },
                    ));
                }
                let serial = remote.orders.len() + 1;
                let output = remote
                    .orders
                    .entry(key)
                    .or_insert_with(|| json!({"order_id":format!("O-{serial}")}))
                    .clone();
                (ordinal, output)
            };
            self.remote.started.notify_one();
            match self.behavior {
                Behavior::LostAck | Behavior::LostThenNotApplied if ordinal == 0 => {
                    Err(operation_error(EffectCertainty::Unknown))
                }
                Behavior::UnknownApplied => Err(operation_error(EffectCertainty::Unknown)),
                Behavior::LateApplied => {
                    self.remote.release.notified().await;
                    Ok(OperationOutput::json(json!({"order_id":"late-ignored"})))
                }
                Behavior::InvalidOutput => Ok(OperationOutput::json(json!({"unexpected":true}))),
                _ => Ok(OperationOutput::json(output)),
            }
        })
    }
}
struct Inspector {
    revision: OperationRevision,
    remote: Arc<Remote>,
}
fn evidence() -> EffectEvidence {
    EffectEvidence {
        authority: "reference.destination".into(),
        reference: "inspection/1".into(),
        note: "Read-only authoritative query".into(),
    }
}
impl EffectInspector for Inspector {
    fn operation(&self) -> &OperationRevision {
        &self.revision
    }
    fn inspect<'a>(
        &'a self,
        _: OperationContext,
        invocation: Invocation,
    ) -> PortFuture<'a, EffectInspection> {
        Box::pin(async move {
            let remote = self.remote.state.lock().unwrap();
            Ok(
                match invocation
                    .effect_key
                    .as_ref()
                    .and_then(|key| remote.orders.get(key))
                {
                    Some(output) => EffectInspection::Applied {
                        output: output.clone(),
                        evidence: evidence(),
                    },
                    None => EffectInspection::NotApplied {
                        evidence: evidence(),
                        quiescent: true,
                    },
                },
            )
        })
    }
}
fn bundle(remote: Arc<Remote>, behavior: Behavior, repetition: Repetition) -> OperationBundle {
    let revision = OperationRevision::new("reference.create_order", "1", "r1");
    let descriptor = OperationDescriptor {
        revision: revision.clone(),
        schema_dialect: SCHEMA_DIALECT.into(),
        config_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","additionalProperties":false}),
        input_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["customer","items"],"properties":{"customer":{"type":"string","minLength":1},"items":{"type":"array","minItems":1}},"additionalProperties":false}),
        output_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["order_id"],"properties":{"order_id":{"type":"string","minLength":1}},"additionalProperties":false}),
        effect: EffectKind::Write,
        repetition,
        reconciliation: true,
        required_resources: Default::default(),
        description: "Create an order at a controlled destination".into(),
        examples: Vec::new(),
    };
    OperationBundle {
        module: ModuleDescriptor {
            id: "reference.orders".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![revision.clone()],
        },
        operations: vec![Arc::new(CreateOrder {
            descriptor,
            remote: remote.clone(),
            behavior,
        })],
        inspectors: vec![Arc::new(Inspector { revision, remote })],
    }
}
fn definition() -> WorkflowDefinition {
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../examples/workflows/customer_lookup.v2.json"
    ))
    .unwrap();
    value["id"] = json!("reference.create_order");
    value["output_schema"] = json!({"type":"object","required":["order_id"],"properties":{"order_id":{"type":"string"}},"additionalProperties":false});
    value["nodes"].as_array_mut().unwrap().push(json!({"id":"create","kind":"operation","operation":{"id":"reference.create_order","contract":"1","implementation":"r1"},"config":{},"input":{"object":{"customer":{"select":{"source":"node","node":"normalize","pointer":""}},"items":{"select":{"source":"input","pointer":"/items"}}}},"retry":{"max_attempts":3,"initial_delay_ms":0,"max_delay_ms":0,"jitter":false}}));
    value["edges"]
        .as_array_mut()
        .unwrap()
        .push(json!({"from":"lookup","to":"create"}));
    value["output"] = json!({"select":{"source":"node","node":"create","pointer":""}});
    serde_json::from_value(value).unwrap()
}
fn input() -> Value {
    json!({"request_id":"req-17","customer":" C-9 ","items":[{"sku":"A-1","quantity":2}]})
}
fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn composition(remote: Arc<Remote>, behavior: Behavior, repetition: Repetition) -> WorkflowBuilder {
    let mut builder = WorkflowBuilder::standard().limits(Limits {
        attempt_timeout_ms: 100,
        ..Default::default()
    });
    builder
        .register_bundle(customer_operations(Arc::new(StaticDirectory(
            BTreeMap::from([("C-9".into(), true)]),
        ))))
        .unwrap();
    builder
        .register_bundle(bundle(remote, behavior, repetition))
        .unwrap();
    builder
}
async fn boot(remote: Arc<Remote>, behavior: Behavior, repetition: Repetition) -> EngineRuntime {
    EngineRuntime::boot(
        composition(remote, behavior, repetition).build().unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap()
}
async fn start(app: &WorkflowApplication) -> RunSnapshot {
    start_with(app, definition(), input()).await
}
async fn start_with(
    app: &WorkflowApplication,
    definition: WorkflowDefinition,
    input: Value,
) -> RunSnapshot {
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/2/workflow.schema.json")).unwrap();
    assert!(
        jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&serde_json::to_value(&definition).unwrap())
    );
    let plan = app.prepare(access(), definition).await.unwrap();
    let receipt = app
        .start(access(), StartRunRequest::new(plan, input))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), app.wait(access(), receipt.run_id))
        .await
        .unwrap()
        .unwrap()
}
fn command(run: &RunSnapshot, id: &str, resolution: EffectResolution) -> ReconcileCommand {
    command_for(run, "/nodes/create", id, resolution)
}
fn command_for(
    run: &RunSnapshot,
    key: &str,
    id: &str,
    resolution: EffectResolution,
) -> ReconcileCommand {
    let invocation = &run.invocations[key];
    ReconcileCommand {
        command_id: id.into(),
        run_id: run.id.clone(),
        invocation_id: invocation.id.clone(),
        expected_revision: run.revision,
        observed_attempt: invocation.attempt_id.clone(),
        resolution,
    }
}

#[tokio::test]
async fn c01b_lost_ack_retries_with_one_effect_and_distinct_attempts() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote.clone(), Behavior::LostAck, Repetition::Keyed).await;
    let app = runtime.application();
    let run = start(&app).await;
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(run.output, Some(json!({"order_id":"O-1"})));
    {
        let state = remote.state.lock().unwrap();
        assert_eq!(state.calls.len(), 2);
        assert_eq!(state.orders.len(), 1);
        assert_eq!(state.calls[0].id, state.calls[1].id);
        assert_eq!(state.calls[0].effect_key, state.calls[1].effect_key);
        assert_ne!(state.calls[0].attempt_id, state.calls[1].attempt_id);
        assert_eq!(state.calls[0].input["customer"], json!("C-9"));
    }
    let another = start(&app).await;
    assert_eq!(another.output, Some(json!({"order_id":"O-2"})));
    assert_ne!(
        run.invocations["/nodes/create"].id,
        another.invocations["/nodes/create"].id
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn definitive_pre_dispatch_failure_can_retry_a_non_idempotent_operation() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote.clone(), Behavior::BeforeSend, Repetition::Unsafe).await;
    let run = start(&runtime.application()).await;
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(run.invocations["/nodes/create"].attempts, 2);
    assert_eq!(remote.state.lock().unwrap().orders.len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn unsafe_unknown_blocks_then_inspection_and_atomic_resolution_confirm_once() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote.clone(), Behavior::UnknownApplied, Repetition::Unsafe).await;
    let app = runtime.application();
    let run = start(&app).await;
    assert_eq!(run.state, RunState::Blocked);
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    let inspection = app
        .inspect_effect(
            access(),
            run.id.clone(),
            run.invocations["/nodes/create"].id.clone(),
        )
        .await
        .unwrap();
    let EffectInspection::Applied { output, evidence } = inspection else {
        panic!("expected applied evidence")
    };
    let resolve = command(
        &run,
        "resolve",
        EffectResolution::ConfirmApplied { output, evidence },
    );
    let receipt = app.reconcile(access(), resolve.clone()).await.unwrap();
    assert_eq!(receipt.status, ResolutionStatus::Applied);
    let done = app.wait(access(), run.id.clone()).await.unwrap();
    assert_eq!(done.state, RunState::Succeeded);
    assert_eq!(done.output, Some(json!({"order_id":"O-1"})));
    assert_eq!(
        app.reconcile(access(), resolve.clone()).await.unwrap(),
        receipt
    );
    let mut altered = resolve;
    altered.expected_revision += 1;
    assert_eq!(
        app.reconcile(access(), altered).await.unwrap_err().code(),
        "state.conflict"
    );
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    assert!(matches!(&done.audit[0],AuditEntry::Resolution(a) if a.actor=="host"));
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn quiescence_is_required_before_authorizing_another_unsafe_attempt() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(
        remote.clone(),
        Behavior::UnknownNotApplied,
        Repetition::Unsafe,
    )
    .await;
    let app = runtime.application();
    let run = start(&app).await;
    let receipt = app
        .reconcile(
            access(),
            command(
                &run,
                "insufficient",
                EffectResolution::ConfirmNotApplied {
                    evidence: evidence(),
                    quiescent: false,
                    retry: true,
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(receipt.status, ResolutionStatus::StillBlocked);
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    let current = app.status(access(), run.id.clone()).await.unwrap();
    let receipt = app
        .reconcile(
            access(),
            command(
                &current,
                "definitive",
                EffectResolution::ConfirmNotApplied {
                    evidence: evidence(),
                    quiescent: true,
                    retry: true,
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(receipt.status, ResolutionStatus::RetryReady);
    let done = app.wait(access(), run.id).await.unwrap();
    assert_eq!(done.state, RunState::Succeeded);
    {
        let state = remote.state.lock().unwrap();
        assert_eq!(state.calls.len(), 2);
        assert_eq!(state.calls[0].id, state.calls[1].id);
        assert_ne!(state.calls[0].attempt_id, state.calls[1].attempt_id);
    }
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn invalid_output_after_effect_never_turns_into_automatic_retry_or_non_application() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote.clone(), Behavior::InvalidOutput, Repetition::Keyed).await;
    let app = runtime.application();
    let run = start(&app).await;
    assert_eq!(run.state, RunState::Blocked);
    assert_eq!(
        run.invocations["/nodes/create"].certainty,
        EffectCertainty::Applied
    );
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    let invalid = app
        .reconcile(
            access(),
            command(
                &run,
                "bad-output",
                EffectResolution::ConfirmApplied {
                    output: json!({"wrong":1}),
                    evidence: evidence(),
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status, ResolutionStatus::StillBlocked);
    let current = app.status(access(), run.id.clone()).await.unwrap();
    let contradiction = app
        .reconcile(
            access(),
            command(
                &current,
                "contradiction",
                EffectResolution::ConfirmNotApplied {
                    evidence: evidence(),
                    quiescent: true,
                    retry: true,
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(contradiction.status, ResolutionStatus::StillBlocked);
    let current = app.status(access(), run.id.clone()).await.unwrap();
    app.reconcile(
        access(),
        command(
            &current,
            "valid",
            EffectResolution::ConfirmApplied {
                output: json!({"order_id":"O-1"}),
                evidence: evidence(),
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        app.wait(access(), run.id).await.unwrap().state,
        RunState::Succeeded
    );
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn competing_resolutions_use_cas_and_stopping_tracking_requires_a_separate_grant() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote, Behavior::UnknownApplied, Repetition::Unsafe).await;
    let app = runtime.application();
    let run = start(&app).await;
    let a = command(
        &run,
        "a",
        EffectResolution::RecordInconclusive {
            reason: "first query".into(),
            evidence: None,
        },
    );
    let b = command(
        &run,
        "b",
        EffectResolution::RecordInconclusive {
            reason: "second query".into(),
            evidence: None,
        },
    );
    let (a, b) = tokio::join!(app.reconcile(access(), a), app.reconcile(access(), b));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(
        a.err().or_else(|| b.err()).unwrap().code(),
        "state.conflict"
    );
    app.cancel(access(), run.id.clone()).await.unwrap();
    let blocked = app.wait(access(), run.id).await.unwrap();
    assert_eq!(blocked.state, RunState::Blocked);
    let stop = command(
        &blocked,
        "stop",
        EffectResolution::StopTracking {
            reason: "Host accepts an unresolved effect".into(),
            evidence: None,
        },
    );
    let mut unauthorized = access();
    unauthorized.permissions.remove(&Permission::StopTracking);
    assert_eq!(
        app.reconcile(unauthorized, stop.clone())
            .await
            .unwrap_err()
            .code(),
        "access.denied"
    );
    assert_eq!(
        app.reconcile(AccessContext::trusted("foreign"), stop.clone())
            .await
            .unwrap_err()
            .code(),
        "access.denied"
    );
    app.reconcile(access(), stop).await.unwrap();
    let done = app.status(access(), blocked.id).await.unwrap();
    assert_eq!(done.state, RunState::Cancelled);
    assert_eq!(done.unresolved_effects.len(), 1);
    assert_eq!(done.error.unwrap().code(), "effect.unresolved");
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn late_completion_is_audited_without_overwriting_a_resolved_terminal_result() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote.clone(), Behavior::LateApplied, Repetition::Unsafe).await;
    let app = runtime.application();
    let run = start(&app).await;
    assert_eq!(run.state, RunState::Blocked);
    app.reconcile(
        access(),
        command(
            &run,
            "confirm",
            EffectResolution::ConfirmApplied {
                output: json!({"order_id":"O-1"}),
                evidence: evidence(),
            },
        ),
    )
    .await
    .unwrap();
    let done = app.wait(access(), run.id.clone()).await.unwrap();
    assert_eq!(done.output, Some(json!({"order_id":"O-1"})));
    remote.release.notify_one();
    let observed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let current = app.status(access(), run.id.clone()).await.unwrap();
            if current
                .audit
                .iter()
                .any(|a| matches!(a, AuditEntry::Late(_)))
            {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observed.state, RunState::Succeeded);
    assert_eq!(observed.output, done.output);
    assert!(
        matches!(&observed.audit[1],AuditEntry::Late(late) if late.attempt_id==run.invocations["/nodes/create"].attempt_id && late.output==Some(json!({"order_id":"late-ignored"})))
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn a_later_not_applied_attempt_does_not_erase_an_earlier_unknown_effect() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(
        remote.clone(),
        Behavior::LostThenNotApplied,
        Repetition::Keyed,
    )
    .await;
    let run = start(&runtime.application()).await;
    assert_eq!(run.state, RunState::Blocked);
    assert_eq!(
        run.invocations["/nodes/create"].certainty,
        EffectCertainty::Unknown
    );
    assert_eq!(run.invocations["/nodes/create"].attempts, 3);
    assert_eq!(remote.state.lock().unwrap().orders.len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn cancelling_during_retry_delay_preserves_the_unconfirmed_write() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote.clone(), Behavior::LostAck, Repetition::Keyed).await;
    let app = runtime.application();
    let mut definition = definition();
    if let Instruction::Operation { retry, .. } = &mut definition.nodes[2].instruction {
        retry.initial_delay_ms = 5000;
        retry.max_delay_ms = 5000;
    }
    let plan = app.prepare(access(), definition).await.unwrap();
    let id = app
        .start(access(), StartRunRequest::new(plan, input()))
        .await
        .unwrap()
        .run_id;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let run = app.status(access(), id.clone()).await.unwrap();
            if run
                .invocations
                .get("/nodes/create")
                .is_some_and(|r| r.state == InvocationState::RetryScheduled)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    app.cancel(access(), id.clone()).await.unwrap();
    let run = app.wait(access(), id).await.unwrap();
    assert_eq!(run.state, RunState::Blocked);
    assert!(run.cancel_requested);
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn full_audit_rejects_resolution_before_mutating_the_effect() {
    let remote = Arc::new(Remote::default());
    let runtime = EngineRuntime::boot(
        composition(remote, Behavior::UnknownApplied, Repetition::Unsafe)
            .limits(Limits {
                audit_entries: 1,
                ..Default::default()
            })
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let run = start(&app).await;
    app.reconcile(
        access(),
        command(
            &run,
            "investigate",
            EffectResolution::RecordInconclusive {
                reason: "query pending".into(),
                evidence: None,
            },
        ),
    )
    .await
    .unwrap();
    let current = app.status(access(), run.id.clone()).await.unwrap();
    assert_eq!(
        app.reconcile(
            access(),
            command(
                &current,
                "resolve",
                EffectResolution::ConfirmApplied {
                    output: json!({"order_id":"O-1"}),
                    evidence: evidence()
                }
            )
        )
        .await
        .unwrap_err()
        .code(),
        "resource.limit"
    );
    let unchanged = app.status(access(), run.id).await.unwrap();
    assert_eq!(unchanged, current);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[derive(Default)]
struct FailingScanStore {
    inner: modules::MemoryExecutionStore,
    fail: std::sync::atomic::AtomicBool,
}
impl ExecutionStore for FailingScanStore {
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
        self.inner.create(owner, run, receipt, limits)
    }
    fn get<'a>(&'a self, id: &'a RunId) -> PortFuture<'a, Option<RunSnapshot>> {
        self.inner.get(id)
    }
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>> {
        if self.fail.swap(false, std::sync::atomic::Ordering::SeqCst) {
            Box::pin(async { Err(ForgeError::new("store.injected", "Injected scan failure")) })
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
        self.inner.commit(owner, expected, next)
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        self.inner.collect(owner, now, limits)
    }
}

#[tokio::test]
async fn reclaiming_memory_after_supervisor_failure_does_not_redispatch_an_unsafe_intention() {
    let remote = Arc::new(Remote::default());
    let store = Arc::new(FailingScanStore::default());
    let builder = || {
        composition(remote.clone(), Behavior::LateApplied, Repetition::Unsafe)
            .execution_store(store.clone())
            .limits(Limits {
                attempt_timeout_ms: 5000,
                ..Default::default()
            })
    };
    let runtime = EngineRuntime::boot(builder().build().unwrap(), BootOptions::default())
        .await
        .unwrap();
    let app = runtime.application();
    let plan = app.prepare(access(), definition()).await.unwrap();
    let id = app
        .start(access(), StartRunRequest::new(plan, input()))
        .await
        .unwrap()
        .run_id;
    tokio::time::timeout(Duration::from_secs(2), async {
        while remote.state.lock().unwrap().calls.is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    store.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while app.is_ready() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        runtime
            .shutdown(ShutdownOptions::default())
            .await
            .unwrap_err()
            .code(),
        "store.injected"
    );
    let recovered = EngineRuntime::boot(
        builder().build().unwrap(),
        BootOptions {
            recovery: RecoveryPolicy::Resume,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let run = tokio::time::timeout(
        Duration::from_secs(2),
        recovered.application().wait(access(), id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(run.state, RunState::Blocked);
    assert_eq!(
        run.invocations["/nodes/create"].certainty,
        EffectCertainty::Unknown
    );
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    recovered
        .shutdown(ShutdownOptions::default())
        .await
        .unwrap();
}

fn input_binding(pointer: &str) -> Value {
    json!({"select":{"source":"input","pointer":pointer}})
}
fn node_binding(node: &str, pointer: &str) -> Value {
    json!({"select":{"source":"node","node":node,"pointer":pointer}})
}
fn inline(node: Value) -> Value {
    json!({"entry":node["id"],"nodes":[node.clone()],"edges":[],"output":node_binding(node["id"].as_str().unwrap(),"")})
}
fn controlled(id: &str, node: Value) -> WorkflowDefinition {
    let mut d = inline(node);
    d["format"] = json!(WORKFLOW_FORMAT);
    d["id"] = json!(id);
    d["revision"] = json!("r1");
    d["schema_dialect"] = json!(SCHEMA_DIALECT);
    d["input_schema"] = json!(true);
    d["output_schema"] = json!(true);
    serde_json::from_value(d).unwrap()
}
struct RejectedCustomer {
    descriptor: OperationDescriptor,
    after_dispatch: Option<Arc<Remote>>,
}
impl Operation for RejectedCustomer {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, _: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            if let Some(remote) = &self.after_dispatch {
                remote.started.notified().await;
            }
            Err(OperationError {
                code: "reference.customer.inactive".into(),
                class: ErrorClass::Rejected,
                certainty: EffectCertainty::NotApplied,
                message: "Customer is inactive".into(),
            })
        })
    }
}
fn register_rejection(builder: &mut WorkflowBuilder, after_dispatch: Option<Arc<Remote>>) {
    let mut descriptor = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    descriptor.revision.id = "reference.reject_customer".into();
    builder
        .register_bundle(OperationBundle {
            module: ModuleDescriptor {
                id: "reference.business_rules".into(),
                version: "1".into(),
                protocol_version: PROTOCOL_VERSION,
                exports: vec![descriptor.revision.clone()],
            },
            operations: vec![Arc::new(RejectedCustomer {
                descriptor,
                after_dispatch,
            })],
            inspectors: Vec::new(),
        })
        .unwrap();
}
fn rejection() -> Value {
    json!({"id":"reject","kind":"operation","input":{"literal":null},"config":{},"operation":{"id":"reference.reject_customer","contract":"1","implementation":"r1"}})
}

#[tokio::test]
async fn c01b_decision_rejects_inactive_customer_and_creates_only_on_active_branch() {
    let remote = Arc::new(Remote::default());
    let mut builder = composition(remote.clone(), Behavior::LostAck, Repetition::Keyed);
    register_rejection(&mut builder, None);
    let runtime = EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
        .await
        .unwrap();
    let app = runtime.application();
    let mut d = serde_json::to_value(definition()).unwrap();
    let mut create = d["nodes"].as_array_mut().unwrap().pop().unwrap();
    create["input"] =
        json!({"object":{"customer":input_binding("/customer"),"items":input_binding("/items")}});
    d["nodes"].as_array_mut().unwrap().push(json!({
        "id":"choose","kind":"decision","input":{"object":{
            "active":node_binding("lookup","/active"),"customer":node_binding("normalize",""),"items":input_binding("/items")
        }},"cases":[{"id":"active","when":input_binding("/active"),"body":inline(create)}],
        "fallback":{"id":"inactive","body":inline(rejection())}
    }));
    d["edges"][1]["to"] = json!("choose");
    d["output"] = node_binding("choose", "/output");
    let d: WorkflowDefinition = serde_json::from_value(d).unwrap();
    let mut inactive = input();
    inactive["customer"] = json!("C-10");
    let rejected = start_with(&app, d.clone(), inactive).await;
    assert_eq!(rejected.state, RunState::Failed);
    assert_eq!(
        rejected.error.unwrap().diagnostics[0]
            .operation_error
            .as_ref()
            .unwrap()
            .code,
        "reference.customer.inactive"
    );
    assert!(remote.state.lock().unwrap().calls.is_empty());
    let active = start_with(&app, d, input()).await;
    assert_eq!(active.output, Some(json!({"order_id":"O-1"})));
    assert_eq!(remote.state.lock().unwrap().orders.len(), 1);
    assert_eq!(remote.state.lock().unwrap().calls.len(), 2);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn collect_and_nested_subworkflows_resume_only_after_all_uncertain_items_are_resolved() {
    let remote = Arc::new(Remote::default());
    let mut builder = composition(remote.clone(), Behavior::UnknownApplied, Repetition::Unsafe);
    builder.register_workflow(definition()).unwrap();
    let runtime = EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
        .await
        .unwrap();
    let app = runtime.application();
    let d = controlled(
        "nested-orders",
        json!({"id":"batch","kind":"foreach","input":input_binding(""),"items":input_binding(""),"concurrency":2,"errors":"collect",
            "body":inline(json!({"id":"call","kind":"subworkflow","input":input_binding("/item"),"workflow":{"id":"reference.create_order","revision":"r1"}}))
        }),
    );
    let mut blocked = start_with(&app, d, json!([input(), input()])).await;
    assert_eq!(blocked.state, RunState::Blocked);
    let effects: Vec<_> = blocked
        .invocations
        .iter()
        .filter(|(_, r)| r.state == InvocationState::Unknown)
        .map(|(key, _)| key.clone())
        .collect();
    assert_eq!(effects.len(), 2);
    let confirmed: BTreeMap<_, _> = blocked
        .invocations
        .iter()
        .filter(|(_, r)| r.state == InvocationState::Succeeded)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(confirmed.len(), 4);
    for (index, key) in effects.iter().enumerate() {
        let EffectInspection::Applied { output, evidence } = app
            .inspect_effect(
                access(),
                blocked.id.clone(),
                blocked.invocations[key].id.clone(),
            )
            .await
            .unwrap()
        else {
            panic!("effect exists")
        };
        app.reconcile(
            access(),
            command_for(
                &blocked,
                key,
                &format!("resolve-{index}"),
                EffectResolution::ConfirmApplied { output, evidence },
            ),
        )
        .await
        .unwrap();
        blocked = tokio::time::timeout(
            Duration::from_secs(3),
            app.wait(access(), blocked.id.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            blocked.state,
            if index == 0 {
                RunState::Blocked
            } else {
                RunState::Succeeded
            }
        );
    }
    assert_eq!(remote.state.lock().unwrap().calls.len(), 2);
    assert_eq!(remote.state.lock().unwrap().orders.len(), 2);
    for (key, original) in confirmed {
        assert_eq!(blocked.invocations[&key], original);
    }
    let output = blocked.output.unwrap();
    assert_eq!(output[0]["index"], 0);
    assert_eq!(output[1]["index"], 1);
    assert_ne!(output[0]["output"], output[1]["output"]);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn fail_fast_cancels_active_writes_then_preserves_failure_after_resolution() {
    let remote = Arc::new(Remote::default());
    let mut builder = composition(remote.clone(), Behavior::LateApplied, Repetition::Unsafe);
    register_rejection(&mut builder, Some(remote.clone()));
    let runtime = EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
        .await
        .unwrap();
    let app = runtime.application();
    let mut create = serde_json::to_value(definition()).unwrap()["nodes"][2].clone();
    create["input"] = json!({"literal":{"customer":"C-9","items":[{"sku":"A-1","quantity":1}]}});
    let d = controlled(
        "failed-parallel",
        json!({"id":"group","kind":"parallel","input":input_binding(""),"branches":{
        "a_write":inline(create.clone()),"b_reject":inline(rejection()),"c_unadmitted":inline(create)
    },"concurrency":2,"errors":"fail_fast","join":"all"}),
    );
    let blocked = start_with(&app, d, Value::Null).await;
    assert_eq!(blocked.state, RunState::Blocked);
    let key = "/nodes/group/branches/a_write/nodes/create";
    assert_eq!(blocked.invocations[key].state, InvocationState::Unknown);
    assert!(
        !blocked
            .invocations
            .contains_key("/nodes/group/branches/c_unadmitted/nodes/create")
    );
    let EffectInspection::Applied { output, evidence } = app
        .inspect_effect(
            access(),
            blocked.id.clone(),
            blocked.invocations[key].id.clone(),
        )
        .await
        .unwrap()
    else {
        panic!("effect exists")
    };
    app.reconcile(
        access(),
        command_for(
            &blocked,
            key,
            "resolve-active",
            EffectResolution::ConfirmApplied { output, evidence },
        ),
    )
    .await
    .unwrap();
    let done = tokio::time::timeout(Duration::from_secs(3), app.wait(access(), blocked.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.state, RunState::Failed);
    assert_eq!(
        done.error.unwrap().diagnostics[0]
            .operation_error
            .as_ref()
            .unwrap()
            .code,
        "reference.customer.inactive"
    );
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    assert!(
        !done
            .invocations
            .contains_key("/nodes/group/branches/c_unadmitted/nodes/create")
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn loop_resumption_advances_cursor_without_repeating_confirmed_iteration() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote.clone(), Behavior::UnknownApplied, Repetition::Unsafe).await;
    let app = runtime.application();
    let mut create = serde_json::to_value(definition()).unwrap()["nodes"][2].clone();
    create["input"] = json!({"object":{"customer":input_binding("/state/customer"),"items":input_binding("/state/items")}});
    let mut body = inline(create);
    body["output"] = json!({"object":{"customer":input_binding("/state/customer"),"items":input_binding("/state/items"),"last":node_binding("create","")}});
    let d = controlled(
        "loop-orders",
        json!({"id":"loop","kind":"loop","input":input_binding(""),"while":{"literal":true},"body":body,"max_iterations":2,"on_limit":"return_last"}),
    );
    let mut blocked = start_with(
        &app,
        d,
        json!({"customer":"C-9","items":[{"sku":"A-1","quantity":1}]}),
    )
    .await;
    for i in 0..2 {
        assert_eq!(blocked.state, RunState::Blocked);
        let key = format!("/nodes/loop/iterations/{i}/nodes/create");
        let EffectInspection::Applied { output, evidence } = app
            .inspect_effect(
                access(),
                blocked.id.clone(),
                blocked.invocations[&key].id.clone(),
            )
            .await
            .unwrap()
        else {
            panic!("effect exists")
        };
        app.reconcile(
            access(),
            command_for(
                &blocked,
                &key,
                &format!("iteration-{i}"),
                EffectResolution::ConfirmApplied { output, evidence },
            ),
        )
        .await
        .unwrap();
        blocked = tokio::time::timeout(
            Duration::from_secs(3),
            app.wait(access(), blocked.id.clone()),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(blocked.state, RunState::Succeeded);
    assert_eq!(blocked.output.unwrap()["last"]["order_id"], "O-2");
    assert_eq!(remote.state.lock().unwrap().calls.len(), 2);
    assert_ne!(
        blocked.invocations["/nodes/loop/iterations/0/nodes/create"].id,
        blocked.invocations["/nodes/loop/iterations/1/nodes/create"].id
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn workflow_fallback_cannot_hide_an_uncertain_write_or_repeat_it() {
    let remote = Arc::new(Remote::default());
    let runtime = boot(remote.clone(), Behavior::UnknownApplied, Repetition::Unsafe).await;
    let mut value = serde_json::to_value(definition()).unwrap();
    let protected = json!({"entry":value["entry"],"nodes":value["nodes"],"edges":value["edges"],"output":value["output"]});
    value["entry"] = json!("guard");
    value["edges"] = json!([]);
    value["nodes"] = json!([{"id":"guard","kind":"try","input":{"select":{"source":"input","pointer":""}},"body":protected,"catches":[],"fallback":{"id":"unsafe","body":{"entry":"mask","nodes":[{"id":"mask","kind":"operation","operation":{"id":"forge.data.identity","contract":"1","implementation":"r1"},"config":{},"input":{"literal":"hidden"}}],"edges":[],"output":{"literal":"hidden"}}}}]);
    value["output"] = json!({"select":{"source":"node","node":"guard","pointer":"/output"}});
    let run = start_with(
        &runtime.application(),
        serde_json::from_value(value).unwrap(),
        input(),
    )
    .await;
    assert_eq!(run.state, RunState::Blocked);
    assert!(run.output.is_none());
    assert_eq!(run.error.unwrap().code(), "effect.unknown");
    assert!(!run.invocations.keys().any(|key| key.contains("/catch/")));
    assert_eq!(remote.state.lock().unwrap().calls.len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
