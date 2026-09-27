use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use workflow_forge::v2::*;

fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn definition(id: &str, node: Value) -> WorkflowDefinition {
    serde_json::from_value(json!({"format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":id,"revision":"r1","input_schema":true,"output_schema":true,"entry":node["id"],"nodes":[node],"edges":[],"output":{"select":{"source":"node","node":node["id"],"pointer":""}}})).unwrap()
}
fn timer(ms: u64) -> Value {
    json!({"id":"pause","kind":"timer","duration_ms":ms,"input":{"select":{"source":"input","pointer":""}}})
}
fn signal() -> Value {
    json!({"id":"callback","kind":"await_signal","input":{"select":{"source":"input","pointer":""}},"correlation":{"literal":"job-1"},"timeout_ms":60000,"payload_schema":{"type":"object","required":["answer"],"properties":{"answer":{"type":"integer"}},"additionalProperties":false}})
}
fn body(node: Value) -> Value {
    json!({"entry":node["id"],"nodes":[node],"edges":[],"output":{"select":{"source":"node","node":node["id"],"pointer":""}}})
}
async fn boot(builder: WorkflowBuilder) -> EngineRuntime {
    EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
        .await
        .unwrap()
}
async fn start(app: &WorkflowApplication, d: WorkflowDefinition, input: Value) -> RunId {
    let schema: Value =
        serde_json::from_slice(include_bytes!("../../../schemas/2/workflow.schema.json")).unwrap();
    assert!(
        jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&json!(d))
    );
    let plan = app.prepare(access(), d).await.unwrap();
    app.start(access(), StartRunRequest::new(plan, input))
        .await
        .unwrap()
        .run_id
}
async fn state(app: &WorkflowApplication, id: &RunId, wanted: RunState) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let run = app.status(access(), id.clone()).await.unwrap();
            if run.state == wanted {
                return run;
            }
            assert!(!run.state.is_terminal(), "Expected {wanted:?}: {run:?}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap()
}
async fn done(app: &WorkflowApplication, id: RunId) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(3), app.wait(access(), id))
        .await
        .unwrap()
        .unwrap()
}
fn command(run: &RunSnapshot) -> SignalCommand {
    let wait = run
        .waits
        .values()
        .find(|w| matches!(w.kind, WaitKind::Signal { .. }))
        .unwrap();
    let WaitKind::Signal { correlation, .. } = &wait.kind else {
        unreachable!()
    };
    SignalCommand {
        run_id: run.id.clone(),
        wait_id: wait.id.clone(),
        message_id: "message-1".into(),
        correlation: correlation.clone(),
        payload: json!({"answer":7}),
        artifacts: vec![],
    }
}

#[tokio::test]
async fn a_suspended_timer_releases_capacity_and_cancellation_closes_its_reservation() {
    let runtime = boot(WorkflowBuilder::standard().limits(Limits {
        active_runs: 1,
        pending_runs: 2,
        ..Default::default()
    }))
    .await;
    let app = runtime.application();
    let paused = start(&app, definition("pause", timer(60000)), json!({"n":1})).await;
    let snapshot = state(&app, &paused, RunState::Waiting).await;
    assert!(snapshot.head().next_wakeup_at_ms.is_some());
    let second = start(&app, definition("immediate", timer(0)), json!({"n":2})).await;
    let result = done(&app, second).await;
    assert_eq!(result.state, RunState::Succeeded, "{:?}", result.error);
    assert_eq!(result.output, Some(json!({"n":2})));
    app.cancel(access(), paused.clone()).await.unwrap();
    let result = done(&app, paused).await;
    assert_eq!(result.state, RunState::Cancelled);
    assert!(result.waits.values().all(|w| w.state == WaitState::Closed));
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn signals_validate_before_accepting_and_keep_their_receipt_after_consumption() {
    let runtime = boot(WorkflowBuilder::standard()).await;
    let app = runtime.application();
    let id = start(&app, definition("approval", signal()), json!({})).await;
    let snapshot = state(&app, &id, RunState::Waiting).await;
    let cmd = command(&snapshot);
    let mut unknown = cmd.clone();
    unknown.wait_id = "unreserved".into();
    assert_eq!(
        app.signal(access(), unknown).await.unwrap_err().code(),
        "wait.not_found"
    );
    let mut invalid = cmd.clone();
    invalid.payload = json!({"answer":"wrong"});
    assert_eq!(
        app.signal(access(), invalid).await.unwrap_err().code(),
        "data.invalid"
    );
    let mut wrong = cmd.clone();
    wrong.correlation = "another-job".into();
    assert_eq!(
        app.signal(access(), wrong).await.unwrap_err().code(),
        "data.invalid"
    );
    let mut forbidden = access();
    forbidden.permissions.remove(&Permission::Signal);
    assert_eq!(
        app.signal(forbidden, cmd.clone()).await.unwrap_err().code(),
        "access.denied"
    );
    let receipt = app.signal(access(), cmd.clone()).await.unwrap();
    assert!(!receipt.durable && !receipt.duplicate);
    let result = done(&app, id).await;
    assert_eq!(result.state, RunState::Succeeded, "{:?}", result.error);
    assert_eq!(
        result.output,
        Some(json!({"start":null,"signal":{"answer":7}}))
    );
    assert_eq!(result.waits[&cmd.wait_id].state, WaitState::Consumed);
    let mut duplicate = receipt;
    duplicate.duplicate = true;
    assert_eq!(app.signal(access(), cmd.clone()).await.unwrap(), duplicate);
    let mut conflict = cmd.clone();
    conflict.payload = json!({"answer":8});
    assert_eq!(
        app.signal(access(), conflict).await.unwrap_err().code(),
        "state.conflict"
    );
    let mut other_actor = access();
    other_actor.actor = "other".into();
    assert_eq!(
        app.signal(other_actor, cmd).await.unwrap_err().code(),
        "state.conflict"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

struct Starter {
    descriptor: OperationDescriptor,
    started: Notify,
    release: Notify,
    invocation: Mutex<Option<Invocation>>,
    calls: AtomicUsize,
    lose_ack: bool,
}
impl Operation for Starter {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.invocation.lock().unwrap() = Some(invocation);
            self.started.notify_one();
            self.release.notified().await;
            if self.lose_ack {
                return Err(OperationError {
                    code: "test.lost_ack".into(),
                    class: ErrorClass::Transient,
                    certainty: EffectCertainty::Unknown,
                    message: "Controlled start effect was applied but its acknowledgement was lost"
                        .into(),
                });
            }
            Ok(OperationOutput::json(json!({"ticket":"T-1"})))
        })
    }
}
fn starter(lose_ack: bool) -> (Arc<Starter>, WorkflowBuilder) {
    let op = named_starter("test.wait.start", lose_ack);
    let mut b = WorkflowBuilder::standard();
    register_starter(&mut b, op.clone());
    (op, b)
}
fn named_starter(name: &str, lose_ack: bool) -> Arc<Starter> {
    let mut d = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    d.revision = OperationRevision::new(name, "1", "r1");
    d.effect = EffectKind::Write;
    d.repetition = Repetition::Unsafe;
    Arc::new(Starter {
        descriptor: d,
        started: Notify::new(),
        release: Notify::new(),
        invocation: Mutex::new(None),
        calls: AtomicUsize::new(0),
        lose_ack,
    })
}
fn register_starter(b: &mut WorkflowBuilder, op: Arc<Starter>) {
    let d = op.descriptor();
    b.register_bundle(OperationBundle {
        module: ModuleDescriptor {
            id: d.revision.id.clone(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![d.revision.clone()],
        },
        operations: vec![op],
        inspectors: vec![],
    })
    .unwrap();
}
fn started_signal() -> Value {
    let mut node = signal();
    node["start"] = json!({"operation":{"id":"test.wait.start","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}}});
    node
}

#[tokio::test]
async fn early_callback_waits_for_confirmed_start_and_never_resolves_uncertainty_itself() {
    for lose_ack in [false, true] {
        let (op, b) = starter(lose_ack);
        let runtime = boot(b).await;
        let app = runtime.application();
        let id = start(&app, definition("job", started_signal()), json!({"job":7})).await;
        tokio::time::timeout(Duration::from_secs(2), op.started.notified())
            .await
            .unwrap();
        let snapshot = app.status(access(), id.clone()).await.unwrap();
        let cmd = command(&snapshot);
        let invocation = op.invocation.lock().unwrap().clone().unwrap();
        assert_eq!(invocation.input["wait"]["id"], cmd.wait_id);
        assert!(!snapshot.waits[&cmd.wait_id].start_confirmed);
        app.signal(access(), cmd.clone()).await.unwrap();
        let early = app.status(access(), id.clone()).await.unwrap();
        assert!(early.output.is_none());
        assert_eq!(early.waits[&cmd.wait_id].state, WaitState::Open);
        op.release.notify_one();
        let mut result = done(&app, id.clone()).await;
        if lose_ack {
            assert_eq!(result.state, RunState::Blocked, "{:?}", result.error);
            assert!(!result.waits[&cmd.wait_id].start_confirmed);
            assert!(app.signal(access(), cmd.clone()).await.unwrap().duplicate);
            let uncertain = result
                .invocations
                .values()
                .find(|r| r.state == InvocationState::Unknown)
                .unwrap();
            app.reconcile(access(),ReconcileCommand { command_id:"confirm-start".into(),run_id:id.clone(),invocation_id:uncertain.id.clone(),expected_revision:result.revision,observed_attempt:uncertain.attempt_id.clone(),resolution:EffectResolution::ConfirmApplied { output:json!({"ticket":"T-1"}),evidence:EffectEvidence { authority:"test.controlled_starter".into(),reference:invocation.effect_key.unwrap(),note:"The controlled starter recorded the invocation before returning its lost acknowledgement".into() } } }).await.unwrap();
            result = done(&app, id).await;
        }
        assert_eq!(result.state, RunState::Succeeded, "{:?}", result.error);
        assert_eq!(
            result.output,
            Some(json!({"start":{"ticket":"T-1"},"signal":{"answer":7}}))
        );
        assert_eq!(op.calls.load(Ordering::SeqCst), 1);
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    }
}

#[tokio::test]
async fn expiry_rejects_late_delivery_without_reopening_the_run() {
    let runtime = boot(WorkflowBuilder::standard()).await;
    let app = runtime.application();
    let mut node = signal();
    node["timeout_ms"] = json!(150);
    let id = start(&app, definition("expires", node), json!({})).await;
    let cmd = command(&state(&app, &id, RunState::Waiting).await);
    let result = done(&app, id).await;
    assert_eq!(result.state, RunState::Failed);
    assert_eq!(result.error.unwrap().code(), "wait.expired");
    assert_eq!(result.waits[&cmd.wait_id].state, WaitState::Expired);
    assert_eq!(
        app.signal(access(), cmd).await.unwrap_err().code(),
        "wait.expired"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn foreach_and_parallel_keep_suspended_children_until_every_result_is_ready() {
    let runtime = boot(WorkflowBuilder::standard()).await;
    let app = runtime.application();
    let mut approval = signal();
    approval["correlation"] = json!({"select":{"source":"input","pointer":"/item"}});
    let foreach = json!({"id":"items","kind":"foreach","input":{"literal":null},"items":{"literal":["a","b","c"]},"body":body(approval),"concurrency":2,"errors":"collect"});
    let parallel = json!({"id":"group","kind":"parallel","input":{"select":{"source":"input","pointer":""}},"branches":{"timer":body(timer(100)),"signals":body(foreach)},"concurrency":2,"errors":"fail_fast","join":"all"});
    let id = start(&app, definition("nested", parallel), json!({"value":9})).await;
    let snapshot = state(&app, &id, RunState::Waiting).await;
    let mut reservations: Vec<_> = snapshot
        .waits
        .values()
        .filter(|w| matches!(w.kind, WaitKind::Signal { .. }))
        .collect();
    assert_eq!(reservations.len(), 3);
    reservations.reverse();
    for wait in reservations {
        let WaitKind::Signal { correlation, .. } = &wait.kind else {
            unreachable!()
        };
        app.signal(
            access(),
            SignalCommand {
                run_id: id.clone(),
                wait_id: wait.id.clone(),
                message_id: format!("for-{correlation}"),
                correlation: correlation.clone(),
                payload: json!({"answer":correlation.as_bytes()[0]}),
                artifacts: vec![],
            },
        )
        .await
        .unwrap();
    }
    let result = done(&app, id).await;
    assert_eq!(result.state, RunState::Succeeded, "{:?}", result.error);
    let output = result.output.unwrap();
    assert_eq!(output["timer"]["output"], json!({"value":9}));
    for (index, value) in output["signals"]["output"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        assert_eq!(value["index"], index);
        assert_eq!(value["output"]["signal"]["answer"], 97 + index);
    }
    assert_eq!(result.waits.len(), 4);
    assert!(
        result
            .waits
            .values()
            .all(|w| w.state == WaitState::Consumed)
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn loop_and_subworkflow_keep_distinct_timer_reservations_and_enforce_the_wait_budget() {
    let child = definition("child", timer(5));
    let mut b = WorkflowBuilder::standard();
    b.register_workflow(child).unwrap();
    let sub = json!({"id":"child","kind":"subworkflow","workflow":{"id":"child","revision":"r1"},"input":{"select":{"source":"input","pointer":"/state"}}});
    let loop_node = json!({"id":"repeat","kind":"loop","while":{"literal":true},"input":{"select":{"source":"input","pointer":""}},"body":body(sub),"max_iterations":2,"on_limit":"return_last"});
    let runtime = boot(b).await;
    let app = runtime.application();
    let id = start(&app, definition("loop", loop_node.clone()), json!(9)).await;
    let result = done(&app, id).await;
    assert_eq!(result.state, RunState::Succeeded, "{:?}", result.error);
    assert_eq!(result.output, Some(json!(9)));
    assert_eq!(result.waits.len(), 2);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    let mut b = WorkflowBuilder::standard().limits(Limits {
        waits_per_run: 1,
        ..Default::default()
    });
    b.register_workflow(definition("child", timer(0))).unwrap();
    let runtime = boot(b).await;
    let id = start(
        &runtime.application(),
        definition("limited", loop_node),
        json!(9),
    )
    .await;
    let result = done(&runtime.application(), id).await;
    assert_eq!(result.state, RunState::Failed);
    assert_eq!(result.error.unwrap().code(), "resource.limit");
    assert_eq!(result.waits.len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn timely_callback_survives_its_wait_deadline_while_start_is_still_unconfirmed() {
    let (op, b) = starter(false);
    let runtime = boot(b).await;
    let app = runtime.application();
    let mut node = started_signal();
    node["timeout_ms"] = json!(150);
    let id = start(&app, definition("timely", node), json!({})).await;
    op.started.notified().await;
    let snapshot = app.status(access(), id.clone()).await.unwrap();
    let cmd = command(&snapshot);
    app.signal(access(), cmd.clone()).await.unwrap();
    // Wait past the persisted deadline while the starter is explicitly held.
    let deadline = snapshot.waits[&cmd.wait_id].deadline_at_ms;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(now) + 50)).await;
    let held = app.status(access(), id.clone()).await.unwrap();
    assert_eq!(held.waits[&cmd.wait_id].state, WaitState::Open);
    assert!(!held.waits[&cmd.wait_id].start_confirmed);
    assert!(held.output.is_none());
    op.release.notify_one();
    let result = done(&app, id).await;
    assert_eq!(result.state, RunState::Succeeded, "{:?}", result.error);
    assert_eq!(op.calls.load(Ordering::SeqCst), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn expiry_during_unsafe_start_keeps_uncertainty_until_authoritative_resolution() {
    let (op, b) = starter(false);
    let runtime = boot(b).await;
    let app = runtime.application();
    let mut node = started_signal();
    node["timeout_ms"] = json!(150);
    let id = start(&app, definition("start_timeout", node), json!({})).await;
    op.started.notified().await;
    let blocked = done(&app, id.clone()).await;
    assert_eq!(blocked.state, RunState::Blocked, "{:?}", blocked.error);
    let cmd = command(&blocked);
    assert_eq!(blocked.waits[&cmd.wait_id].state, WaitState::Expired);
    assert_eq!(
        app.signal(access(), cmd.clone()).await.unwrap_err().code(),
        "wait.expired"
    );
    let unknown = blocked
        .invocations
        .values()
        .find(|r| r.state == InvocationState::Unknown)
        .unwrap();
    app.reconcile(
        access(),
        ReconcileCommand {
            command_id: "confirm-expired-start".into(),
            run_id: id.clone(),
            invocation_id: unknown.id.clone(),
            expected_revision: blocked.revision,
            observed_attempt: unknown.attempt_id.clone(),
            resolution: EffectResolution::ConfirmApplied {
                output: json!({"ticket":"T-1"}),
                evidence: EffectEvidence {
                    authority: "controlled-test".into(),
                    reference: unknown.effect_key.clone().unwrap(),
                    note: "Controlled starter has recorded its effect; its response remains held"
                        .into(),
                },
            },
        },
    )
    .await
    .unwrap();
    let result = done(&app, id).await;
    assert_eq!(result.state, RunState::Failed, "{:?}", result.error);
    assert_eq!(result.error.as_ref().unwrap().code(), "wait.expired");
    assert_eq!(result.waits[&cmd.wait_id].state, WaitState::Expired);
    assert_eq!(op.calls.load(Ordering::SeqCst), 1);
    op.release.notify_one();
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn a_stopped_group_closes_its_suspended_waits_without_closing_other_scopes() {
    let (unknown, mut b) = starter(true);
    let known = named_starter("test.wait.known", false);
    register_starter(&mut b, known.clone());
    let operation = |name: &str| json!({"id":"work","kind":"operation","operation":{"id":name,"contract":"1","implementation":"r1"},"config":{},"input":{"literal":null}});
    let mut failing = body(operation("test.wait.known"));
    failing["output"] = json!({"select":{"source":"node","node":"work","pointer":"/missing"}});
    let inner = json!({"id":"stop","kind":"parallel","input":{"literal":null},"branches":{"a_wait":body(signal()),"b_fail":failing},"concurrency":2,"errors":"fail_fast","join":"all"});
    let outer = json!({"id":"outer","kind":"parallel","input":{"literal":null},"branches":{"a_inner":body(inner),"b_unknown":body(operation("test.wait.start")),"c_wait":body(signal())},"concurrency":3,"errors":"collect","join":"all"});
    let runtime = boot(b).await;
    let app = runtime.application();
    let id = start(&app, definition("scoped_stop", outer), json!(null)).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        known.started.notified().await;
        unknown.started.notified().await;
        loop {
            if app.status(access(), id.clone()).await.unwrap().waits.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    known.release.notify_one();
    unknown.release.notify_one();
    let blocked = done(&app, id.clone()).await;
    assert_eq!(blocked.state, RunState::Blocked, "{:?}", blocked.error);
    let inner = blocked
        .waits
        .values()
        .find(|w| w.node.contains("/branches/a_inner/"))
        .unwrap();
    let other = blocked
        .waits
        .values()
        .find(|w| w.node.contains("/branches/c_wait/"))
        .unwrap();
    assert_eq!(inner.state, WaitState::Closed);
    assert_eq!(other.state, WaitState::Open);
    let mut cmd = command(&blocked);
    cmd.wait_id = inner.id.clone();
    assert_eq!(
        app.signal(access(), cmd.clone()).await.unwrap_err().code(),
        "state.conflict"
    );
    cmd.wait_id = other.id.clone();
    app.signal(access(), cmd.clone()).await.unwrap();
    assert_eq!(
        app.status(access(), id.clone()).await.unwrap().state,
        RunState::Blocked
    );
    app.cancel(access(), id.clone()).await.unwrap();
    let cancelled = done(&app, id).await;
    assert_eq!(cancelled.state, RunState::Blocked);
    assert!(cancelled.cancel_requested);
    assert!(
        cancelled
            .waits
            .values()
            .all(|w| w.state == WaitState::Closed)
    );
    assert!(app.signal(access(), cmd).await.unwrap().duplicate);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn wait_contracts_enforce_offline_schemas_deadline_and_payload_budgets() {
    let runtime = boot(WorkflowBuilder::standard().limits(Limits {
        signal_bytes: 32,
        wait_timeout_ms: 60_000,
        ..Default::default()
    }))
    .await;
    let app = runtime.application();
    for field in ["timeout_ms", "correlation", "payload_schema"] {
        let mut node = signal();
        node[field] = match field {
            "timeout_ms" => json!(60_001),
            "correlation" => json!({"literal":""}),
            _ => json!({"$ref":"https://example.test/missing"}),
        };
        assert!(
            app.prepare(access(), definition(field, node))
                .await
                .is_err()
        );
    }
    assert!(
        app.prepare(access(), definition("timer_budget", timer(60_001)))
            .await
            .is_err()
    );
    let mut node = signal();
    node["payload_schema"] = json!(true);
    let id = start(&app, definition("signal_budget", node), json!(null)).await;
    let mut cmd = command(&state(&app, &id, RunState::Waiting).await);
    cmd.payload = json!("x".repeat(33));
    assert_eq!(
        app.signal(access(), cmd.clone()).await.unwrap_err().code(),
        "data.invalid"
    );
    assert!(
        app.status(access(), id.clone()).await.unwrap().waits[&cmd.wait_id]
            .delivery
            .is_none()
    );
    cmd.payload = json!("small");
    app.signal(access(), cmd).await.unwrap();
    assert_eq!(done(&app, id).await.state, RunState::Succeeded);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();

    let (op, b) = starter(false);
    let runtime = boot(b.limits(Limits {
        activations: 1,
        ..Default::default()
    }))
    .await;
    assert!(
        runtime
            .application()
            .prepare(access(), definition("start_budget", started_signal()))
            .await
            .is_err()
    );
    assert_eq!(op.calls.load(Ordering::SeqCst), 0);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
