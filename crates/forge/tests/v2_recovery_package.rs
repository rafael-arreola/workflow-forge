//! These tests retain a memory store across compositions. Process durability is
//! tested separately by the SQLite profile; this suite checks frozen resolution.
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use workflow_forge::v2::*;

fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn input() -> Value {
    json!({"select":{"source":"input","pointer":""}})
}
fn result(node: &str) -> Value {
    json!({"select":{"source":"node","node":node,"pointer":""}})
}
fn op(id: &str, input: Value) -> Value {
    json!({"id":id,"kind":"operation","operation":{"id":"test.recovery.echo","contract":"1","implementation":"r1"},"config":{},"input":input})
}
fn definition(id: &str, nodes: Vec<Value>, edges: Value) -> WorkflowDefinition {
    serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":id,"revision":"r1",
        "entry":nodes.first().unwrap()["id"],"output":result(nodes.last().unwrap()["id"].as_str().unwrap()),
        "input_schema":true,"output_schema":true,"nodes":nodes,"edges":edges,
    })).unwrap()
}
fn schema(kind: &str, revision: &str) -> SchemaResource {
    SchemaResource {
        uri: "urn:recovery:value".into(),
        revision: revision.into(),
        schema: json!({"$schema":SCHEMA_DIALECT,"type":kind}),
    }
}
fn descriptor() -> OperationDescriptor {
    let mut d = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    d.revision = OperationRevision::new("test.recovery.echo", "1", "r1");
    d
}
struct Probe {
    descriptor: OperationDescriptor,
    calls: Arc<AtomicUsize>,
}
impl Operation for Probe {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(OperationOutput::json(invocation.input))
        })
    }
}
fn register(
    builder: &mut WorkflowBuilder,
    descriptor: OperationDescriptor,
    calls: Arc<AtomicUsize>,
) {
    builder
        .register_bundle(OperationBundle {
            module: ModuleDescriptor {
                id: "test.recovery".into(),
                version: "1".into(),
                protocol_version: PROTOCOL_VERSION,
                exports: vec![descriptor.revision.clone()],
            },
            operations: vec![Arc::new(Probe { descriptor, calls })],
            inspectors: vec![],
        })
        .unwrap();
}
async fn boot(builder: WorkflowBuilder) -> EngineRuntime {
    EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
        .await
        .unwrap()
}
async fn wait(app: &WorkflowApplication, id: RunId) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(5), app.wait(access(), id))
        .await
        .unwrap()
        .unwrap()
}
async fn complete(builder: WorkflowBuilder, definition: WorkflowDefinition) -> RunSnapshot {
    let runtime = boot(builder).await;
    let app = runtime.application();
    let plan = app.prepare(access(), definition).await.unwrap();
    let receipt = app
        .start(access(), StartRunRequest::new(plan, json!(7)))
        .await
        .unwrap();
    let snapshot = wait(&app, receipt.run_id).await;
    assert_eq!(snapshot.state, RunState::Succeeded, "{:?}", snapshot.error);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    snapshot
}

/// Install a checkpoint at an explicit transition boundary, using public ports.
async fn checkpoint(
    mut captured: RunSnapshot,
    edit: impl FnOnce(&mut RunSnapshot),
) -> Arc<modules::MemoryExecutionStore> {
    let store = Arc::new(modules::MemoryExecutionStore::default());
    let mut initial = captured.clone();
    initial.revision = 0;
    initial.state = RunState::Accepted;
    initial.invocations.clear();
    initial.output = None;
    initial.finished_at_ms = None;
    store.claim("fixture").await.unwrap();
    store
        .create("fixture", initial, None, &Limits::default())
        .await
        .unwrap();
    captured.revision = 1;
    captured.state = RunState::Running;
    captured.output = None;
    captured.finished_at_ms = None;
    edit(&mut captured);
    store.commit("fixture", 0, captured).await.unwrap();
    store.release("fixture").await.unwrap();
    store
}
async fn subworkflow_fixture(calls: Arc<AtomicUsize>) -> (RunSnapshot, WorkflowDefinition) {
    let child = definition(
        "child",
        vec![op("first", input()), op("second", result("first"))],
        json!([{"from":"first","to":"second"}]),
    );
    let mut root = definition(
        "root",
        vec![
            json!({"id":"call","kind":"subworkflow","workflow":{"id":"child","revision":"r1"},"input":input()}),
        ],
        json!([]),
    );
    root.input_schema = json!({"$schema":SCHEMA_DIALECT,"$ref":"urn:recovery:value"});
    let mut builder = WorkflowBuilder::standard();
    builder
        .register_schemas(vec![schema("integer", "r1")])
        .unwrap();
    builder.register_workflow(child.clone()).unwrap();
    register(&mut builder, descriptor(), calls.clone());
    let snapshot = complete(builder, root).await;
    calls.store(0, Ordering::SeqCst);
    (snapshot, child)
}
fn keep_first(run: &mut RunSnapshot) {
    run.invocations
        .retain(|key, _| key.ends_with("/nodes/first"));
    assert_eq!(run.invocations.len(), 1);
}

#[tokio::test]
async fn accepted_package_survives_json_roundtrip_and_changed_authoring_catalog() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (snapshot, child) = subworkflow_fixture(calls.clone()).await;
    let snapshot: RunSnapshot =
        serde_json::from_slice(&serde_json::to_vec(&snapshot).unwrap()).unwrap();
    assert_eq!(snapshot.package.definitions.len(), 2);
    let id = snapshot.id.clone();
    let store = checkpoint(snapshot, keep_first).await;
    let mut builder = WorkflowBuilder::standard().execution_store(store);
    // This catalog has a conflicting child and no authoring schema resources.
    // The accepted run must reconstruct both from its own package.
    let mut changed = serde_json::to_value(child).unwrap();
    changed["nodes"][1]["input"] = json!({"literal":999});
    builder
        .register_workflow(serde_json::from_value(changed).unwrap())
        .unwrap();
    register(&mut builder, descriptor(), calls.clone());
    let runtime = boot(builder).await;
    let done = wait(&runtime.application(), id).await;
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.error);
    assert_eq!(done.output, Some(json!(7)));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "confirmed first node must not execute again"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn missing_implementation_blocks_then_exact_reinstallation_recovers() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (snapshot, _) = subworkflow_fixture(calls.clone()).await;
    let id = snapshot.id.clone();
    let store = checkpoint(snapshot, keep_first).await;
    let runtime = boot(WorkflowBuilder::standard().execution_store(store.clone())).await;
    let blocked = wait(&runtime.application(), id.clone()).await;
    assert_eq!(blocked.state, RunState::Blocked);
    assert_eq!(blocked.error.unwrap().code(), "recovery.unavailable");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();

    let mut builder = WorkflowBuilder::standard().execution_store(store);
    let mut d = descriptor();
    d.description = "Presentation may change independently".into();
    register(&mut builder, d, calls.clone());
    let runtime = boot(builder).await;
    let done = wait(&runtime.application(), id).await;
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.error);
    assert_eq!(done.output, Some(json!(7)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn reusing_an_operation_revision_with_a_different_contract_never_dispatches() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (snapshot, _) = subworkflow_fixture(calls.clone()).await;
    let id = snapshot.id.clone();
    let store = checkpoint(snapshot, keep_first).await;
    let mut builder = WorkflowBuilder::standard().execution_store(store);
    let mut d = descriptor();
    d.output_schema = json!(false);
    register(&mut builder, d, calls.clone());
    let runtime = boot(builder).await;
    let blocked = wait(&runtime.application(), id).await;
    assert_eq!(blocked.state, RunState::Blocked);
    assert_eq!(blocked.error.unwrap().code(), "recovery.unavailable");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn reconciliation_uses_the_accepted_external_schema_after_restart() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut d = descriptor();
    d.effect = EffectKind::Write;
    d.repetition = Repetition::Unsafe;
    d.output_schema = json!({"$schema":SCHEMA_DIALECT,"$ref":"urn:recovery:value"});
    let mut builder = WorkflowBuilder::standard();
    builder
        .register_schemas(vec![schema("integer", "r1")])
        .unwrap();
    register(&mut builder, d.clone(), calls.clone());
    let snapshot = complete(
        builder,
        definition("write", vec![op("write", input())], json!([])),
    )
    .await;
    calls.store(0, Ordering::SeqCst);
    let id = snapshot.id.clone();
    let store = checkpoint(snapshot, |run| {
        run.state = RunState::Blocked;
        run.error = Some(ForgeError::new(
            "effect.unknown",
            "Result needs confirmation",
        ));
        let record = run.invocations.get_mut("/nodes/write").unwrap();
        record.state = InvocationState::Unknown;
        record.input = json!(7);
        record.output = None;
        record.certainty = EffectCertainty::Unknown;
    })
    .await;
    let mut builder = WorkflowBuilder::standard().execution_store(store);
    builder
        .register_schemas(vec![schema("string", "r2")])
        .unwrap();
    register(&mut builder, d, calls.clone());
    let runtime = boot(builder).await;
    let app = runtime.application();
    let blocked = wait(&app, id.clone()).await;
    let record = &blocked.invocations["/nodes/write"];
    let receipt = app
        .reconcile(
            access(),
            ReconcileCommand {
                command_id: "confirm".into(),
                run_id: id.clone(),
                invocation_id: record.id.clone(),
                expected_revision: blocked.revision,
                observed_attempt: record.attempt_id.clone(),
                resolution: EffectResolution::ConfirmApplied {
                    output: json!(7),
                    evidence: EffectEvidence {
                        authority: "test.destination".into(),
                        reference: "receipt-7".into(),
                        note: "Destination confirms the accepted numeric result".into(),
                    },
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(receipt.status, ResolutionStatus::Applied);
    let done = wait(&app, id).await;
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.error);
    assert_eq!(done.output, Some(json!(7)));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "confirmation must not repeat the external effect"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
