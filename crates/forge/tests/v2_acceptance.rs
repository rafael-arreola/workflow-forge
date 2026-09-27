use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use workflow_forge::v2::*;
use workflow_forge_reference_module::{StaticDirectory, customer_operations};

fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn echo(input: Value) -> WorkflowDefinition {
    serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"id":"test.echo","revision":"r1","schema_dialect":SCHEMA_DIALECT,
        "input_schema":true,"output_schema":true,"entry":"echo",
        "nodes":[{"id":"echo","kind":"operation","operation":{"id":"forge.data.identity","contract":"1","implementation":"r1"},"config":{},"input":input}],
        "edges":[],"output":{"select":{"source":"node","node":"echo","pointer":""}}
    })).unwrap()
}
async fn boot(builder: WorkflowBuilder) -> EngineRuntime {
    EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
        .await
        .unwrap()
}
async fn run(
    app: &WorkflowApplication,
    definition: WorkflowDefinition,
    input: Value,
) -> RunSnapshot {
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

#[tokio::test]
async fn c01a_external_module_normalizes_queries_and_returns_typed_data() {
    let mut builder = WorkflowBuilder::standard();
    builder
        .register_bundle(customer_operations(Arc::new(StaticDirectory(
            BTreeMap::from([("C-9".into(), true)]),
        ))))
        .unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let definition: WorkflowDefinition = serde_json::from_str(include_str!(
        "../../../examples/workflows/customer_lookup.v2.json"
    ))
    .unwrap();
    let original = serde_json::to_value(&definition).unwrap();
    let plan = app.prepare(access(), definition.clone()).await.unwrap();
    assert!(
        plan.diagnostics()
            .iter()
            .any(|d| d.code == "compatibility.unknown")
    );
    assert_eq!(serde_json::to_value(plan.definition()).unwrap(), original);
    let input =
        json!({"request_id":"req-17","customer":" C-9 ","items":[{"sku":"A-1","quantity":2}]});
    let completed = run(&app, definition.clone(), input.clone()).await;
    assert_eq!(completed.state, RunState::Succeeded);
    assert_eq!(
        completed.output,
        Some(json!({"customer":"C-9","active":true}))
    );
    let mut invalid = input;
    invalid["items"][0]["quantity"] = json!(0);
    let error = app
        .start(access(), StartRunRequest::new(plan, invalid))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "data.invalid");
    assert_eq!(
        error.diagnostics[0].location.data.as_deref(),
        Some("/items/0/quantity")
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

/// Adapter substitution and an observation decorator use the same public contracts.
#[tokio::test]
async fn external_client_and_decorator_preserve_independent_invocations() {
    struct Directory;
    impl workflow_forge_reference_module::CustomerDirectory for Directory {
        fn active<'a>(&'a self, customer: &'a str) -> PortFuture<'a, bool, OperationError> {
            Box::pin(async move { Ok(customer == "C-9") })
        }
    }
    struct Observed {
        inner: Arc<dyn Operation>,
        calls: Arc<std::sync::Mutex<Vec<(String, String, Value)>>>,
    }
    impl Operation for Observed {
        fn descriptor(&self) -> &OperationDescriptor {
            self.inner.descriptor()
        }
        fn execute<'a>(&'a self, ctx: OperationContext, inv: Invocation) -> OperationFuture<'a> {
            assert_eq!(inv.operation, self.inner.descriptor().revision);
            assert!(inv.effect_key.is_none());
            self.calls.lock().unwrap().push((
                inv.id.clone(),
                inv.attempt_id.clone(),
                inv.input.clone(),
            ));
            self.inner.execute(ctx, inv)
        }
    }
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut bundle = customer_operations(Arc::new(Directory));
    bundle.operations[0] = Arc::new(Observed {
        inner: bundle.operations[0].clone(),
        calls: calls.clone(),
    });
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(bundle).unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let definition: WorkflowDefinition = serde_json::from_str(include_str!(
        "../../../examples/workflows/customer_lookup.v2.json"
    ))
    .unwrap();
    let plan = app.prepare(access(), definition).await.unwrap();
    let start = |customer: &str| {
        StartRunRequest::new(
            plan.clone(),
            json!({"request_id":customer,"customer":customer,"items":[{"sku":"A-1","quantity":1}]}),
        )
    };
    let a = app.start(access(), start(" C-9 ")).await.unwrap();
    let b = app.start(access(), start(" C-10 ")).await.unwrap();
    let (a, b) = tokio::join!(app.wait(access(), a.run_id), app.wait(access(), b.run_id));
    assert_eq!(
        a.unwrap().output,
        Some(json!({"customer":"C-9","active":true}))
    );
    assert_eq!(
        b.unwrap().output,
        Some(json!({"customer":"C-10","active":false}))
    );
    {
        let observed = calls.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_ne!(observed[0].0, observed[1].0);
        assert_ne!(observed[0].1, observed[1].1);
        assert_ne!(observed[0].2, observed[1].2);
    }
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn bindings_distinguish_literal_missing_null_and_scalar_traversal() {
    let runtime = boot(WorkflowBuilder::standard()).await;
    let app = runtime.application();
    let fallback =
        json!({"select":{"source":"input","pointer":"/value","fallback":{"literal":"fallback"}}});
    assert_eq!(
        run(&app, echo(fallback.clone()), json!({})).await.output,
        Some(json!("fallback"))
    );
    assert_eq!(
        run(&app, echo(fallback), json!({"value":null}))
            .await
            .output,
        Some(Value::Null)
    );
    let mut literal = echo(json!({"literal":"$.secret"}));
    literal.revision = "literal".into();
    assert_eq!(
        run(&app, literal, json!({"secret":"hidden"})).await.output,
        Some(json!("$.secret"))
    );
    let mut scalar = echo(
        json!({"select":{"source":"input","pointer":"/value/field","fallback":{"literal":"wrong"}}}),
    );
    scalar.revision = "scalar".into();
    assert_eq!(
        run(&app, scalar, json!({"value":null}))
            .await
            .error
            .unwrap()
            .code(),
        "mapping.invalid"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn prepared_revisions_are_immutable_and_layout_is_not_semantic() {
    let runtime = boot(WorkflowBuilder::standard()).await;
    let app = runtime.application();
    let definition = echo(json!({"literal":1}));
    let plan = app.prepare(access(), definition.clone()).await.unwrap();
    let mut moved = definition.clone();
    moved.presentation.insert("echo".into(), json!({"x":50}));
    app.prepare(access(), moved).await.unwrap();
    let mut changed = definition;
    changed.nodes[0].input = Binding::Literal(json!(2));
    assert_eq!(
        app.prepare(access(), changed).await.err().unwrap().code(),
        "state.conflict"
    );
    let receipt = app
        .start(access(), StartRunRequest::new(plan.clone(), Value::Null))
        .await
        .unwrap();
    assert_eq!(
        app.wait(access(), receipt.run_id).await.unwrap().output,
        Some(json!(1))
    );
    let other = boot(WorkflowBuilder::standard()).await;
    assert_eq!(
        other
            .application()
            .start(access(), StartRunRequest::new(plan, Value::Null))
            .await
            .unwrap_err()
            .code(),
        "state.conflict"
    );
    other.shutdown(ShutdownOptions::default()).await.unwrap();
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn compiler_rejects_invalid_control_mappings_and_offline_refs_before_execution() {
    let runtime = boot(WorkflowBuilder::standard()).await;
    let app = runtime.application();
    let unavailable = echo(json!({"select":{"source":"node","node":"echo","pointer":""}}));
    assert_eq!(
        app.prepare(access(), unavailable)
            .await
            .err()
            .unwrap()
            .code(),
        "mapping.invalid"
    );
    let mut missing = echo(json!({"literal":null}));
    missing.input_schema = json!({"$ref":"https://unregistered.invalid/input.json"});
    assert_eq!(
        app.prepare(access(), missing).await.err().unwrap().code(),
        "reference.missing"
    );
    let mut cyclic = echo(json!({"literal":null}));
    cyclic.edges.push(Edge {
        from: "echo".into(),
        to: "echo".into(),
    });
    assert_eq!(
        app.prepare(access(), cyclic).await.err().unwrap().code(),
        "definition.invalid"
    );
    let mut recursive = echo(json!({"literal":null}));
    recursive.input_schema = json!({"$ref":"#"});
    assert_eq!(
        app.prepare(access(), recursive).await.err().unwrap().code(),
        "capability.unsupported"
    );
    let mut raw = serde_json::to_value(echo(json!({"literal":null}))).unwrap();
    raw["unrecognized"] = json!(true);
    assert_eq!(
        app.prepare_json(access(), &serde_json::to_vec(&raw).unwrap())
            .await
            .err()
            .unwrap()
            .code(),
        "definition.invalid"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

struct Probe {
    descriptor: OperationDescriptor,
    calls: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Notify>,
    mode: u8,
}
impl Operation for Probe {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, ctx: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            if self.mode == 1 {
                self.release.notified().await;
            }
            if self.mode == 2 {
                panic!("simulated extension panic");
            }
            if self.mode == 3 {
                let value = ctx.secret("partner").await.map_err(|_| OperationError {
                    code: "test.secret.denied".into(),
                    class: ErrorClass::Resource,
                    certainty: EffectCertainty::NotApplied,
                    message: "Secret unavailable".into(),
                })?;
                assert_eq!(value.expose(), "private");
            }
            Ok(OperationOutput::json(invocation.input))
        })
    }
}
fn probe(mode: u8) -> (Arc<Probe>, OperationBundle) {
    let mut descriptor = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    descriptor.revision.id = "test.probe".into();
    if mode == 3 {
        descriptor
            .required_resources
            .insert("secret:partner".into());
    }
    let probe = Arc::new(Probe {
        descriptor,
        calls: Arc::new(AtomicUsize::new(0)),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        mode,
    });
    let bundle = OperationBundle {
        inspectors: Vec::new(),
        module: ModuleDescriptor {
            id: "test.probes".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![probe.descriptor.revision.clone()],
        },
        operations: vec![probe.clone()],
    };
    (probe, bundle)
}
fn probe_definition() -> WorkflowDefinition {
    let mut d = echo(json!({"select":{"source":"input","pointer":""}}));
    d.nodes[0].instruction.operation_revision_mut().unwrap().id = "test.probe".into();
    d
}

#[tokio::test]
async fn admission_deduplicates_even_when_full_and_cancel_has_a_terminal_result() {
    let limits = Limits {
        active_runs: 1,
        pending_runs: 0,
        ..Limits::default()
    };
    let (probe, bundle) = probe(1);
    let mut builder = WorkflowBuilder::standard().limits(limits);
    builder.register_bundle(bundle).unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let plan = app.prepare(access(), probe_definition()).await.unwrap();
    let request = || StartRunRequest {
        plan: plan.clone(),
        input: json!({"n":1}),
        options: StartOptions {
            receipt_key: Some("same".into()),
            ..Default::default()
        },
    };
    let first = app.start(access(), request()).await.unwrap();
    probe.started.notified().await;
    let duplicate = app.start(access(), request()).await.unwrap();
    assert_eq!(duplicate.run_id, first.run_id);
    assert!(duplicate.duplicate);
    let mut changed = request();
    changed.input = json!({"n":2});
    assert_eq!(
        app.start(access(), changed).await.unwrap_err().code(),
        "state.conflict"
    );
    let mut another = request();
    another.options.receipt_key = None;
    assert_eq!(
        app.start(access(), another).await.unwrap_err().code(),
        "admission.full"
    );
    app.cancel(access(), first.run_id.clone()).await.unwrap();
    assert_eq!(
        app.wait(access(), first.run_id).await.unwrap().state,
        RunState::Cancelled
    );
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    let handle = app.clone();
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    assert_eq!(
        handle.catalog(&access()).unwrap_err().code(),
        "runtime.unavailable"
    );
}

#[tokio::test]
async fn bundle_registration_is_atomic_and_rejects_unsupported_protocol() {
    let (_, bundle) = probe(0);
    let operation = bundle.operations[0].clone();
    let mut builder = WorkflowBuilder::standard();
    let identity = modules::data_operations().operations[0].clone();
    let mut conflict = bundle;
    conflict
        .module
        .exports
        .push(identity.descriptor().revision.clone());
    conflict.operations.push(identity);
    assert_eq!(
        builder.register_bundle(conflict).unwrap_err().code(),
        "state.conflict"
    );
    let module = ModuleDescriptor {
        id: "test.probes".into(),
        version: "1".into(),
        protocol_version: PROTOCOL_VERSION,
        exports: vec![operation.descriptor().revision.clone()],
    };
    builder
        .register_bundle(OperationBundle {
            inspectors: Vec::new(),
            module: module.clone(),
            operations: vec![operation.clone()],
        })
        .unwrap();
    let mut unsupported = module;
    unsupported.id = "future".into();
    unsupported.protocol_version = 99;
    assert_eq!(
        builder
            .register_bundle(OperationBundle {
                inspectors: Vec::new(),
                module: unsupported,
                operations: vec![operation]
            })
            .unwrap_err()
            .code(),
        "capability.unsupported"
    );
    let runtime = boot(builder).await;
    assert_eq!(runtime.application().catalog(&access()).unwrap().len(), 4);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn permissions_and_resource_grants_apply_to_prepare_and_every_run() {
    let (_, bundle) = probe(3);
    let mut secrets = modules::MemorySecrets::default();
    secrets.insert("default", "partner", "private".into());
    let mut builder = WorkflowBuilder::standard().secret_provider(Arc::new(secrets));
    builder.register_bundle(bundle).unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let definition = probe_definition();
    let plan = app.prepare(access(), definition.clone()).await.unwrap();
    let mut denied = access();
    denied.resources.clear();
    assert_eq!(
        app.start(denied, StartRunRequest::new(plan, Value::Null))
            .await
            .unwrap_err()
            .code(),
        "access.denied"
    );
    let mut stranger = access();
    stranger.scope = "other".into();
    assert_eq!(app.catalog(&stranger).unwrap_err().code(), "access.denied");
    assert_eq!(
        run(&app, definition, json!("ok")).await.output,
        Some(json!("ok"))
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn panic_becomes_a_run_error_and_runtime_stays_usable() {
    let (_, bundle) = probe(2);
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(bundle).unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let failed = run(&app, probe_definition(), Value::Null).await;
    assert_eq!(failed.state, RunState::Failed);
    assert_eq!(failed.error.unwrap().code(), "operation.failed");
    assert!(app.is_ready());
    let mut other = echo(json!({"literal":true}));
    other.id = "other".into();
    assert_eq!(
        run(&app, other, Value::Null).await.output,
        Some(json!(true))
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn failed_boot_releases_store_and_double_ownership_is_rejected() {
    let store = Arc::new(modules::MemoryExecutionStore::default());
    let mut invalid = echo(json!({"literal":1}));
    invalid.nodes[0]
        .instruction
        .operation_revision_mut()
        .unwrap()
        .implementation = "absent".into();
    let result = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap(),
        BootOptions {
            definitions: vec![invalid],
            ..Default::default()
        },
    )
    .await;
    assert_eq!(result.err().unwrap().code(), "reference.missing");
    let runtime = boot(WorkflowBuilder::standard().execution_store(store.clone())).await;
    let other = EngineRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await;
    assert_eq!(other.err().unwrap().code(), "state.conflict");
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    let second = boot(WorkflowBuilder::standard().execution_store(store)).await;
    second.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[test]
fn published_document_schema_supports_visual_authoring_and_rejects_ambiguous_bindings() {
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/2/workflow.schema.json")).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../examples/workflows/customer_lookup.v2.json"
    ))
    .unwrap();
    assert!(validator.is_valid(&fixture));
    let source = include_str!("../../../docs/CONTRACTS.md");
    let example = source
        .split("```json\n")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    let documented: Value = serde_json::from_str(example).unwrap();
    assert!(validator.is_valid(&documented));
    for binding in [
        json!({"literal":1,"select":{"source":"input","pointer":""}}),
        json!({"select":{"source":"input","pointer":"","node":null}}),
        json!({"select":{"source":"input","pointer":"","fallback":null}}),
    ] {
        let mut invalid = documented.clone();
        invalid["nodes"][0]["input"] = binding;
        assert!(!validator.is_valid(&invalid));
        assert!(serde_json::from_value::<WorkflowDefinition>(invalid).is_err());
    }
}

struct SlowObserver;
impl ExecutionObserver for SlowObserver {
    fn observe(&self, _: ExecutionEvent) -> PortFuture<'_, ()> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        })
    }
}

#[tokio::test]
async fn concurrent_runs_and_slow_observation_preserve_independent_results() {
    let runtime = boot(WorkflowBuilder::standard().observer(Arc::new(SlowObserver))).await;
    let app = runtime.application();
    let plan = app
        .prepare(
            access(),
            echo(json!({"select":{"source":"input","pointer":""}})),
        )
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..32 {
        let app = app.clone();
        let plan = plan.clone();
        tasks.spawn(async move {
            let receipt = app
                .start(access(), StartRunRequest::new(plan, json!(n)))
                .await
                .unwrap();
            let result = app.wait(access(), receipt.run_id).await.unwrap();
            assert_eq!(result.output, Some(json!(n)));
        });
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn shutdown_deadline_stops_admission_and_reports_a_forced_drain() {
    let (probe, bundle) = probe(1);
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(bundle).unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let plan = app.prepare(access(), probe_definition()).await.unwrap();
    let receipt = app
        .start(access(), StartRunRequest::new(plan.clone(), Value::Null))
        .await
        .unwrap();
    probe.started.notified().await;
    app.close_admission().await;
    assert_eq!(
        app.start(access(), StartRunRequest::new(plan, Value::Null))
            .await
            .unwrap_err()
            .code(),
        "runtime.unavailable"
    );
    assert_eq!(
        app.status(access(), receipt.run_id).await.unwrap().state,
        RunState::Running
    );
    let report = runtime
        .shutdown(ShutdownOptions {
            timeout: Duration::from_millis(10),
        })
        .await
        .unwrap();
    assert!(report.forced);
    assert!(report.pending.is_empty());
    assert!(!app.is_ready());
}

#[tokio::test]
async fn data_budgets_reject_before_the_next_operation_and_durable_requests_are_explicit() {
    let limits = Limits {
        value_bytes: 16,
        binding_steps: 3,
        ..Default::default()
    };
    let runtime = boot(WorkflowBuilder::standard().limits(limits)).await;
    let app = runtime.application();
    let plan = app
        .prepare(
            access(),
            echo(json!({"select":{"source":"input","pointer":""}})),
        )
        .await
        .unwrap();
    let huge = app
        .start(
            access(),
            StartRunRequest::new(
                plan.clone(),
                json!("a very long string exceeding the budget"),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(huge.code(), "data.invalid");
    let mut durable = StartRunRequest::new(plan, Value::Null);
    durable.options.require_durable = true;
    assert_eq!(
        app.start(access(), durable).await.unwrap_err().code(),
        "capability.unsupported"
    );
    let mut expensive = echo(json!({"array":[{"literal":1},{"literal":2},{"literal":3}]}));
    expensive.revision = "expensive".into();
    assert_eq!(
        app.prepare(access(), expensive).await.err().unwrap().code(),
        "mapping.invalid"
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn registered_schema_resources_are_resolved_without_network() {
    let mut builder = WorkflowBuilder::standard();
    builder
        .register_schemas(vec![SchemaResource {
            uri: "urn:test:input".into(),
            revision: "r1".into(),
            schema: json!({"$schema":SCHEMA_DIALECT,"type":"string"}),
        }])
        .unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let mut definition = echo(json!({"select":{"source":"input","pointer":""}}));
    definition.input_schema = json!({"$ref":"urn:test:input"});
    assert_eq!(
        run(&app, definition, json!("registered")).await.output,
        Some(json!("registered"))
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
