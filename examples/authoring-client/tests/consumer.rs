use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use workflow_forge::v2::*;
use workflow_forge_authoring_example::{move_node, one_operation};
use workflow_forge_reference_module::text::text_operations;

struct Counted {
    inner: Arc<dyn Operation>,
    calls: Arc<AtomicUsize>,
}
impl Operation for Counted {
    fn descriptor(&self) -> &OperationDescriptor {
        self.inner.descriptor()
    }
    fn execute<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> OperationFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.execute(context, invocation)
    }
}

fn selected() -> OperationRevision {
    OperationRevision::new("example.text.prefix", "1", "r1")
}

async fn boot() -> (EngineRuntime, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut bundle = text_operations();
    bundle.operations = bundle
        .operations
        .into_iter()
        .map(|inner| {
            Arc::new(Counted {
                inner,
                calls: calls.clone(),
            }) as Arc<dyn Operation>
        })
        .collect();
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(bundle).unwrap();
    (
        EngineRuntime::boot(builder.build().unwrap(), BootOptions::default())
            .await
            .unwrap(),
        calls,
    )
}

#[tokio::test]
async fn catalog_consumer_roundtrips_layout_and_executes_without_engine_internals() {
    let (runtime, calls) = boot().await;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let catalog: Vec<OperationDescriptor> =
        serde_json::from_slice(&serde_json::to_vec(&app.catalog(&access).unwrap()).unwrap())
            .unwrap();
    let descriptor = catalog.iter().find(|d| d.revision == selected()).unwrap();
    let mut draft = one_operation(
        &catalog,
        &selected(),
        "authored",
        "r1",
        json!({"prefix":"ID-"}),
    )
    .unwrap();
    assert_eq!(draft.input_schema, descriptor.input_schema);
    assert_eq!(draft.output_schema, descriptor.output_schema);
    draft.presentation.insert(
        "external.editor".into(),
        json!({"opaque":[null, true, "keep"]}),
    );
    draft.presentation.insert(
        "nodes".into(),
        json!({"step":{"label":"Prefix","plugin":{"v":3}}}),
    );
    let semantic = draft.semantic_value();
    move_node(&mut draft, "step", 40.0, -10.0).unwrap();
    let exported = serde_json::to_value(&draft).unwrap();
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/2/workflow.schema.json")).unwrap();
    jsonschema::validator_for(&schema)
        .unwrap()
        .validate(&exported)
        .unwrap();
    let mut imported: WorkflowDefinition = serde_json::from_value(exported).unwrap();
    assert_eq!(imported, draft);
    let plan = app.prepare(access.clone(), imported.clone()).await.unwrap();
    move_node(&mut imported, "step", 300.0, 400.0).unwrap();
    assert_eq!(imported.semantic_value(), semantic);
    assert_eq!(
        imported.presentation["external.editor"],
        draft.presentation["external.editor"]
    );
    assert_eq!(imported.presentation["nodes"]["step"]["label"], "Prefix");
    assert_eq!(
        imported.presentation["nodes"]["step"]["plugin"],
        json!({"v":3})
    );
    app.prepare(access.clone(), imported.clone()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    if let Instruction::Operation { config, .. } = &mut imported.nodes[0].instruction {
        *config = json!({"prefix":"CHANGED-"});
    }
    assert_eq!(
        app.prepare(access.clone(), imported.clone())
            .await
            .err()
            .unwrap()
            .code(),
        "state.conflict"
    );
    imported.revision = "r2".into();
    let new_plan = app.prepare(access.clone(), imported).await.unwrap();
    for (plan, expected) in [(plan, "ID-42"), (new_plan, "CHANGED-42")] {
        let receipt = app
            .start(access.clone(), StartRunRequest::new(plan, json!("42")))
            .await
            .unwrap();
        let run = app.wait(access.clone(), receipt.run_id).await.unwrap();
        assert_eq!(run.state, RunState::Succeeded);
        assert_eq!(run.output, Some(json!(expected)));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn authoring_errors_use_structured_locations_and_never_execute_to_validate() {
    let (runtime, calls) = boot().await;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let catalog = app.catalog(&access).unwrap();
    let bad_config = one_operation(
        &catalog,
        &selected(),
        "invalid.config",
        "r1",
        json!({"prefix":42}),
    )
    .unwrap();
    let error = app.prepare(access.clone(), bad_config).await.err().unwrap();
    let diagnostic = error
        .diagnostics
        .iter()
        .find(|d| d.location.node.as_deref() == Some("step"))
        .unwrap();
    assert_eq!(diagnostic.phase, "prepare");
    assert_eq!(diagnostic.location.field, "/config");
    assert_eq!(diagnostic.location.data.as_deref(), Some("/prefix"));
    let serialized = serde_json::to_value(&error).unwrap();
    let restored: ForgeError = serde_json::from_value(serialized).unwrap();
    assert_eq!(restored.diagnostics, error.diagnostics);

    let mut bad_mapping = one_operation(
        &catalog,
        &selected(),
        "invalid.mapping",
        "r1",
        json!({"prefix":"ID-"}),
    )
    .unwrap();
    bad_mapping.nodes[0].input = Binding::Select(Selection {
        source: DataSource::Node,
        node: Some("absent".into()),
        pointer: String::new(),
        fallback: None,
    });
    let error = app
        .prepare(access.clone(), bad_mapping)
        .await
        .err()
        .unwrap();
    assert!(
        error
            .diagnostics
            .iter()
            .any(|d| d.location.node.as_deref() == Some("step") && d.location.field == "/input")
    );

    let base = one_operation(
        &catalog,
        &selected(),
        "locations",
        "r1",
        json!({"prefix":"ID-"}),
    )
    .unwrap();
    let mut literal = base.clone();
    literal.nodes[0].input = Binding::Literal(json!(42));
    let mut missing = base.clone();
    if let Instruction::Operation { operation, .. } = &mut missing.nodes[0].instruction {
        operation.implementation = "unavailable".into();
    }
    let mut retry = base.clone();
    if let Instruction::Operation { retry, .. } = &mut retry.nodes[0].instruction {
        retry.max_attempts = 0;
    }
    let mut wait = base.clone();
    wait.nodes[0].instruction = Instruction::AwaitSignal {
        correlation: Binding::Literal(json!("correlation")),
        timeout_ms: 1000,
        payload_schema: json!(true),
        start: Some(SignalStart {
            operation: selected(),
            config: json!({"prefix":false}),
            input: Binding::Literal(json!("42")),
            retry: RetryPolicy::default(),
        }),
    };
    for (definition, field, data) in [
        (literal, "/input/literal", Some("")),
        (missing, "/operation", None),
        (retry, "/retry", None),
        (wait, "/start/config", Some("/prefix")),
    ] {
        let error = app.prepare(access.clone(), definition).await.err().unwrap();
        assert_eq!(error.diagnostics[0].location.node.as_deref(), Some("step"));
        assert_eq!(error.diagnostics[0].location.field, field);
        assert_eq!(error.diagnostics[0].location.data.as_deref(), data);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[test]
fn stale_or_ambiguous_catalog_cannot_silently_select_a_different_revision() {
    let bundle = text_operations();
    let descriptor = bundle.operations[0].descriptor().clone();
    let draft = |catalog: &[OperationDescriptor]| {
        one_operation(catalog, &selected(), "draft", "r1", json!({"prefix":""}))
    };
    assert_eq!(
        draft(&[]).unwrap_err().code(),
        "authoring.operation_missing"
    );
    assert_eq!(
        draft(&[descriptor.clone(), descriptor.clone()])
            .unwrap_err()
            .code(),
        "authoring.catalog_ambiguous"
    );
    let mut newer = descriptor.clone();
    newer.revision.implementation = "r2".into();
    assert_eq!(
        draft(&[newer.clone()]).unwrap_err().code(),
        "authoring.operation_missing"
    );
    assert!(draft(&[descriptor.clone(), newer]).is_ok());
    let mut unsupported = descriptor;
    unsupported.schema_dialect = "unrecognized".into();
    assert_eq!(
        draft(&[unsupported]).unwrap_err().code(),
        "authoring.dialect_unsupported"
    );
}

#[test]
fn moving_a_node_rejects_invalid_coordinates_and_preserves_foreign_metadata() {
    let bundle = text_operations();
    let descriptor = bundle.operations[0].descriptor().clone();
    let mut draft = one_operation(
        &[descriptor],
        &selected(),
        "draft",
        "r1",
        json!({"prefix":""}),
    )
    .unwrap();
    for (node, x, y) in [
        ("absent", 0.0, 0.0),
        ("step", f64::NAN, 0.0),
        ("step", 0.0, f64::INFINITY),
    ] {
        let before = draft.clone();
        assert!(move_node(&mut draft, node, x, y).is_err());
        assert_eq!(draft, before);
    }
    for metadata in [json!(true), json!({"step":"foreign-format"})] {
        draft.presentation.insert("nodes".into(), metadata);
        let before = draft.clone();
        assert_eq!(
            move_node(&mut draft, "step", 10.0, 20.0)
                .unwrap_err()
                .code(),
            "authoring.presentation_conflict"
        );
        assert_eq!(draft, before);
    }
}
