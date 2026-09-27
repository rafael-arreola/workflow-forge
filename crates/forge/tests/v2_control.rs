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

fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn select(pointer: &str) -> Value {
    json!({"select":{"source":"input","pointer":pointer}})
}
fn result(node: &str) -> Value {
    json!({"select":{"source":"node","node":node,"pointer":""}})
}
fn operation(id: &str, action: &str, input: Value) -> Value {
    json!({"id":id,"kind":"operation","operation":{"id":"test.control","contract":"1","implementation":"r1"},"input":input,"config":{"action":action}})
}
fn body(node: Value) -> Value {
    json!({"entry":node["id"],"nodes":[node.clone()],"edges":[],"output":result(node["id"].as_str().unwrap())})
}
fn definition(id: &str, node: Value) -> WorkflowDefinition {
    let mut value = body(node);
    value["format"] = json!(WORKFLOW_FORMAT);
    value["id"] = json!(id);
    value["revision"] = json!("r1");
    value["schema_dialect"] = json!(SCHEMA_DIALECT);
    value["input_schema"] = json!(true);
    value["output_schema"] = json!(true);
    // Authoring schema and Rust must accept the same structured documents.
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/2/workflow.schema.json")).unwrap();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&value));
    serde_json::from_value(value).unwrap()
}
fn parallel(branches: Value, concurrency: usize, errors: &str) -> Value {
    json!({"id":"group","kind":"parallel","input":select(""),"branches":branches,"concurrency":concurrency,"errors":errors,"join":"all"})
}
fn foreach(concurrency: usize, errors: &str) -> Value {
    json!({"id":"group","kind":"foreach","input":select(""),"items":select(""),"body":body(operation("step","echo",select("/item"))),"concurrency":concurrency,"errors":errors})
}

#[derive(Default)]
struct Observation {
    calls: Mutex<Vec<Invocation>>,
    completed: Mutex<Vec<Value>>,
    active: AtomicUsize,
    peak: AtomicUsize,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Probe {
    descriptor: OperationDescriptor,
    observed: Arc<Observation>,
}
impl Operation for Probe {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            self.observed.calls.lock().unwrap().push(invocation.clone());
            let count = self.observed.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.observed.peak.fetch_max(count, Ordering::SeqCst);
            let _active = Active(&self.observed.active);
            if let Some(delay) = invocation.input["delay_ms"].as_u64() {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if invocation.config["action"] == "fail" || invocation.input["fail"] == true {
                return Err(OperationError {
                    code: "test.control.rejected".into(),
                    class: ErrorClass::Rejected,
                    certainty: EffectCertainty::NotApplied,
                    message: "Controlled rejection".into(),
                });
            }
            let output = if invocation.config["action"] == "increment" {
                let next = invocation.input["n"].as_u64().unwrap() + 1;
                json!({"n":next,"more":next < 3})
            } else {
                invocation.input
            };
            self.observed.completed.lock().unwrap().push(output.clone());
            Ok(OperationOutput::json(output))
        })
    }
}
fn builder(limits: Limits, restricted: bool) -> (WorkflowBuilder, Arc<Observation>) {
    let observed = Arc::new(Observation::default());
    let mut descriptor = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    descriptor.revision.id = "test.control".into();
    descriptor.config_schema = json!(true);
    if restricted {
        descriptor
            .required_resources
            .insert("secret:partner".into());
    }
    let mut builder = WorkflowBuilder::standard().limits(limits);
    if restricted {
        let mut secrets = modules::MemorySecrets::default();
        secrets.insert("default", "partner", "test-only".into());
        builder = builder.secret_provider(Arc::new(secrets));
    }
    builder
        .register_bundle(OperationBundle {
            module: ModuleDescriptor {
                id: "test.control".into(),
                version: "1".into(),
                protocol_version: PROTOCOL_VERSION,
                exports: vec![descriptor.revision.clone()],
            },
            operations: vec![Arc::new(Probe {
                descriptor,
                observed: observed.clone(),
            })],
            inspectors: Vec::new(),
        })
        .unwrap();
    (builder, observed)
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
async fn decisions_select_first_true_or_fallback_without_invoking_skipped_bodies() {
    let (builder, observed) = builder(Limits::default(), false);
    let runtime = boot(builder).await;
    let app = runtime.application();
    let node = json!({"id":"choose","kind":"decision","input":select(""),"cases":[
        {"id":"first","when":select("/active"),"body":body(operation("same","echo",json!({"literal":"first"})))},
        {"id":"second","when":select("/active"),"body":body(operation("same","fail",json!({"literal":null})))}
    ],"fallback":{"id":"fallback","body":body(operation("same","echo",json!({"literal":"fallback"})))}});
    let d = definition("decisions", node.clone());
    let first = run(&app, d.clone(), json!({"active":true})).await;
    assert_eq!(
        first.output,
        Some(json!({"selected":"first","output":"first"}))
    );
    let fallback = run(&app, d.clone(), json!({"active":false})).await;
    assert_eq!(
        fallback.output,
        Some(json!({"selected":"fallback","output":"fallback"}))
    );
    let invalid = run(&app, d, json!({"active":"true"})).await;
    assert_eq!(invalid.state, RunState::Failed);
    assert_eq!(invalid.error.unwrap().code(), "data.invalid");
    let mut no_match = node;
    no_match.as_object_mut().unwrap().remove("fallback");
    let missing = run(
        &app,
        definition("no-match", no_match),
        json!({"active":false}),
    )
    .await;
    assert_eq!(missing.error.unwrap().code(), "control.no_match");
    assert_eq!(observed.calls.lock().unwrap().len(), 2);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn parallel_joins_by_branch_and_scoped_ids_cannot_alias() {
    let (builder, observed) = builder(Limits::default(), false);
    let runtime = boot(builder).await;
    let branches = json!({
        "a/nodes/b":body(operation("c~d","echo",json!({"literal":{"delay_ms":50,"name":"slow"}}))),
        "a":body(operation("b/nodes/c~d","echo",json!({"literal":{"delay_ms":1,"name":"fast"}})))
    });
    let completed = run(
        &runtime.application(),
        definition("parallel", parallel(branches, 2, "collect")),
        Value::Null,
    )
    .await;
    assert_eq!(completed.state, RunState::Succeeded);
    let output = completed.output.unwrap();
    assert_eq!(output["a"]["output"]["name"], "fast");
    assert_eq!(output["a/nodes/b"]["output"]["name"], "slow");
    assert!(
        completed
            .invocations
            .contains_key("/nodes/group/branches/a~1nodes~1b/nodes/c~0d")
    );
    assert!(
        completed
            .invocations
            .contains_key("/nodes/group/branches/a/nodes/b~1nodes~1c~0d")
    );
    assert_eq!(observed.peak.load(Ordering::SeqCst), 2);
    assert_eq!(observed.completed.lock().unwrap()[0]["name"], "fast");
    let ids: BTreeSet<_> = observed
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.id.clone())
        .collect();
    assert_eq!(ids.len(), 2);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn foreach_preserves_source_order_distinct_duplicates_and_host_concurrency() {
    let (builder, observed) = builder(
        Limits {
            group_concurrency: 2,
            ..Default::default()
        },
        false,
    );
    let runtime = boot(builder).await;
    let app = runtime.application();
    let d = definition("foreach", foreach(99, "collect"));
    let items =
        json!([{"delay_ms":60,"name":"slow"},{"delay_ms":1,"name":"fast"},{"same":1},{"same":1}]);
    let completed = run(&app, d.clone(), items.clone()).await;
    assert_eq!(completed.state, RunState::Succeeded);
    let output = completed.output.unwrap();
    for i in 0..4 {
        assert_eq!(output[i]["index"], i);
        assert_eq!(output[i]["output"], items[i]);
    }
    assert_eq!(observed.peak.load(Ordering::SeqCst), 2);
    assert_eq!(observed.completed.lock().unwrap()[0]["name"], "fast");
    let ids: BTreeSet<_> = observed
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.id.clone())
        .collect();
    assert_eq!(ids.len(), 4);
    assert_eq!(run(&app, d, json!([])).await.output, Some(json!([])));
    assert_eq!(observed.calls.lock().unwrap().len(), 4);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn collect_keeps_known_errors_while_fail_fast_stops_admitting_items() {
    let (builder, observed) = builder(Limits::default(), false);
    let runtime = boot(builder).await;
    let app = runtime.application();
    let items = json!([{"fail":true},{"name":"valid"}]);
    let collected = run(
        &app,
        definition("collect", foreach(1, "collect")),
        items.clone(),
    )
    .await;
    assert_eq!(collected.state, RunState::Succeeded);
    let output = collected.output.unwrap();
    assert_eq!(output[0]["status"], "failed");
    assert_eq!(output[1]["status"], "succeeded");
    let failed = run(
        &app,
        definition("fail-fast", foreach(1, "fail_fast")),
        items,
    )
    .await;
    assert_eq!(failed.state, RunState::Failed);
    assert!(
        !failed
            .invocations
            .contains_key("/nodes/group/items/1/nodes/step")
    );
    assert_eq!(observed.calls.lock().unwrap().len(), 3);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn loop_carries_state_checks_before_iteration_and_enforces_limit_policy() {
    let (builder, observed) = builder(Limits::default(), false);
    let runtime = boot(builder).await;
    let app = runtime.application();
    let node = |max, policy| json!({"id":"repeat","kind":"loop","input":select(""),"while":select("/state/more"),"body":body(operation("increment","increment",select("/state"))),"max_iterations":max,"on_limit":policy});
    let completed = run(
        &app,
        definition("loop", node(5, "fail")),
        json!({"n":0,"more":true}),
    )
    .await;
    assert_eq!(completed.output, Some(json!({"n":3,"more":false})));
    assert_eq!(completed.invocations.len(), 4);
    let skipped = run(
        &app,
        definition("skip", node(5, "fail")),
        json!({"n":8,"more":false}),
    )
    .await;
    assert_eq!(skipped.output, Some(json!({"n":8,"more":false})));
    assert_eq!(skipped.invocations.len(), 1);
    let failed = run(
        &app,
        definition("limit-fail", node(2, "fail")),
        json!({"n":0,"more":true}),
    )
    .await;
    assert_eq!(failed.state, RunState::Failed);
    assert_eq!(failed.error.unwrap().code(), "control.iteration_limit");
    let last = run(
        &app,
        definition("limit-last", node(2, "return_last")),
        json!({"n":0,"more":true}),
    )
    .await;
    assert_eq!(last.output, Some(json!({"n":2,"more":true})));
    assert_eq!(observed.calls.lock().unwrap().len(), 7);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

fn subworkflow(id: &str, workflow: &str, input: Value) -> Value {
    json!({"id":id,"kind":"subworkflow","input":input,"workflow":{"id":workflow,"revision":"r1"}})
}

#[tokio::test]
async fn subworkflows_share_run_capacity_but_have_local_inputs_outputs_and_identities() {
    let (mut builder, observed) = builder(
        Limits {
            active_runs: 1,
            ..Default::default()
        },
        false,
    );
    builder
        .register_workflow(definition("child", operation("echo", "echo", select(""))))
        .unwrap();
    let runtime = boot(builder).await;
    let branches = json!({
        "left":body(subworkflow("call","child",json!({"literal":"L"}))),
        "right":body(subworkflow("call","child",json!({"literal":"R"})))
    });
    let completed = run(
        &runtime.application(),
        definition("parent", parallel(branches, 2, "collect")),
        Value::Null,
    )
    .await;
    assert_eq!(completed.state, RunState::Succeeded);
    let output = completed.output.unwrap();
    assert_eq!(output["left"]["output"], "L");
    assert_eq!(output["right"]["output"], "R");
    let calls = observed.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    assert_ne!(calls[0].id, calls[1].id);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[test]
fn missing_and_cyclic_subworkflows_reject_composition_before_boot() {
    let (mut missing, _) = builder(Limits::default(), false);
    missing
        .register_workflow(definition("a", subworkflow("call", "absent", select(""))))
        .unwrap();
    assert_eq!(missing.build().err().unwrap().code(), "reference.missing");
    let (mut cyclic, _) = builder(Limits::default(), false);
    cyclic
        .register_workflow(definition("a", subworkflow("call", "b", select(""))))
        .unwrap();
    cyclic
        .register_workflow(definition("b", subworkflow("call", "a", select(""))))
        .unwrap();
    assert_eq!(cyclic.build().err().unwrap().code(), "definition.invalid");
}

#[tokio::test]
async fn nested_resources_are_authorized_before_prepare_and_before_each_start() {
    let (mut builder, observed) = builder(Limits::default(), true);
    builder
        .register_workflow(definition("child", operation("echo", "echo", select(""))))
        .unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let d = definition("parent", subworkflow("call", "child", select("")));
    let mut denied = access();
    denied.resources.clear();
    assert_eq!(
        app.prepare(denied.clone(), d.clone())
            .await
            .err()
            .unwrap()
            .code(),
        "access.denied"
    );
    let plan = app.prepare(access(), d).await.unwrap();
    assert_eq!(
        app.start(denied, StartRunRequest::new(plan, Value::Null))
            .await
            .unwrap_err()
            .code(),
        "access.denied"
    );
    assert!(observed.calls.lock().unwrap().is_empty());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn child_schema_failure_and_parent_reference_are_not_hidden_by_subworkflow() {
    let (mut builder, _) = builder(Limits::default(), false);
    let mut child = definition("child", operation("echo", "echo", select("")));
    child.output_schema = json!({"type":"string"});
    builder.register_workflow(child).unwrap();
    let runtime = boot(builder).await;
    let app = runtime.application();
    let completed = run(
        &app,
        definition("parent", subworkflow("call", "child", select(""))),
        json!(42),
    )
    .await;
    assert_eq!(completed.state, RunState::Failed);
    assert_eq!(completed.error.unwrap().code(), "data.invalid");
    assert_eq!(
        completed.invocations["/nodes/call"].state,
        InvocationState::Failed
    );
    assert_eq!(
        completed.invocations["/nodes/call/workflows/child/revisions/r1/nodes/echo"].state,
        InvocationState::Succeeded
    );
    let invalid = definition(
        "illegal-parent",
        parallel(
            json!({"branch":body(operation("step","echo",result("group")))}),
            1,
            "collect",
        ),
    );
    assert!(app.prepare(access(), invalid).await.is_err());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn dynamic_item_and_nested_scope_quotas_fail_without_deadlock_or_extra_dispatch() {
    let (builder, observed) = builder(
        Limits {
            foreach_items: 2,
            active_scopes: 1,
            ..Default::default()
        },
        false,
    );
    let runtime = boot(builder).await;
    let app = runtime.application();
    let over = run(
        &app,
        definition("too-many", foreach(2, "collect")),
        json!([1, 2, 3]),
    )
    .await;
    assert_eq!(over.state, RunState::Failed);
    assert_eq!(over.error.unwrap().code(), "resource.limit");
    let inner = parallel(
        json!({"leaf":body(operation("echo","echo",select("")))}),
        1,
        "fail_fast",
    );
    let outer = parallel(json!({"nested":body(inner)}), 1, "fail_fast");
    let nested = run(&app, definition("scope-budget", outer), Value::Null).await;
    assert_eq!(nested.state, RunState::Failed);
    assert_eq!(nested.error.unwrap().code(), "resource.limit");
    assert!(observed.calls.lock().unwrap().is_empty());
    assert!(app.is_ready());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn control_result_budget_records_failure_after_confirmed_children() {
    let (builder, _) = builder(
        Limits {
            value_bytes: 256,
            ..Default::default()
        },
        false,
    );
    let runtime = boot(builder).await;
    let leaf = body(operation(
        "echo",
        "echo",
        json!({"literal":"x".repeat(150)}),
    ));
    let completed = run(
        &runtime.application(),
        definition(
            "large-join",
            parallel(json!({"a":leaf,"b":leaf}), 2, "collect"),
        ),
        Value::Null,
    )
    .await;
    assert_eq!(completed.state, RunState::Failed);
    assert_eq!(completed.error.unwrap().code(), "resource.limit");
    assert_eq!(
        completed.invocations["/nodes/group"].state,
        InvocationState::Failed
    );
    assert!(
        completed
            .invocations
            .values()
            .filter(|r| r.operation.is_some())
            .all(|r| r.state == InvocationState::Succeeded)
    );
    let decision = json!({"id":"choose","kind":"decision","input":select(""),"cases":[{
        "id":"selected","when":{"literal":true},
        "body":body(operation("echo","echo",json!({"literal":"x".repeat(240)})))
    }]});
    let oversized = run(
        &runtime.application(),
        definition("large-decision", decision),
        Value::Null,
    )
    .await;
    assert_eq!(oversized.state, RunState::Failed);
    assert_eq!(oversized.error.unwrap().code(), "data.invalid");
    assert_eq!(
        oversized.invocations["/nodes/choose"].state,
        InvocationState::Failed
    );
    assert_eq!(
        oversized.invocations["/nodes/choose/cases/selected/nodes/echo"].state,
        InvocationState::Succeeded
    );
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn expanded_plan_accounts_for_schema_payload_and_nested_depth_before_run() {
    let (builder, observed) = builder(
        Limits {
            plan_bytes: 2048,
            control_depth: 1,
            ..Default::default()
        },
        false,
    );
    let runtime = boot(builder).await;
    let app = runtime.application();
    let mut schema_heavy = definition("schema-budget", operation("echo", "echo", select("")));
    schema_heavy.input_schema = json!({"enum":["x".repeat(2100)]});
    assert_eq!(
        app.prepare(access(), schema_heavy)
            .await
            .err()
            .unwrap()
            .code(),
        "resource.limit"
    );
    let inner = parallel(
        json!({"leaf":body(operation("echo","echo",select("")))}),
        1,
        "collect",
    );
    let outer = parallel(json!({"nested":body(inner)}), 1, "collect");
    assert_eq!(
        app.prepare(access(), definition("depth-budget", outer))
            .await
            .err()
            .unwrap()
            .code(),
        "resource.limit"
    );
    assert!(observed.calls.lock().unwrap().is_empty());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

fn guarded(action: &str) -> Value {
    json!({"id":"guard","kind":"try","input":select(""),
        "body":body(operation("work",action,select(""))),
        "catches":[{"id":"rejected","code":"test.control.rejected","body":body(operation("handled","echo",select("")))}],
        "fallback":{"id":"other","body":body(operation("fallback","echo",select("")))}})
}

#[tokio::test]
async fn try_routes_success_exact_errors_and_fallback_without_repeating_the_operation() {
    let (builder, observed) = builder(Limits::default(), false);
    let runtime = boot(builder).await;
    let app = runtime.application();
    let success = run(&app, definition("success", guarded("echo")), json!(42)).await;
    assert_eq!(
        success.output,
        Some(json!({"outcome":"success","output":42}))
    );
    for (id, exact) in [("exact", true), ("fallback", false)] {
        let mut guard = guarded("fail");
        if !exact {
            guard["catches"] = json!([]);
        }
        let result = run(&app, definition(id, guard), json!({"value":42})).await;
        assert_eq!(result.state, RunState::Succeeded, "{:?}", result.error);
        let output = result.output.unwrap();
        assert_eq!(output["outcome"], "handled");
        assert_eq!(output["handler"], if exact { "rejected" } else { "other" });
        assert_eq!(output["output"]["input"]["value"], 42);
        assert_eq!(
            output["output"]["error"]["diagnostics"][0]["operation_error"]["code"],
            "test.control.rejected"
        );
    }
    assert_eq!(observed.calls.lock().unwrap().len(), 5);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn try_requires_a_fallback_and_unique_codes_and_propagates_handler_failure() {
    let (builder, _) = builder(Limits::default(), false);
    let runtime = boot(builder).await;
    let app = runtime.application();
    let mut node = guarded("fail");
    node.as_object_mut().unwrap().remove("fallback");
    assert!(serde_json::from_value::<NodeDefinition>(node).is_err());
    let mut node = guarded("fail");
    let mut duplicate = node["catches"][0].clone();
    duplicate["id"] = json!("duplicate");
    node["catches"].as_array_mut().unwrap().push(duplicate);
    assert_eq!(
        app.prepare(access(), definition("duplicate", node))
            .await
            .err()
            .unwrap()
            .code(),
        "definition.invalid"
    );
    let mut node = guarded("fail");
    node["catches"][0]["body"] = body(operation("handler", "fail", select("")));
    let failed = run(&app, definition("handler-failure", node), Value::Null).await;
    assert_eq!(failed.state, RunState::Failed);
    assert_eq!(failed.invocations.len(), 3);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn owned_execution_returns_output_and_cancels_on_host_request_or_future_drop() {
    let (builder, observed) = builder(Limits::default(), false);
    let runtime = boot(builder).await;
    let app = runtime.application();
    let plan = app
        .prepare(
            access(),
            definition("owned", operation("work", "echo", select(""))),
        )
        .await
        .unwrap();
    let result = app
        .execute(
            access(),
            StartRunRequest::new(plan.clone(), json!(42)),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!(42));
    for drop_future in [false, true] {
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let handle = app.clone();
        let request = StartRunRequest::new(plan.clone(), json!({"delay_ms":60_000}));
        let task = tokio::spawn(async move { handle.execute(access(), request, token).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while observed.active.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if drop_future {
            task.abort();
            let _ = task.await;
        } else {
            cancellation.cancel();
            let error = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code(), "operation.cancelled");
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while observed.active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let report = runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    assert!(!report.forced);
    assert!(report.pending.is_empty());
}

#[tokio::test]
async fn try_cannot_catch_the_global_deadline() {
    let (builder, observed) = builder(
        Limits {
            run_timeout_ms: 25,
            ..Default::default()
        },
        false,
    );
    let runtime = boot(builder).await;
    let failed = run(
        &runtime.application(),
        definition("deadline", guarded("echo")),
        json!({"delay_ms":60_000}),
    )
    .await;
    assert_eq!(failed.state, RunState::Failed);
    assert_eq!(failed.error.unwrap().code(), "operation.timeout");
    assert_eq!(observed.calls.lock().unwrap().len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn boot_requires_explicit_recovery_and_resumes_a_selected_error_handler() {
    let store = Arc::new(modules::MemoryExecutionStore::default());
    let (builder, observed) = builder(Limits::default(), false);
    let runtime = boot(builder.execution_store(store.clone())).await;
    let app = runtime.application();
    let mut node = guarded("fail");
    node["catches"][0]["body"] =
        body(json!({"id":"pause","kind":"timer","duration_ms":100,"input":select("/input")}));
    let plan = app
        .prepare(access(), definition("recovery-handler", node))
        .await
        .unwrap();
    let id = app
        .start(access(), StartRunRequest::new(plan, json!(42)))
        .await
        .unwrap()
        .run_id;
    tokio::time::timeout(Duration::from_secs(2), async {
        while app.status(access(), id.clone()).await.unwrap().state != RunState::Waiting {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    let (builder, after) = self::builder(Limits::default(), false);
    let rejected = EngineRuntime::boot(
        builder.execution_store(store.clone()).build().unwrap(),
        BootOptions::default(),
    )
    .await;
    assert_eq!(rejected.err().unwrap().code(), "recovery.required");
    assert_eq!(observed.calls.lock().unwrap().len(), 1);
    assert!(after.calls.lock().unwrap().is_empty());
    let (builder, after) = self::builder(Limits::default(), false);
    let runtime = EngineRuntime::boot(
        builder.execution_store(store).build().unwrap(),
        BootOptions {
            recovery: RecoveryPolicy::Resume,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let run = tokio::time::timeout(
        Duration::from_secs(2),
        runtime.application().wait(access(), id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(run.state, RunState::Succeeded, "{:?}", run.error);
    assert_eq!(run.output.unwrap()["handler"], "rejected");
    assert!(after.calls.lock().unwrap().is_empty());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}
