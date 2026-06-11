//! Aristas `on: panic`: un panic en una tarea no tumba el runtime, no
//! reintenta, no cae en `on: error` y solo rutea por `on: panic`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::context::WorkflowContext;
use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::executor::WorkflowExecutor;
use workflow_forge_core::observe::{EventKind, InMemoryHistory, NodeStatus};
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};
use workflow_forge_core::workflow::WorkflowDefinition;

/// Panickea si el input trae `boom: true`; si no, lo devuelve tal cual.
struct BoomTask {
    manifest: TaskManifest,
    calls: Arc<AtomicU32>,
}

impl BoomTask {
    fn new() -> (Self, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        (
            Self {
                manifest: TaskManifest::new("test.boom"),
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }
}

#[async_trait]
impl Task for BoomTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if input.get("boom").and_then(Value::as_bool).unwrap_or(false) {
            panic!("bug simulado en la extensión");
        }
        Ok(input)
    }
}

fn registry() -> (Arc<TaskRegistry>, Arc<AtomicU32>) {
    let registry = Arc::new(TaskRegistry::new());
    let (task, calls) = BoomTask::new();
    registry.register(task);
    (registry, calls)
}

fn executor(
    workflow: Value,
    registry: Arc<TaskRegistry>,
) -> (WorkflowExecutor, Arc<InMemoryHistory>) {
    let history = Arc::new(InMemoryHistory::new());
    let executor =
        WorkflowExecutor::new(
            serde_json::from_value::<WorkflowDefinition>(workflow).unwrap(),
            registry,
        )
        .map_err(|e| format!("{e:?}"))
        .unwrap()
        .with_observer(
            Arc::clone(&history) as Arc<dyn workflow_forge_core::observe::ExecutionObserver>
        );
    (executor, history)
}

/// start → boom → end, con aristas extra opcionales
fn wf(extra_edges: Value, extra_nodes: Value, boom_extra: Value) -> Value {
    let mut boom = json!({ "id": "boom", "kind": "task", "task": "test.boom" });
    boom.as_object_mut()
        .unwrap()
        .extend(boom_extra.as_object().unwrap().clone());
    let mut nodes = vec![
        json!({ "id": "start", "kind": "start" }),
        boom,
        json!({ "id": "end", "kind": "end" }),
    ];
    nodes.extend(extra_nodes.as_array().unwrap().iter().cloned());
    let mut edges = vec![
        json!({ "from": "start", "to": "boom" }),
        json!({ "from": "boom", "to": "end" }),
    ];
    edges.extend(extra_edges.as_array().unwrap().iter().cloned());
    json!({ "name": "panico", "version": "0.1.0", "nodes": nodes, "edges": edges })
}

async fn run(
    workflow: Value,
    registry: Arc<TaskRegistry>,
    trigger: Value,
) -> (Result<WorkflowData, WorkflowError>, Arc<InMemoryHistory>) {
    let (executor, history) = executor(workflow, registry);
    (executor.run(WorkflowData(trigger)).await, history)
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn sin_arista_panic_el_workflow_falla_y_el_runtime_sobrevive() {
    let (registry, _) = registry();
    let (result, _) = run(
        wf(json!([]), json!([]), json!({})),
        registry,
        json!({ "boom": true }),
    )
    .await;

    let err = result.unwrap_err();
    assert_eq!(err.code, "TASK_PANIC");
    assert!(
        err.message.contains("bug simulado"),
        "mensaje: {}",
        err.message
    );
    // El test mismo sigue vivo: el panic no tumbó el runtime
}

#[tokio::test]
async fn con_arista_panic_rutea_y_el_workflow_completa() {
    let (registry, _) = registry();
    let workflow = wf(
        json!([{ "from": "boom", "on": "panic", "to": "end-panico" }]),
        json!([{ "id": "end-panico", "kind": "end" }]),
        json!({}),
    );
    let (result, history) = run(workflow, registry, json!({ "boom": true })).await;

    let output = result.unwrap();
    assert_eq!(output.0["code"], json!("TASK_PANIC"));

    let report = history.report();
    let node = report.nodes.iter().find(|n| n.node_id == "boom").unwrap();
    assert_eq!(node.status, NodeStatus::ErrorRouted);
    assert_eq!(node.error.as_ref().unwrap().code, "TASK_PANIC");
}

#[tokio::test]
async fn un_panic_no_cae_en_la_ruta_de_error() {
    let (registry, _) = registry();
    let workflow = wf(
        json!([{ "from": "boom", "on": "error", "to": "end-error" }]),
        json!([{ "id": "end-error", "kind": "end" }]),
        json!({}),
    );
    let (result, _) = run(workflow, registry, json!({ "boom": true })).await;
    assert_eq!(result.unwrap_err().code, "TASK_PANIC");
}

#[tokio::test]
async fn un_error_normal_no_cae_en_la_ruta_de_panic() {
    let registry = Arc::new(TaskRegistry::new());
    struct FailTask(TaskManifest);
    #[async_trait]
    impl Task for FailTask {
        fn manifest(&self) -> &TaskManifest {
            &self.0
        }
        async fn execute(&self, _ctx: &WorkflowContext, _input: WorkflowData) -> WorkflowResult {
            Err(WorkflowError::new("FALLO_NORMAL", "error operacional"))
        }
    }
    registry.register(FailTask(TaskManifest::new("test.boom")));

    let workflow = wf(
        json!([{ "from": "boom", "on": "panic", "to": "end-panico" }]),
        json!([{ "id": "end-panico", "kind": "end" }]),
        json!({}),
    );
    let (result, _) = run(workflow, registry, json!({})).await;
    assert_eq!(result.unwrap_err().code, "FALLO_NORMAL");
}

#[tokio::test]
async fn un_panic_no_reintenta() {
    let (registry, calls) = registry();
    let workflow = wf(
        json!([]),
        json!([]),
        json!({ "retry": { "max": 3, "initial_ms": 1 } }),
    );
    let (result, history) = run(workflow, registry, json!({ "boom": true })).await;

    assert_eq!(result.unwrap_err().code, "TASK_PANIC");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "el panic no debe reintentar"
    );

    let attempts: Vec<bool> = history
        .events()
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::TaskAttemptFailed { will_retry, .. } => Some(*will_retry),
            _ => None,
        })
        .collect();
    assert_eq!(attempts, vec![false]);
}

#[tokio::test]
async fn en_foreach_collect_un_panic_aborta_el_nodo_y_rutea() {
    let (registry, _) = registry();
    let workflow = json!({
        "name": "lote-panico", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "lote", "kind": "foreach", "task": "test.boom",
              "items": "$.trigger.items", "on_item_error": "collect" },
            { "id": "end", "kind": "end" },
            { "id": "end-panico", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "lote" },
            { "from": "lote", "to": "end" },
            { "from": "lote", "on": "panic", "to": "end-panico" }
        ]
    });
    let (result, _) = run(
        workflow,
        registry,
        json!({ "items": [{ "boom": false }, { "boom": true }, { "boom": false }] }),
    )
    .await;

    // collect no colecciona panics: el nodo aborta y rutea por on:panic
    let output = result.unwrap();
    assert_eq!(output.0["code"], json!("TASK_PANIC"));
}

#[tokio::test]
async fn los_eventos_distinguen_el_panic() {
    let (registry, _) = registry();
    let workflow = wf(
        json!([{ "from": "boom", "on": "panic", "to": "end-panico" }]),
        json!([{ "id": "end-panico", "kind": "end" }]),
        json!({}),
    );
    let (result, history) = run(workflow, registry, json!({ "boom": true })).await;
    result.unwrap();

    let mut saw_attempt = false;
    let mut saw_node_failed = false;
    for event in history.events() {
        match &event.kind {
            EventKind::TaskAttemptFailed {
                error, will_retry, ..
            } => {
                assert_eq!(error.code, "TASK_PANIC");
                assert!(!will_retry);
                saw_attempt = true;
            }
            EventKind::NodeFailed {
                error,
                error_routed,
                ..
            } => {
                assert_eq!(error.code, "TASK_PANIC");
                assert!(*error_routed);
                saw_node_failed = true;
            }
            _ => {}
        }
    }
    assert!(saw_attempt && saw_node_failed);
}
