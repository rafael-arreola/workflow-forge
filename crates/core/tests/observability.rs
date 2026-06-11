//! Observabilidad: el executor emite eventos tipados a un ExecutionObserver
//! y InMemoryHistory los convierte en un ExecutionReport por nodo.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::observe::{EventKind, ExecutionStatus, InMemoryHistory, NodeStatus};
use workflow_forge_core::runtime::{WorkflowContext, WorkflowExecutor};
use workflow_forge_core::spec::WorkflowDefinition;
use workflow_forge_core::task::{Task, TaskManifest, TaskRegistry};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

struct EchoTask {
    manifest: TaskManifest,
}

#[async_trait]
impl Task for EchoTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        Ok(input)
    }
}

struct FlakyTask {
    manifest: TaskManifest,
    fail_times: u32,
    calls: AtomicU32,
}

#[async_trait]
impl Task for FlakyTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call <= self.fail_times {
            Err(WorkflowError::new("FLAKY", format!("fallo #{call}")))
        } else {
            Ok(input)
        }
    }
}

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    registry.register(EchoTask {
        manifest: TaskManifest::new("test.echo"),
    });
    registry
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

fn lineal() -> Value {
    json!({
        "name": "lineal", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "eco", "kind": "task", "task": "test.echo" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "eco" },
            { "from": "eco", "to": "end" }
        ]
    })
}

fn event_types(history: &InMemoryHistory) -> Vec<String> {
    history
        .events()
        .iter()
        .map(|e| {
            serde_json::to_value(&e.kind).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn workflow_lineal_emite_la_secuencia_completa() {
    let (executor, history) = executor(lineal(), registry());
    executor.run(WorkflowData(json!({ "x": 1 }))).await.unwrap();

    assert_eq!(
        event_types(&history),
        vec![
            "workflow_started",
            "node_started",   // start
            "node_completed", // start
            "node_started",   // eco
            "task_attempt_started",
            "node_completed", // eco
            "node_started",   // end
            "node_completed", // end
            "workflow_completed",
        ]
    );

    // seq estrictamente creciente y mismo execution_id
    let events = history.events();
    let execution_id = &events[0].execution_id;
    for pair in events.windows(2) {
        assert!(pair[0].seq < pair[1].seq);
        assert_eq!(&pair[1].execution_id, execution_id);
    }

    // payloads completos
    let attempt = &events[4];
    assert!(matches!(
        &attempt.kind,
        EventKind::TaskAttemptStarted { input: Some(input), .. } if input == &json!({ "x": 1 })
    ));
}

#[tokio::test]
async fn los_reintentos_emiten_attempts() {
    let registry = registry();
    registry.register(FlakyTask {
        manifest: TaskManifest::new("test.flaky"),
        fail_times: 2,
        calls: AtomicU32::new(0),
    });
    let workflow = json!({
        "name": "flaky", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "f", "kind": "task", "task": "test.flaky",
              "retry": { "max": 2, "initial_ms": 1 } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "f" },
            { "from": "f", "to": "end" }
        ]
    });
    let (executor, history) = executor(workflow, registry);
    executor.run(WorkflowData(json!({}))).await.unwrap();

    let attempts_failed: Vec<(u32, bool)> = history
        .events()
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::TaskAttemptFailed {
                attempt,
                will_retry,
                ..
            } => Some((*attempt, *will_retry)),
            _ => None,
        })
        .collect();
    assert_eq!(attempts_failed, vec![(1, true), (2, true)]);

    let report = history.report();
    let node = report.nodes.iter().find(|n| n.node_id == "f").unwrap();
    assert_eq!(node.attempts, 3);
    assert_eq!(node.status, NodeStatus::Completed);
}

#[tokio::test]
async fn el_fallo_ruteado_queda_marcado_en_el_reporte() {
    let registry = registry();
    registry.register(FlakyTask {
        manifest: TaskManifest::new("test.siempre_falla"),
        fail_times: u32::MAX,
        calls: AtomicU32::new(0),
    });
    let workflow = json!({
        "name": "ruta-error", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "f", "kind": "task", "task": "test.siempre_falla" },
            { "id": "end", "kind": "end" },
            { "id": "end-error", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "f" },
            { "from": "f", "to": "end" },
            { "from": "f", "on": "error", "to": "end-error" }
        ]
    });
    let (executor, history) = executor(workflow, registry);
    executor.run(WorkflowData(json!({}))).await.unwrap();

    let report = history.report();
    assert_eq!(report.status, ExecutionStatus::Completed);
    let node = report.nodes.iter().find(|n| n.node_id == "f").unwrap();
    assert_eq!(node.status, NodeStatus::ErrorRouted);
    assert_eq!(node.error.as_ref().unwrap().code, "FLAKY");
}

#[tokio::test]
async fn workflow_fallido_y_reporte_global() {
    let registry = registry();
    registry.register(FlakyTask {
        manifest: TaskManifest::new("test.siempre_falla"),
        fail_times: u32::MAX,
        calls: AtomicU32::new(0),
    });
    let workflow = json!({
        "name": "fatal", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "f", "kind": "task", "task": "test.siempre_falla" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "f" },
            { "from": "f", "to": "end" }
        ]
    });
    let (executor, history) = executor(workflow, registry);
    executor.run(WorkflowData(json!({}))).await.unwrap_err();

    let report = history.report();
    assert_eq!(report.status, ExecutionStatus::Failed);
    assert_eq!(report.error.as_ref().unwrap().code, "FLAKY");
    assert!(report.duration_ms.is_some());
    let node = report.nodes.iter().find(|n| n.node_id == "f").unwrap();
    assert_eq!(node.status, NodeStatus::Failed);
}

#[tokio::test]
async fn ramas_paralelas_emiten_todos_sus_nodos() {
    let workflow = json!({
        "name": "paralelo", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "fan", "kind": "gateway", "gateway": "parallel" },
            { "id": "a", "kind": "task", "task": "test.echo" },
            { "id": "b", "kind": "task", "task": "test.echo" },
            { "id": "meet", "kind": "gateway", "gateway": "join" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "fan" },
            { "from": "fan", "to": "a" },
            { "from": "fan", "to": "b" },
            { "from": "a", "to": "meet" },
            { "from": "b", "to": "meet" },
            { "from": "meet", "to": "end" }
        ]
    });
    let (executor, history) = executor(workflow, registry());
    executor.run(WorkflowData(json!({}))).await.unwrap();

    let report = history.report();
    let ids: Vec<&str> = report.nodes.iter().map(|n| n.node_id.as_str()).collect();
    for expected in ["start", "fan", "a", "b", "meet", "end"] {
        assert!(ids.contains(&expected), "falta {expected} en {ids:?}");
    }
    // El join emite started+completed exactamente una vez
    let join_events = history
        .events()
        .iter()
        .filter(|e| e.kind.node_id() == Some("meet"))
        .count();
    assert_eq!(join_events, 2);
}

#[tokio::test]
async fn foreach_emite_eventos_por_elemento() {
    let registry = registry();
    let workflow = json!({
        "name": "lote", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "lote", "kind": "foreach", "task": "test.echo",
              "items": "$.trigger.items", "on_item_error": "collect" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "lote" },
            { "from": "lote", "to": "end" }
        ]
    });
    let (executor, history) = executor(workflow, registry);
    executor
        .run(WorkflowData(json!({ "items": [1, 2, 3] })))
        .await
        .unwrap();

    let report = history.report();
    let node = report.nodes.iter().find(|n| n.node_id == "lote").unwrap();
    assert_eq!(node.kind, "foreach");
    assert_eq!(node.items_ok, Some(3));
    assert_eq!(node.items_failed, Some(0));
}

#[tokio::test]
async fn los_eventos_son_serializables_round_trip() {
    let (executor, history) = executor(lineal(), registry());
    executor.run(WorkflowData(json!({ "x": 1 }))).await.unwrap();

    for event in history.events() {
        let as_json = serde_json::to_value(&event).unwrap();
        assert!(as_json["type"].is_string(), "evento sin type: {as_json}");
        assert!(as_json["execution_id"].is_string());
        let back: workflow_forge_core::observe::ExecutionEvent =
            serde_json::from_value(as_json.clone()).unwrap();
        assert_eq!(back.seq, event.seq);
    }

    // El reporte también serializa
    let report_json = serde_json::to_value(history.report()).unwrap();
    assert_eq!(report_json["status"], json!("completed"));
}
