use std::sync::Arc;
use std::time::Instant;

use serde_json::{Value, json};
use workflow_forge_core::runtime::WorkflowExecutor;
use workflow_forge_core::spec::WorkflowDefinition;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::WorkflowData;

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    workflow_forge_ext_util::register(&registry);
    registry
}

async fn run(workflow: Value, trigger: Value) -> WorkflowData {
    let workflow: WorkflowDefinition = serde_json::from_value(workflow).unwrap();
    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    executor.run(WorkflowData(trigger)).await.unwrap()
}

#[test]
fn registra_las_tareas_con_manifiestos() {
    let catalog = registry().catalog();
    let ids: Vec<&str> = catalog.iter().map(|m| m.id.0.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "util.delay",
            "util.idempotency_key",
            "util.log",
            "util.noop"
        ]
    );
    assert!(catalog.iter().all(|m| m.description.is_some()));
}

#[tokio::test]
async fn idempotency_key_es_estable_por_payload() {
    let workflow = json!({
        "name": "idem", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "key", "kind": "task", "task": "util.idempotency_key",
              "input": { "value": "$.trigger" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "key" },
            { "from": "key", "to": "end" }
        ]
    });

    // Igual payload (aunque el orden de llaves difiera) → igual clave.
    let a = run(workflow.clone(), json!({ "sku": "A", "qty": 2 })).await;
    let b = run(workflow.clone(), json!({ "qty": 2, "sku": "A" })).await;
    let c = run(workflow, json!({ "sku": "B", "qty": 2 })).await;

    let key_a = a.0["key"].as_str().unwrap();
    assert_eq!(key_a, b.0["key"].as_str().unwrap());
    assert_ne!(key_a, c.0["key"].as_str().unwrap());
}

#[tokio::test]
async fn noop_log_y_delay_encadenados() {
    let workflow = json!({
        "name": "util-chain", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "paso", "kind": "task", "task": "util.noop" },
            { "id": "audita", "kind": "task", "task": "util.log",
              "input": {
                  "level": "info",
                  "message": "procesando",
                  "value": "$.nodes.paso.output"
              } },
            { "id": "espera", "kind": "task", "task": "util.delay",
              "input": { "ms": 30, "value": "$.nodes.audita.output" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "paso" },
            { "from": "paso", "to": "audita" },
            { "from": "audita", "to": "espera" },
            { "from": "espera", "to": "end" }
        ]
    });

    let inicio = Instant::now();
    let result = run(workflow, json!({ "dato": 42 })).await;
    // El token original atravesó noop → log.value → delay.value
    assert_eq!(result.0, json!({ "dato": 42 }));
    assert!(inicio.elapsed().as_millis() >= 30, "delay no esperó");
}

#[tokio::test]
async fn delay_sin_ms_es_rechazado_por_schema() {
    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "bad", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "espera", "kind": "task", "task": "util.delay", "input": { "value": 1 } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "espera" },
            { "from": "espera", "to": "end" }
        ]
    }))
    .unwrap();

    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    let err = executor.run(WorkflowData(json!({}))).await.unwrap_err();
    assert_eq!(err.code, "TASK_INPUT_INVALID");
    assert!(err.message.contains("ms"), "mensaje: {}", err.message);
}
