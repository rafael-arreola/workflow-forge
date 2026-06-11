use std::sync::Arc;

use serde_json::{Value, json};
use workflow_forge_core::runtime::WorkflowExecutor;
use workflow_forge_core::spec::WorkflowDefinition;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::WorkflowData;

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    workflow_forge_ext_tabular::register(&registry);
    registry
}

/// write → parse dentro de la misma ejecución: el blob vive en el store
/// de la ejecución y se limpia al terminar.
fn roundtrip_workflow(format: &str, name: &str) -> Value {
    json!({
        "name": "tabular-roundtrip", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "escribe", "kind": "task", "task": "tabular.write",
              "input": { "rows": "$.trigger.rows", "format": format, "name": name } },
            { "id": "lee", "kind": "task", "task": "tabular.parse",
              "input": { "file": "$.nodes.escribe.output.file" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "escribe" },
            { "from": "escribe", "to": "lee" },
            { "from": "lee", "to": "end" }
        ]
    })
}

async fn run(workflow: Value, trigger: Value) -> WorkflowData {
    let workflow: WorkflowDefinition = serde_json::from_value(workflow).unwrap();
    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    executor.run(WorkflowData(trigger)).await.unwrap()
}

fn rows() -> Value {
    json!([
        { "ciudad": "Xalapa", "habitantes": 580000, "capital": true },
        { "ciudad": "Veracruz", "habitantes": 607209, "capital": false }
    ])
}

#[tokio::test]
async fn roundtrip_csv() {
    let result = run(
        roundtrip_workflow("csv", "ciudades.csv"),
        json!({ "rows": rows() }),
    )
    .await;
    assert_eq!(result.0["count"], 2);
    assert_eq!(result.0["rows"], rows());
}

#[tokio::test]
async fn roundtrip_xlsx() {
    let result = run(
        roundtrip_workflow("xlsx", "ciudades.xlsx"),
        json!({ "rows": rows() }),
    )
    .await;
    assert_eq!(result.0["count"], 2);
    assert_eq!(result.0["rows"], rows());
}

#[tokio::test]
async fn formato_desconocido_es_error_claro() {
    let workflow = json!({
        "name": "bad", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "escribe", "kind": "task", "task": "tabular.write",
              "input": { "rows": "$.trigger.rows", "format": "csv", "name": "sin-extension" } },
            { "id": "lee", "kind": "task", "task": "tabular.parse",
              "input": { "file": "$.nodes.escribe.output.file" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "escribe" },
            { "from": "escribe", "to": "lee" },
            { "from": "lee", "to": "end" }
        ]
    });

    let workflow: WorkflowDefinition = serde_json::from_value(workflow).unwrap();
    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    let err = executor
        .run(WorkflowData(json!({ "rows": rows() })))
        .await
        .unwrap_err();
    assert_eq!(err.code, "TABULAR_FORMAT_UNKNOWN");
}
