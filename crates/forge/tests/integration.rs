use serde_json::json;
use workflow_forge::prelude::*;

#[test]
fn default_registry_incluye_las_extensiones_habilitadas() {
    let catalog = workflow_forge::default_registry().catalog();
    let ids: Vec<&str> = catalog.iter().map(|m| m.id.0.as_str()).collect();

    for expected in [
        "data.merge",
        "data.template",
        "data.transform",
        "http.request",
        "util.delay",
        "util.log",
        "util.noop",
    ] {
        assert!(ids.contains(&expected), "falta {expected} en {ids:?}");
    }
    #[cfg(feature = "tabular")]
    for expected in ["tabular.parse", "tabular.write"] {
        assert!(ids.contains(&expected), "falta {expected} en {ids:?}");
    }
}

#[tokio::test]
async fn workflow_cruzando_extensiones() {
    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "cross", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "saluda", "kind": "task", "task": "data.template",
              "input": { "template": "Hola {who}", "values": "$.trigger" } },
            { "id": "audita", "kind": "task", "task": "util.log",
              "input": { "message": "$.nodes.saluda.output", "value": "$.nodes.saluda.output" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "saluda" },
            { "from": "saluda", "to": "audita" },
            { "from": "audita", "to": "end" }
        ]
    }))
    .unwrap();

    let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry()).unwrap();
    let result = executor
        .run(WorkflowData(json!({ "who": "mundo" })))
        .await
        .unwrap();
    assert_eq!(result.0, json!("Hola mundo"));
}
