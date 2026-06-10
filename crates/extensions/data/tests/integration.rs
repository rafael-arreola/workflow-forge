use std::sync::Arc;

use serde_json::{Value, json};
use workflow_forge_core::executor::WorkflowExecutor;
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::task::WorkflowData;
use workflow_forge_core::workflow::WorkflowDefinition;

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    workflow_forge_ext_data::register(&registry);
    registry
}

async fn run(workflow: Value, trigger: Value) -> WorkflowData {
    let workflow: WorkflowDefinition = serde_json::from_value(workflow).unwrap();
    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    executor.run(WorkflowData(trigger)).await.unwrap()
}

#[tokio::test]
async fn pipeline_transform_merge_template() {
    // Simula el caso típico: reshape de un payload, merge con defaults,
    // y construcción de un string final — todo declarativo.
    let workflow = json!({
        "name": "data-pipeline", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "reshape", "kind": "task", "task": "data.transform",
              "input": {
                  "source": "$.trigger",
                  "shape": { "nombre": "@.user.name", "ciudad": "@.user.address.city" }
              } },
            { "id": "defaults", "kind": "task", "task": "data.merge",
              "input": { "objects": [ { "ciudad": "CDMX", "pais": "MX" }, "$.nodes.reshape.output" ] } },
            { "id": "saluda", "kind": "task", "task": "data.template",
              "input": {
                  "template": "Hola {nombre} desde {ciudad}, {pais}",
                  "values": "$.nodes.defaults.output"
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "reshape" },
            { "from": "reshape", "to": "defaults" },
            { "from": "defaults", "to": "saluda" },
            { "from": "saluda", "to": "end" }
        ]
    });

    // user.address.city no existe → null en reshape → merge conserva el default? No:
    // merge reemplaza con null (posteriores ganan). Usamos un trigger completo.
    let result = run(
        workflow,
        json!({ "user": { "name": "ada", "address": { "city": "Xalapa" } } }),
    )
    .await;
    assert_eq!(result.0, json!("Hola ada desde Xalapa, MX"));
}

#[tokio::test]
async fn map_reestructura_cada_fila() {
    // El caso CSV→JSON con otra estructura: renombrar columnas y agregar
    // campos constantes, fila por fila.
    let workflow = json!({
        "name": "remap", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "renombra", "kind": "task", "task": "data.map",
              "input": {
                  "items": "$.trigger.rows",
                  "shape": {
                      "full_name": "@.nombre",
                      "city": "@.direccion.ciudad",
                      "source": "csv-import"
                  }
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "renombra" },
            { "from": "renombra", "to": "end" }
        ]
    });

    let result = run(
        workflow,
        json!({ "rows": [
            { "nombre": "ada", "direccion": { "ciudad": "Xalapa" } },
            { "nombre": "alan", "direccion": {} }
        ]}),
    )
    .await;

    assert_eq!(
        result.0,
        json!([
            { "full_name": "ada", "city": "Xalapa", "source": "csv-import" },
            { "full_name": "alan", "city": null, "source": "csv-import" }
        ])
    );
}

#[test]
fn catalogo_con_schemas() {
    let catalog = registry().catalog();
    let ids: Vec<&str> = catalog.iter().map(|m| m.id.0.as_str()).collect();
    assert_eq!(
        ids,
        vec!["data.map", "data.merge", "data.template", "data.transform"]
    );
    assert!(catalog.iter().all(|m| m.input_schema.is_some()));
}
