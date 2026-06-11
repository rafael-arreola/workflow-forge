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

#[tokio::test]
async fn cast_normaliza_filas_de_un_lote() {
    // El caso CSV del cliente: fechas dd/mm/yyyy, precios con coma decimal,
    // SKUs sucios y columnas opcionales — todo a formato canónico.
    let workflow = json!({
        "name": "normaliza", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "normaliza", "kind": "task", "task": "data.cast",
              "input": {
                  "source": "$.trigger.rows",
                  "fields": {
                      "fecha": [{ "op": "date", "from": "%d/%m/%Y" }],
                      "precio": [{ "op": "number", "decimal": ",", "thousands": "." }],
                      "sku": [{ "op": "trim" }, { "op": "upper" }],
                      "notas": [{ "op": "default", "value": "" }]
                  }
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "normaliza" },
            { "from": "normaliza", "to": "end" }
        ]
    });

    let result = run(
        workflow,
        json!({ "rows": [
            { "fecha": "10/06/2026", "precio": "1.234,56", "sku": "  abc-1 ", "notas": "ok" },
            { "fecha": "01/01/2026", "precio": "10", "sku": "xyz-9" }
        ]}),
    )
    .await;

    assert_eq!(
        result.0,
        json!([
            { "fecha": "2026-06-10", "precio": 1234.56, "sku": "ABC-1", "notas": "ok" },
            { "fecha": "2026-01-01", "precio": 10, "sku": "XYZ-9", "notas": "" }
        ])
    );
}

#[tokio::test]
async fn cast_collect_separa_filas_buenas_y_malas() {
    let workflow = json!({
        "name": "normaliza-collect", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "normaliza", "kind": "task", "task": "data.cast",
              "input": {
                  "source": "$.trigger.rows",
                  "fields": { "fecha": [{ "op": "date", "from": "%d/%m/%Y" }] },
                  "on_invalid": "collect"
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "normaliza" },
            { "from": "normaliza", "to": "end" }
        ]
    });

    let result = run(
        workflow,
        json!({ "rows": [
            { "fecha": "10/06/2026" },
            { "fecha": "no es fecha" },
            { "fecha": "01/01/2026" }
        ]}),
    )
    .await;

    assert_eq!(
        result.0["ok"],
        json!([{ "fecha": "2026-06-10" }, { "fecha": "2026-01-01" }])
    );
    let failed = result.0["failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["index"], json!(1));
    assert_eq!(failed[0]["item"], json!({ "fecha": "no es fecha" }));
    assert_eq!(failed[0]["errors"][0]["field"], json!("fecha"));
}

#[tokio::test]
async fn cast_fail_reporta_fila_y_campo() {
    let workflow = json!({
        "name": "normaliza-fail", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "normaliza", "kind": "task", "task": "data.cast",
              "input": {
                  "source": "$.trigger.rows",
                  "fields": { "fecha": [{ "op": "date", "from": "%d/%m/%Y" }] }
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "normaliza" },
            { "from": "normaliza", "to": "end" }
        ]
    });

    let workflow: WorkflowDefinition = serde_json::from_value(workflow).unwrap();
    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    let error = executor
        .run(WorkflowData(json!({ "rows": [
            { "fecha": "10/06/2026" },
            { "fecha": "abc" }
        ]})))
        .await
        .unwrap_err();

    assert_eq!(error.code, "CAST_FIELD_INVALID");
    assert!(
        error.message.contains("fila 1"),
        "mensaje: {}",
        error.message
    );
    assert!(
        error.message.contains("'fecha'"),
        "mensaje: {}",
        error.message
    );
}

#[tokio::test]
async fn cast_objeto_suelto_conserva_la_forma() {
    let workflow = json!({
        "name": "normaliza-objeto", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "normaliza", "kind": "task", "task": "data.cast",
              "input": {
                  "source": "$.trigger",
                  "fields": { "total": [{ "op": "number", "decimal": "," }] }
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "normaliza" },
            { "from": "normaliza", "to": "end" }
        ]
    });

    let result = run(workflow, json!({ "total": "99,9" })).await;
    assert_eq!(result.0, json!({ "total": 99.9 }));
}

#[test]
fn catalogo_con_schemas() {
    let catalog = registry().catalog();
    let ids: Vec<&str> = catalog.iter().map(|m| m.id.0.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "data.cast",
            "data.map",
            "data.merge",
            "data.template",
            "data.transform"
        ]
    );
    assert!(catalog.iter().all(|m| m.input_schema.is_some()));
}
