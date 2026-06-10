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

/// El patrón "request construido como dato" del ejemplo 6 de EXAMPLES.md:
/// ramas opcionales construyen el request HTTP completo y un único nodo
/// `submit` SIN input lo ejecuta recibiéndolo como token.
#[tokio::test]
async fn ramas_opcionales_convergen_en_un_submit_sin_input() {
    use httpmock::prelude::*;

    let server = MockServer::start_async().await;
    let lookup = server
        .mock_async(|when, then| {
            when.method(GET).path("/customers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({ "id": "c-9", "segment": "enterprise" }));
        })
        .await;
    let terms = server
        .mock_async(|when, then| {
            when.method(GET).path("/terms");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({ "net_days": 60 }));
        })
        .await;
    let billing = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/invoices")
                .json_body(serde_json::json!({ "customer_id": "c-9", "payment_terms_days": 60 }));
            then.status(201)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({ "id": "inv-42" }));
        })
        .await;

    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "quote-to-invoice-mini", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "lookup", "kind": "task", "task": "http.request",
              "input": { "url": format!("{}/customers", server.base_url()) } },
            { "id": "segment", "kind": "gateway", "gateway": "exclusive", "branches": [
                { "when": { "path": "$.nodes.lookup.output.body.segment", "eq": "enterprise" },
                  "edge": "ent" },
                { "else": true, "edge": "std" }
            ]},
            { "id": "fetch_terms", "kind": "task", "task": "http.request",
              "input": { "url": format!("{}/terms", server.base_url()) } },
            { "id": "build_ent", "kind": "task", "task": "data.transform",
              "input": {
                  "source": {
                      "customer": "$.nodes.lookup.output.body",
                      "terms": "$.nodes.fetch_terms.output.body"
                  },
                  "shape": {
                      "url": format!("{}/invoices", server.base_url()),
                      "method": "POST",
                      "body": { "customer_id": "@.customer.id",
                                "payment_terms_days": "@.terms.net_days" }
                  }
              } },
            { "id": "build_std", "kind": "task", "task": "data.transform",
              "input": {
                  "source": { "customer": "$.nodes.lookup.output.body" },
                  "shape": {
                      "url": format!("{}/invoices", server.base_url()),
                      "method": "POST",
                      "body": { "customer_id": "@.customer.id", "payment_terms_days": 30 }
                  }
              } },
            { "id": "submit", "kind": "task", "task": "http.request" },
            { "id": "end", "kind": "end",
              "output": { "invoice_id": "$.nodes.submit.output.body.id" } }
        ],
        "edges": [
            { "from": "start", "to": "lookup" },
            { "from": "lookup", "to": "segment" },
            { "from": "segment", "label": "ent", "to": "fetch_terms" },
            { "from": "fetch_terms", "to": "build_ent" },
            { "from": "segment", "label": "std", "to": "build_std" },
            { "from": "build_ent", "to": "submit" },
            { "from": "build_std", "to": "submit" },
            { "from": "submit", "to": "end" }
        ]
    }))
    .unwrap();

    let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry()).unwrap();
    let result = executor.run(WorkflowData(json!({}))).await.unwrap();

    assert_eq!(result.0, json!({ "invoice_id": "inv-42" }));
    lookup.assert_async().await;
    terms.assert_async().await; // la rama enterprise SÍ corrió
    billing.assert_async().await; // y el submit envió el body construido
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

/// El caso de uso que motivó los perfiles: el JSON de un cliente llega en un
/// formato incompatible, un nodo `data.transform` lo reconvierte campo a campo
/// al schema del perfil, y el perfil (un `http.request` preconfigurado con
/// URL/auth/método horneados y schemas propios) ejecuta la integración.
#[tokio::test]
async fn integracion_cliente_via_perfil_http_preconfigurado() {
    use httpmock::prelude::*;
    use std::collections::HashMap;
    use workflow_forge::core::secret::SecretProvider;

    struct MapSecrets(HashMap<String, String>);
    impl SecretProvider for MapSecrets {
        fn get(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
    }

    let server = MockServer::start_async().await;
    let crear_envio = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/shipments")
                .header("authorization", "Bearer tok-secreto")
                .json_body(json!({ "sku": "ABC-1", "qty": 3, "customer_id": "c-77" }));
            then.status(201)
                .header("content-type", "application/json")
                .json_body(json!({ "tracking_id": "trk-001", "eta_days": 2 }));
        })
        .await;

    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "alta-de-envio", "version": "0.1.0",
        "tasks": [{
            "id": "miapi.crear_envio",
            "extends": "http.request",
            "description": "Crea un envío en mi API logística",
            "input_schema": {
                "type": "object",
                "required": ["sku", "qty", "customer_id"],
                "properties": {
                    "sku": { "type": "string" },
                    "qty": { "type": "integer", "minimum": 1 },
                    "customer_id": { "type": "string" }
                }
            },
            "output_schema": { "type": "object", "required": ["tracking_id"] },
            "bind": {
                "url": format!("{}/shipments", server.base_url()),
                "method": "POST",
                "auth": { "type": "bearer", "token": { "$secret": "MIAPI_TOKEN" } },
                "fail_on_error_status": true,
                "body": "@"
            },
            "output": "@.body"
        }],
        "nodes": [
            { "id": "start", "kind": "start" },
            // El parser: del formato del cliente al schema del perfil
            { "id": "adaptar", "kind": "task", "task": "data.transform",
              "input": {
                  "source": "$.trigger",
                  "shape": {
                      "sku": "@.producto.codigo",
                      "qty": "@.producto.unidades",
                      "customer_id": "@.cliente_ref"
                  }
              }},
            { "id": "crear", "kind": "task", "task": "miapi.crear_envio",
              "retry": { "max": 2, "initial_ms": 10 } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "adaptar" },
            { "from": "adaptar", "to": "crear" },
            { "from": "crear", "to": "end" }
        ]
    }))
    .unwrap();

    let secrets = MapSecrets(HashMap::from([(
        "MIAPI_TOKEN".to_string(),
        "tok-secreto".to_string(),
    )]));
    let executor =
        WorkflowExecutor::new_with_secrets(workflow, workflow_forge::default_registry(), &secrets)
            .map_err(|errors| format!("{errors:?}"))
            .unwrap();

    // El JSON del cliente, en SU formato, no en el del perfil
    let result = executor
        .run(WorkflowData(json!({
            "cliente_ref": "c-77",
            "producto": { "codigo": "ABC-1", "unidades": 3 }
        })))
        .await
        .unwrap();

    crear_envio.assert_async().await;
    assert_eq!(result.0, json!({ "tracking_id": "trk-001", "eta_days": 2 }));
}

/// Integración por lotes: el cliente manda N filas en su formato, `data.map`
/// las adapta, `foreach` invoca el perfil http por cada una con `collect`
/// (una fila rota no aborta el lote) y el observer entrega el reporte.
#[tokio::test]
async fn lote_de_cliente_via_foreach_y_perfil_con_reporte() {
    use httpmock::prelude::*;
    use std::sync::Arc;

    let server = MockServer::start_async().await;
    let alta = server
        .mock_async(|when, then| {
            when.method(POST).path("/shipments");
            then.status(201)
                .header("content-type", "application/json")
                .json_body(json!({ "tracking_id": "trk" }));
        })
        .await;

    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "alta-de-envios-lote", "version": "0.1.0",
        "tasks": [{
            "id": "miapi.crear_envio",
            "extends": "http.request",
            "input_schema": {
                "type": "object",
                "required": ["sku", "qty"],
                "properties": {
                    "sku": { "type": "string" },
                    "qty": { "type": "integer", "minimum": 1 }
                }
            },
            "bind": {
                "url": format!("{}/shipments", server.base_url()),
                "method": "POST",
                "fail_on_error_status": true,
                "body": "@"
            },
            "output": "@.body"
        }],
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "adaptar", "kind": "task", "task": "data.map",
              "input": {
                  "items": "$.trigger.filas",
                  "shape": { "sku": "@.codigo", "qty": "@.unidades" }
              }},
            { "id": "lote", "kind": "foreach",
              "task": "miapi.crear_envio",
              "items": "$.nodes.adaptar.output",
              "concurrency": 2,
              "on_item_error": "collect" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "adaptar" },
            { "from": "adaptar", "to": "lote" },
            { "from": "lote", "to": "end" }
        ]
    }))
    .unwrap();

    let history = Arc::new(InMemoryHistory::new());
    let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry())
        .map_err(|errors| format!("{errors:?}"))
        .unwrap()
        .with_observer(Arc::clone(&history) as Arc<dyn ExecutionObserver>);

    // Tres filas del cliente; la segunda viene rota (qty 0 viola el schema)
    let result = executor
        .run(WorkflowData(json!({ "filas": [
            { "codigo": "A-1", "unidades": 2 },
            { "codigo": "B-2", "unidades": 0 },
            { "codigo": "C-3", "unidades": 5 }
        ]})))
        .await
        .unwrap();

    // Solo las 2 filas válidas llegaron a la API; la rota quedó en failed
    assert_eq!(alta.calls_async().await, 2);
    assert_eq!(
        result.0["ok"],
        json!([{ "tracking_id": "trk" }, { "tracking_id": "trk" }])
    );
    let failed = result.0["failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["index"], json!(1));
    assert_eq!(failed[0]["error"]["code"], json!("TASK_INPUT_INVALID"));

    // El reporte responde "¿qué pasó con el lote?"
    let report = history.report();
    let node = report.nodes.iter().find(|n| n.node_id == "lote").unwrap();
    assert_eq!(node.kind, "foreach");
    assert_eq!(node.items_ok, Some(2));
    assert_eq!(node.items_failed, Some(1));
}
