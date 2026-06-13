use serde_json::json;
use workflow_forge::prelude::*;

#[test]
fn default_registry_includes_enabled_extensions() {
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
        assert!(ids.contains(&expected), "missing {expected} in {ids:?}");
    }
    #[cfg(feature = "tabular")]
    for expected in ["tabular.parse", "tabular.write"] {
        assert!(ids.contains(&expected), "missing {expected} in {ids:?}");
    }
}

/// The "request built as data" pattern from example 6 of EXAMPLES.md:
/// optional branches build the full HTTP request and a single
/// `submit` node WITHOUT input executes it, receiving it as the token.
#[tokio::test]
async fn optional_branches_converge_in_a_submit_without_input() {
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
    terms.assert_async().await; // the enterprise branch DID run
    billing.assert_async().await; // and the submit sent the built body
}

#[tokio::test]
async fn workflow_across_extensions() {
    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "cross", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "greet", "kind": "task", "task": "data.template",
              "input": { "template": "Hello {who}", "values": "$.trigger" } },
            { "id": "audit", "kind": "task", "task": "util.log",
              "input": { "message": "$.nodes.greet.output", "value": "$.nodes.greet.output" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "greet" },
            { "from": "greet", "to": "audit" },
            { "from": "audit", "to": "end" }
        ]
    }))
    .unwrap();

    let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry()).unwrap();
    let result = executor
        .run(WorkflowData(json!({ "who": "world" })))
        .await
        .unwrap();
    assert_eq!(result.0, json!("Hello world"));
}

/// The use case that motivated profiles: a client's JSON arrives in an
/// incompatible format, a `data.transform` node remaps it field by field
/// to the profile's schema, and the profile (an `http.request` preconfigured
/// with baked-in URL/auth/method and its own schemas) executes the integration.
#[tokio::test]
async fn client_integration_via_preconfigured_http_profile() {
    use httpmock::prelude::*;
    use std::collections::HashMap;
    use workflow_forge::core::io::secret::SecretProvider;

    struct MapSecrets(HashMap<String, String>);
    impl SecretProvider for MapSecrets {
        fn get(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
    }

    let server = MockServer::start_async().await;
    let create_shipment = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/shipments")
                .header("authorization", "Bearer secret-tok")
                .json_body(json!({ "sku": "ABC-1", "qty": 3, "customer_id": "c-77" }));
            then.status(201)
                .header("content-type", "application/json")
                .json_body(json!({ "tracking_id": "trk-001", "eta_days": 2 }));
        })
        .await;

    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "create-shipment", "version": "0.1.0",
        "tasks": [{
            "id": "myapi.create_shipment",
            "extends": "http.request",
            "description": "Creates a shipment in my logistics API",
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
                "auth": { "type": "bearer", "token": { "$secret": "MYAPI_TOKEN" } },
                "fail_on_error_status": true,
                "body": "@"
            },
            "output": "@.body"
        }],
        "nodes": [
            { "id": "start", "kind": "start" },
            // The parser: from client format to profile schema
            { "id": "adapt", "kind": "task", "task": "data.transform",
              "input": {
                  "source": "$.trigger",
                  "shape": {
                      "sku": "@.product.code",
                      "qty": "@.product.units",
                      "customer_id": "@.client_ref"
                  }
              }},
            { "id": "create", "kind": "task", "task": "myapi.create_shipment",
              "retry": { "max": 2, "initial_ms": 10 } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "adapt" },
            { "from": "adapt", "to": "create" },
            { "from": "create", "to": "end" }
        ]
    }))
    .unwrap();

    let secrets = MapSecrets(HashMap::from([(
        "MYAPI_TOKEN".to_string(),
        "secret-tok".to_string(),
    )]));
    let executor =
        WorkflowExecutor::new_with_secrets(workflow, workflow_forge::default_registry(), &secrets)
            .map_err(|errors| format!("{errors:?}"))
            .unwrap();

    // The client's JSON, in THEIR format, not the profile's
    let result = executor
        .run(WorkflowData(json!({
            "client_ref": "c-77",
            "product": { "code": "ABC-1", "units": 3 }
        })))
        .await
        .unwrap();

    create_shipment.assert_async().await;
    assert_eq!(result.0, json!({ "tracking_id": "trk-001", "eta_days": 2 }));
}

/// Batch integration: the client sends N rows in their format, `data.map`
/// adapts them, `foreach` invokes the http profile for each one with `collect`
/// (one broken row does not abort the batch) and the observer delivers the report.
#[tokio::test]
async fn client_batch_via_foreach_and_profile_with_report() {
    use httpmock::prelude::*;
    use std::sync::Arc;

    let server = MockServer::start_async().await;
    let create = server
        .mock_async(|when, then| {
            when.method(POST).path("/shipments");
            then.status(201)
                .header("content-type", "application/json")
                .json_body(json!({ "tracking_id": "trk" }));
        })
        .await;

    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "batch-shipments", "version": "0.1.0",
        "tasks": [{
            "id": "myapi.create_shipment",
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
            { "id": "adapt", "kind": "task", "task": "data.map",
              "input": {
                  "items": "$.trigger.rows",
                  "shape": { "sku": "@.code", "qty": "@.units" }
              }},
            { "id": "batch", "kind": "foreach",
              "task": "myapi.create_shipment",
              "items": "$.nodes.adapt.output",
              "concurrency": 2,
              "on_item_error": "collect" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "adapt" },
            { "from": "adapt", "to": "batch" },
            { "from": "batch", "to": "end" }
        ]
    }))
    .unwrap();

    let history = Arc::new(InMemoryHistory::new());
    let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry())
        .map_err(|errors| format!("{errors:?}"))
        .unwrap()
        .with_observer(Arc::clone(&history) as Arc<dyn ExecutionObserver>);

    // Three client rows; the second one is broken (qty 0 violates the schema)
    let result = executor
        .run(WorkflowData(json!({ "rows": [
            { "code": "A-1", "units": 2 },
            { "code": "B-2", "units": 0 },
            { "code": "C-3", "units": 5 }
        ]})))
        .await
        .unwrap();

    // Only the 2 valid rows reached the API; the broken one ended up in failed
    assert_eq!(create.calls_async().await, 2);
    assert_eq!(
        result.0["ok"],
        json!([{ "tracking_id": "trk" }, { "tracking_id": "trk" }])
    );
    let failed = result.0["failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["index"], json!(1));
    assert_eq!(failed[0]["error"]["code"], json!("TASK_INPUT_INVALID"));

    // The report answers "what happened with the batch?"
    let report = history.report();
    let node = report.nodes.iter().find(|n| n.node_id == "batch").unwrap();
    assert_eq!(node.kind, "foreach");
    assert_eq!(node.items_ok, Some(2));
    assert_eq!(node.items_failed, Some(1));
}

/// Full file lifecycle without passing through the JSON context: download a
/// binary report from a client API (`response_body: blob`) and upload it as
/// multipart to another API, all via streaming through the execution's BlobStore.
#[tokio::test]
async fn download_report_and_upload_multipart() {
    use httpmock::prelude::*;

    let source = MockServer::start_async().await;
    let target = MockServer::start_async().await;

    let content = "date,sku,units\n2026-06-10,A-1,3\n2026-06-10,B-2,7\n";
    let download = source
        .mock_async(|when, then| {
            when.method(GET)
                .path("/reports/daily")
                .header("authorization", "Bearer client-tok");
            then.status(200)
                .header("content-type", "text/csv")
                .header(
                    "content-disposition",
                    "attachment; filename=\"daily-sales.csv\"",
                )
                .body(content);
        })
        .await;
    let upload = target
        .mock_async(|when, then| {
            when.method(POST)
                .path("/ingest")
                .header_includes("content-type", "multipart/form-data")
                .body_includes("name=\"source\"")
                .body_includes("acme-client")
                .body_includes("name=\"file\"")
                .body_includes("filename=\"daily-sales.csv\"")
                .body_includes(content);
            then.status(202)
                .header("content-type", "application/json")
                .json_body(json!({ "ingest_id": "ing-9" }));
        })
        .await;

    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "sync-daily-report", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "download", "kind": "task", "task": "http.request",
              "input": {
                  "url": format!("{}/reports/daily", source.base_url()),
                  "auth": { "type": "bearer", "token": "client-tok" },
                  "response_body": "blob",
                  "fail_on_error_status": true
              } },
            { "id": "upload", "kind": "task", "task": "http.request",
              "input": {
                  "url": format!("{}/ingest", target.base_url()),
                  "method": "POST",
                  "multipart": {
                      "source": "acme-client",
                      "file": {
                          "blob": "$.nodes.download.output.body",
                          "content_type": "text/csv"
                      }
                  },
                  "fail_on_error_status": true
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "download" },
            { "from": "download", "to": "upload" },
            { "from": "upload", "to": "end" }
        ]
    }))
    .unwrap();

    let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry()).unwrap();
    let result = executor.run(WorkflowData(json!({}))).await.unwrap();

    download.assert_async().await;
    upload.assert_async().await;
    assert_eq!(result.0["status"], 202);
    assert_eq!(result.0["body"], json!({ "ingest_id": "ing-9" }));
}

/// The reuse pattern across clients: a shared sub-workflow
/// ("normalize and send") registered once in the `WorkflowRegistry`,
/// invoked from each client's workflow with its own mapping.
/// Exercises `kind: subworkflow` + `data.cast` + http, end to end.
#[tokio::test]
async fn shared_subworkflow_normalizes_with_cast_and_sends() {
    use httpmock::prelude::*;
    use std::sync::Arc;

    let server = MockServer::start_async().await;
    let create_order = server
        .mock_async(|when, then| {
            when.method(POST).path("/orders").json_body(json!({
                "date": "2026-06-10",
                "total": 1234.56,
                "sku": "ABC-1"
            }));
            then.status(201)
                .header("content-type", "application/json")
                .json_body(json!({ "order_id": "ord-5" }));
        })
        .await;

    // The reusable block: normalizes dirty fields and does the POST
    let shared: WorkflowDefinition = serde_json::from_value(json!({
        "name": "normalize-and-send", "version": "1.0.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "normalize", "kind": "task", "task": "data.cast",
              "input": {
                  "source": "$.trigger",
                  "fields": {
                      "date": [{ "op": "date", "from": "%d/%m/%Y" }],
                      "total": [{ "op": "number", "decimal": ",", "thousands": "." }],
                      "sku": [{ "op": "trim" }, { "op": "upper" }]
                  }
              } },
            { "id": "send", "kind": "task", "task": "http.request",
              "input": {
                  "url": format!("{}/orders", server.base_url()),
                  "method": "POST",
                  "body": "$.nodes.normalize.output",
                  "fail_on_error_status": true
              } },
            { "id": "end", "kind": "end",
              "output": "$.nodes.send.output.body" }
        ],
        "edges": [
            { "from": "start", "to": "normalize" },
            { "from": "normalize", "to": "send" },
            { "from": "send", "to": "end" }
        ]
    }))
    .unwrap();
    let workflows = Arc::new(WorkflowRegistry::new());
    workflows.register(shared).unwrap();

    // The client's workflow: only adapts its format and delegates
    let client: WorkflowDefinition = serde_json::from_value(json!({
        "name": "acme-client", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "process", "kind": "subworkflow", "workflow": "normalize-and-send",
              "input": {
                  "date": "$.trigger.order_date",
                  "total": "$.trigger.amount",
                  "sku": "$.trigger.item"
              } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "process" },
            { "from": "process", "to": "end" }
        ]
    }))
    .unwrap();

    let executor = WorkflowExecutor::builder(client, workflow_forge::default_registry())
        .workflows(workflows)
        .build()
        .map_err(|errors| format!("{errors:?}"))
        .unwrap();

    // The client's payload, in their dirty format
    let result = executor
        .run(WorkflowData(json!({
            "order_date": "10/06/2026",
            "amount": "1,234.56",
            "item": "  abc-1 "
        })))
        .await
        .unwrap();

    create_order.assert_async().await;
    assert_eq!(result.0, json!({ "order_id": "ord-5" }));
}
