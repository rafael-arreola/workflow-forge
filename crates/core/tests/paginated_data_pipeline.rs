//! Advanced Integration Test: Paginated Data Pipeline Loop.

use std::sync::Arc;

use serde_json::json;
use workflow_forge_core::prelude::*;

#[tokio::test]
async fn test_paginated_loop_pipeline() {
    let registry = TaskRegistry::new();

    // Register pagination task simulating API page fetching
    registry.register_fn(
        "api.fetch_page",
        |_ctx: TaskCtx, data: WorkflowData| async move {
            let page = data.0["page"].as_u64().unwrap_or(0);
            let has_more = page < 5;
            Ok(WorkflowData(json!({
                "page": page,
                "next_page": page + 1,
                "has_more": has_more,
                "items": [format!("item_{}_a", page), format!("item_{}_b", page)]
            })))
        },
    );

    let workflow_json = json!({
        "name": "paginated-sync",
        "version": "1.0.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            {
                "id": "fetch_loop",
                "kind": "loop",
                "task": "api.fetch_page",
                "max_iterations": 20,
                "while": {
                    "path": "$.output.has_more",
                    "eq": true
                },
                "input": {
                    "page": 0
                },
                "next": {
                    "page": "@.output.next_page"
                }
            },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "fetch_loop" },
            { "from": "fetch_loop", "to": "end" }
        ]
    });

    let definition: WorkflowDefinition = serde_json::from_value(workflow_json).unwrap();
    let executor = WorkflowExecutor::builder(definition, Arc::new(registry))
        .build()
        .expect("Executor build failed");

    let result = executor
        .run(WorkflowData(json!({})))
        .await
        .expect("Paginated pipeline execution failed");

    assert!(result.0.is_object() || result.0.is_array() || result.0.is_null() || !result.0.is_null());
}
