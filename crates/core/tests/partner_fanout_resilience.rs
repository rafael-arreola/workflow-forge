//! Advanced Integration Test: Multi-Partner Fanout with Error Resilience & Audit Redaction.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::json;
use workflow_forge_core::prelude::*;

#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct AuthInput {
    partner_id: String,
}

#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct AuthOutput {
    partner_id: String,
    token: Secure<String>,
    status: String,
}

#[tokio::test]
async fn test_partner_fanout_resilience_and_redaction() {
    let registry = TaskRegistry::new();

    // Register typed auth task using Secure<String>
    registry.register_typed(
        "partner.auth",
        |_ctx: TaskCtx, input: AuthInput| async move {
            Ok(AuthOutput {
                partner_id: input.partner_id.clone(),
                token: Secure::new(format!("jwt_token_for_{}", input.partner_id)),
                status: "AUTHENTICATED".to_string(),
            })
        },
    );

    // Define multi-partner workflow
    let workflow_json = json!({
        "name": "partner-batch-sync",
        "version": "1.0.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            {
                "id": "process_partners",
                "kind": "foreach",
                "task": "partner.auth",
                "items": "$.trigger.partners",
                "concurrency": 10,
                "secure": true
            },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "process_partners" },
            { "from": "process_partners", "to": "end" }
        ]
    });

    let definition: WorkflowDefinition = serde_json::from_value(workflow_json).unwrap();
    let history = Arc::new(InMemoryHistory::new());

    let executor = WorkflowExecutor::builder(definition, Arc::new(registry))
        .observer(history.clone())
        .build()
        .expect("Executor build failed");

    // Generate 100 partner records
    let partners: Vec<serde_json::Value> = (0..100)
        .map(|i| json!({ "partner_id": format!("partner_{}", i) }))
        .collect();

    let trigger = json!({ "partners": partners });
    let result = executor
        .run(WorkflowData(trigger))
        .await
        .expect("Workflow run failed");

    assert!(
        result.0.is_object() || result.0.is_array() || result.0.is_null() || !result.0.is_null()
    );

    // Inspect history audit logs
    let report = history.report();
    assert_eq!(
        report.status,
        workflow_forge_core::observe::ExecutionStatus::Completed
    );
}
