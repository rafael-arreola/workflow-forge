//! Exclusive gateway: routes based on a trigger field, with two
//! terminal outputs. Uses only `util`/`data` tasks — no network.
//!
//! Run with:
//!     cargo run -p workflow-forge --example routing
//!     cargo run -p workflow-forge --example routing -- 1500

use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Optional CLI arg: the order amount (default 250).
    let amount: i64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(250);

    let workflow: WorkflowDefinition = serde_json::from_str(
        r#"{
        "spec": "1.0",
        "name": "route-order",
        "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            {
                "id": "decide", "kind": "gateway", "gateway": "exclusive",
                "branches": [
                    { "when": { "path": "$.trigger.amount", "gte": 1000 }, "edge": "high" },
                    { "else": true, "edge": "normal" }
                ]
            },
            { "id": "escalate", "kind": "task", "task": "util.log",
              "input": { "level": "warn", "message": "high-value order",
                         "value": { "queue": "manual-review", "amount": "$.trigger.amount" } } },
            { "id": "auto", "kind": "task", "task": "util.log",
              "input": { "message": "auto-approved",
                         "value": { "queue": "auto", "amount": "$.trigger.amount" } } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start",    "to": "decide" },
            { "from": "decide",   "label": "high",   "to": "escalate" },
            { "from": "decide",   "label": "normal", "to": "auto" },
            { "from": "escalate", "to": "end" },
            { "from": "auto",     "to": "end" }
        ]
    }"#,
    )?;

    let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry())
        .map_err(|errors| format!("invalid: {errors:?}"))?;

    let result = executor
        .run(WorkflowData(serde_json::json!({ "amount": amount })))
        .await?;

    println!("routed to: {}", result.0);
    Ok(())
}
