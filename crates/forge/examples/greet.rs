//! Quickstart: renderiza un string desde el trigger con `data.template`.
//!
//! Ejecutar con:
//!     cargo run -p workflow-forge --example greet

use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workflow: WorkflowDefinition = serde_json::from_str(
        r#"{
        "spec": "1.0",
        "name": "greet",
        "version": "0.1.0",
        "nodes": [
            { "id": "start",  "kind": "start" },
            { "id": "render", "kind": "task", "task": "data.template",
              "input": { "template": "Hello {who}!", "values": "$.trigger" } },
            { "id": "end",    "kind": "end" }
        ],
        "edges": [
            { "from": "start",  "to": "render" },
            { "from": "render", "to": "end" }
        ]
    }"#,
    )?;

    let registry = workflow_forge::default_registry();
    let executor = WorkflowExecutor::new(workflow, registry)
        .map_err(|errors| format!("invalid: {errors:?}"))?;

    let result = executor
        .run(WorkflowData(serde_json::json!({ "who": "world" })))
        .await?;

    println!("{}", result.0); // "Hello world!"
    Ok(())
}
