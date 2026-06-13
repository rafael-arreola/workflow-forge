//! Escribe tus propias tareas y recórrelas con `foreach`.
//!
//! Registra una tarea tipada (schemas derivados de los tipos Rust) y una
//! tarea por closure cruda sobre el registry por defecto, y luego cotiza un
//! lote de órdenes.
//!
//! Ejecutar con:
//!     cargo run -p workflow-forge --example custom_tasks

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use workflow_forge::prelude::*;

#[derive(Deserialize, JsonSchema)]
struct OrderIn {
    sku: String,
    qty: u32,
}

#[derive(Serialize, JsonSchema)]
struct PricedOut {
    sku: String,
    total: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = workflow_forge::default_registry();

    // Nivel 1: tarea tipada — los JSON Schema de input/output se derivan de
    // los tipos Rust y el engine los valida en cada llamada.
    registry.register_typed("demo.price", |_ctx, order: OrderIn| async move {
        Ok(PricedOut {
            sku: order.sku,
            total: order.qty * 10,
        })
    });

    // Nivel 2: tarea por closure cruda — sin schema, JSON entra / JSON sale.
    registry.register_fn("demo.stamp", |_ctx, input| async move { Ok(input) });

    let workflow: WorkflowDefinition = serde_json::from_str(
        r#"{
        "spec": "1.0",
        "name": "price-batch",
        "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "price", "kind": "foreach",
              "items": "$.trigger.orders", "task": "demo.price", "concurrency": 4 },
            { "id": "stamp", "kind": "task", "task": "demo.stamp",
              "input": { "priced": "$.nodes.price.output" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "price" },
            { "from": "price", "to": "stamp" },
            { "from": "stamp", "to": "end" }
        ]
    }"#,
    )?;

    let executor = WorkflowExecutor::new(workflow, registry)
        .map_err(|errors| format!("invalid: {errors:?}"))?;

    let trigger = serde_json::json!({
        "orders": [
            { "sku": "A", "qty": 2 },
            { "sku": "B", "qty": 5 },
            { "sku": "C", "qty": 1 }
        ]
    });

    let result = executor.run(WorkflowData(trigger)).await?;
    println!("{}", serde_json::to_string_pretty(&result.0)?);
    Ok(())
}
