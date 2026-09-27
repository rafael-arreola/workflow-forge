//! A durable C-01A host. Reusing the receipt key recovers the same accepted run
//! inside its published deduplication window, including across process restarts.
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use workflow_forge::v2::*;
use workflow_forge_reference_module::{StaticDirectory, customer_operations};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Usage: v2_sqlite DATABASE_PATH [RECEIPT_KEY]")?;
    let key = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "demo-customer-1".into());
    let store = Arc::new(modules::SqliteExecutionStore::open(
        path,
        modules::SqliteOptions::default(),
    )?);
    let mut builder = WorkflowBuilder::standard().execution_store(store);
    builder.register_bundle(customer_operations(Arc::new(StaticDirectory(
        BTreeMap::from([("C-9".into(), true)]),
    ))))?;
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let result = async {
        let plan = app
            .prepare_json(
                access.clone(),
                include_bytes!("../../../examples/workflows/customer_lookup.v2.json"),
            )
            .await?;
        let mut request = StartRunRequest::new(
            plan,
            json!({"request_id":"req-17","customer":" C-9 ","items":[{"sku":"A-1","quantity":2}]}),
        );
        request.options.require_durable = true;
        request.options.receipt_key = Some(key);
        let receipt = app.start(access.clone(), request).await?;
        let run = app.wait(access.clone(), receipt.run_id.clone()).await?;
        let output = app.result(access, receipt.run_id.clone()).await?;
        println!(
            "{}",
            json!({"receipt":receipt,"state":run.state,"output":output})
        );
        Ok::<_, ForgeError>(())
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
    result?;
    shutdown?;
    Ok(())
}
