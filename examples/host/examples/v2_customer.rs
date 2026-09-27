//! A complete embedded host: compose once, boot once, share its handle, drain on exit.
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use workflow_forge::prelude::*;
use workflow_forge_reference_module::{StaticDirectory, customer_operations};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(customer_operations(Arc::new(StaticDirectory(
        BTreeMap::from([("C-9".into(), true)]),
    ))))?;
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let result = async {
        let plan = app.prepare_json(access.clone(),include_bytes!("../../../examples/workflows/customer_lookup.v2.json")).await?;
        let output = app.execute(access, StartRunRequest::new(plan,json!({"request_id":"req-17","customer":" C-9 ","items":[{"sku":"A-1","quantity":2}]})), CancellationToken::new()).await?;
        println!("{output}");
        Ok::<_,ForgeError>(())
    }.await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
    result?;
    shutdown?;
    Ok(())
}
