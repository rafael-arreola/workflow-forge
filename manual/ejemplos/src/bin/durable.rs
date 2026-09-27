use serde_json::json;
use std::sync::Arc;
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Uso: durable RUTA_SQLITE CLAVE")?;
    let key = std::env::args()
        .nth(2)
        .ok_or("Falta la clave de recepción")?;
    let store = Arc::new(modules::SqliteExecutionStore::open(
        path,
        modules::SqliteOptions::default(),
    )?);
    let builder = WorkflowBuilder::standard()
        .execution_store(store.clone())
        .artifact_store(store);
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let outcome = async {
        let plan = app
            .prepare_json(
                access.clone(),
                include_bytes!("../../workflows/normalizar.json"),
            )
            .await?;
        let mut request = StartRunRequest::new(plan, json!("  Rafael  "));
        request.options.require_durable = true;
        request.options.receipt_key = Some(key);
        let receipt = app.start(access.clone(), request).await?;
        app.wait(access.clone(), receipt.run_id.clone()).await?;
        let output = app.result(access, receipt.run_id.clone()).await?;
        assert!(receipt.durable);
        assert_eq!(output, json!("Rafael"));
        Ok::<_, ForgeError>(json!({"receipt":receipt,"output":output}))
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("El cierre dejó trabajo pendiente".into());
    }
    println!("{}", outcome?);
    Ok(())
}
