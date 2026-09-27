use serde_json::json;
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(modulo_saludos::operaciones())?;
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let outcome = async {
        let plan = app
            .prepare_json(
                access.clone(),
                include_bytes!("../../workflows/saludo.json"),
            )
            .await?;
        let receipt = app
            .start(
                access.clone(),
                StartRunRequest::new(plan, json!({"nombre":"  Rafael  "})),
            )
            .await?;
        app.wait(access.clone(), receipt.run_id.clone()).await?;
        app.result(access, receipt.run_id).await
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("El cierre dejó trabajo pendiente".into());
    }
    let output = outcome?;
    assert_eq!(output, json!({"saludo":"Hola, Rafael"}));
    println!("{output}");
    Ok(())
}
