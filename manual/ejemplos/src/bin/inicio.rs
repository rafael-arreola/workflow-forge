use serde_json::json;
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime =
        EngineRuntime::boot(WorkflowBuilder::standard().build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");

    let outcome = async {
        let plan = app
            .prepare_json(
                access.clone(),
                include_bytes!("../../workflows/normalizar.json"),
            )
            .await?;
        let receipt = app
            .start(
                access.clone(),
                StartRunRequest::new(plan, json!("  Rafael  ")),
            )
            .await?;
        let run = app.wait(access.clone(), receipt.run_id.clone()).await?;
        if run.state != RunState::Succeeded {
            return Err(run.error.unwrap_or_else(|| {
                ForgeError::new("manual.not_succeeded", "La ejecución no terminó con éxito")
            }));
        }
        app.result(access, receipt.run_id).await
    }
    .await;

    // Apagar también cuando prepare/start/wait devuelve un error.
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("El cierre dejó trabajo pendiente".into());
    }
    let output = outcome?;
    assert_eq!(output, json!("Rafael"));
    println!("{output}");
    Ok(())
}
