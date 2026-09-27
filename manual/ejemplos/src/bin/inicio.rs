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
        app.execute(
            access,
            StartRunRequest::new(plan, json!("  Rafael  ")),
            CancellationToken::new(),
        )
        .await
    }
    .await;

    // Shut down even when prepare/execute returns an error.
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("Shutdown left pending work".into());
    }
    let output = outcome?;
    assert_eq!(output, json!("Rafael"));
    println!("{output}");
    Ok(())
}
