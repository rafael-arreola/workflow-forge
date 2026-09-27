use serde_json::json;
use std::time::Duration;
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
                include_bytes!("../../workflows/aprobacion.json"),
            )
            .await?;
        let receipt = app
            .start(
                access.clone(),
                StartRunRequest::new(plan, json!({"pedido":"P-42"})),
            )
            .await?;
        let wait_id = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let run = app.status(access.clone(), receipt.run_id.clone()).await?;
                if let Some(wait) = run.waits.values().next() {
                    return Ok::<_, ForgeError>(wait.id.clone());
                }
                if run.state.is_terminal() || run.state == RunState::Blocked {
                    return Err(ForgeError::new("manual.wait", "No se creó la reserva"));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| ForgeError::new("manual.timeout", "La reserva no estuvo lista"))??;
        // En una aplicación real, estos datos llegan por un callback/cola autenticado.
        app.signal(
            access.clone(),
            SignalCommand {
                run_id: receipt.run_id.clone(),
                wait_id,
                message_id: "aprobacion-P-42".into(),
                correlation: "P-42".into(),
                payload: json!({"aprobado":true}),
                artifacts: vec![],
            },
        )
        .await?;
        app.wait(access.clone(), receipt.run_id.clone()).await?;
        let output = app.result(access, receipt.run_id).await?;
        assert_eq!(output, json!({"start":null,"signal":{"aprobado":true}}));
        Ok::<_, ForgeError>(output)
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("El cierre dejó trabajo pendiente".into());
    }
    println!("{}", outcome?);
    Ok(())
}
