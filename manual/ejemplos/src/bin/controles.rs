use serde_json::{Value, json};
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = WorkflowBuilder::standard();
    let child: WorkflowDefinition =
        serde_json::from_slice(include_bytes!("../../workflows/normalizar.json"))?;
    builder.register_workflow(child)?;
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let cases: Vec<(&[u8], Value, Value)> = vec![
        (
            include_bytes!("../../workflows/lote.json"),
            json!([" A ", " B "]),
            json!([{"index":0,"status":"succeeded","output":"A"},{"index":1,"status":"succeeded","output":"B"}]),
        ),
        (
            include_bytes!("../../workflows/decision.json"),
            json!({"aprobado":true}),
            json!({"selected":"si","output":"continuar"}),
        ),
        (
            include_bytes!("../../workflows/decision.json"),
            json!({"aprobado":false}),
            json!({"selected":"no","output":"revisar"}),
        ),
        (
            include_bytes!("../../workflows/paralelo.json"),
            json!({}),
            json!({"izquierda":{"status":"succeeded","output":"A"},"derecha":{"status":"succeeded","output":"B"}}),
        ),
        (
            include_bytes!("../../workflows/bucle.json"),
            json!({"continuar":true,"valor":"inicial"}),
            json!({"continuar":false,"valor":"terminado"}),
        ),
        (
            include_bytes!("../../workflows/subworkflow.json"),
            json!("  reutilizable  "),
            json!("reutilizable"),
        ),
        (
            include_bytes!("../../workflows/timer.json"),
            json!({"ok":true}),
            json!({"ok":true}),
        ),
    ];
    let outcome = async {
        for (definition, input, expected) in cases {
            let plan = app.prepare_json(access.clone(), definition).await?;
            let output = app
                .execute(
                    access.clone(),
                    StartRunRequest::new(plan, input),
                    CancellationToken::new(),
                )
                .await?;
            if output != expected {
                return Err(ForgeError::new(
                    "manual.unexpected",
                    "Result differs from the documented output",
                ));
            }
            println!("{output}");
        }
        Ok::<_, ForgeError>(())
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("Shutdown left pending work".into());
    }
    outcome?;
    Ok(())
}
