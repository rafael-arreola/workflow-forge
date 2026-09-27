//! Local batch: foreach → subworkflow → custom operation, with per-item failures.
use serde_json::json;
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(modulo_saludos::operaciones())?;
    let child: WorkflowDefinition =
        serde_json::from_slice(include_bytes!("../../workflows/saludo.json"))?;
    builder.register_workflow(child)?;
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let outcome = async {
        let plan = app
            .prepare_json(
                access.clone(),
                include_bytes!("../../workflows/importacion.json"),
            )
            .await?;
        for input in [
            json!([]),
            json!([{"nombre":"  Ada  "},{"nombre":42},{"nombre":"  Rafael  "}]),
        ] {
            let expected_len = input.as_array().expect("array input").len();
            let output = app
                .execute(
                    access.clone(),
                    StartRunRequest::new(plan.clone(), input),
                    CancellationToken::new(),
                )
                .await?;
            assert_eq!(output.as_array().expect("array output").len(), expected_len);
            if expected_len == 0 {
                assert_eq!(output, json!([]));
            } else {
                assert_eq!(output[0]["output"], json!({"saludo":"Hola, Ada"}));
                assert_eq!(output[1]["status"], json!("failed"));
                assert_eq!(output[2]["output"], json!({"saludo":"Hola, Rafael"}));
            }
            println!("{output}");
        }
        Ok::<_, ForgeError>(())
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("Incomplete shutdown".into());
    }
    outcome?;
    Ok(())
}
