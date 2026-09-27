//! Offline responses demonstrate workflow-owned routing; no external integration.
use serde_json::json;
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime =
        EngineRuntime::boot(WorkflowBuilder::standard().build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let result = async {
        let plan = app
            .prepare_json(
                access.clone(),
                include_bytes!("../../workflows/respuestas.json"),
            )
            .await?;
        for (input, expected) in [
            (
                json!({"status":200,"body":{"nombre":"Ada"}}),
                json!({"resultado":"encontrado"}),
            ),
            (
                json!({"status":404,"body":null}),
                json!({"resultado":"ausente"}),
            ),
            (
                json!({"status":429,"body":null}),
                json!({"resultado":"posponer"}),
            ),
            (
                json!({"status":500,"body":null}),
                json!({"resultado":"revisar_status"}),
            ),
            (
                json!({"body":null}),
                json!({"resultado":"entrada_incompleta"}),
            ),
        ] {
            let output = app
                .execute(
                    access.clone(),
                    StartRunRequest::new(plan.clone(), input),
                    CancellationToken::new(),
                )
                .await?;
            assert_eq!(output, expected);
            println!("{output}");
        }
        // Variante local: sin ruta por defecto, un status distinto produce control.no_match.
        let mut no_route: serde_json::Value =
            serde_json::from_slice(include_bytes!("../../workflows/respuestas.json"))
                .expect("fixture válido");
        no_route["revision"] = json!("sin-ruta");
        no_route["nodes"][0]["body"]["nodes"][3]
            .as_object_mut()
            .expect("decision")
            .remove("fallback");
        let bytes = serde_json::to_vec(&no_route).expect("fixture serializable");
        let alternate = app.prepare_json(access.clone(), &bytes).await?;
        let output = app
            .execute(
                access.clone(),
                StartRunRequest::new(alternate, json!({"status":500,"body":null})),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(output, json!({"resultado":"error_no_previsto"}));
        println!("{output}");
        Ok::<_, ForgeError>(())
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("Cierre incompleto".into());
    }
    result?;
    Ok(())
}
