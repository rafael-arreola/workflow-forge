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
                include_bytes!("../../../examples/workflows/response_routing.v2.json"),
            )
            .await?;
        for (input, expected) in [
            (
                json!({"status":404,"body":{"reason":"missing"}}),
                json!({"found":false}),
            ),
            (
                json!({"status":200,"body":{"name":"Ada"}}),
                json!({"status":200,"body":{"name":"Ada"}}),
            ),
            (
                json!({"unexpected":true}),
                json!({"error":"unexpected_response"}),
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
        Ok::<_, ForgeError>(())
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
    result?;
    shutdown?;
    Ok(())
}
