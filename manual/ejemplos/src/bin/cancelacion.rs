//! Cancellation and time budgets owned by the application.
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
        let mut definition: serde_json::Value =
            serde_json::from_slice(include_bytes!("../../workflows/timer.json"))
                .expect("valid JSON fixture");
        definition["nodes"][0]["duration_ms"] = json!(5_000);
        let bytes = serde_json::to_vec(&definition).expect("serializable fixture");
        let plan = app.prepare_json(access.clone(), &bytes).await?;
        let cancel = CancellationToken::new();
        let call = app.execute(
            access.clone(),
            StartRunRequest::new(plan.clone(), json!({})),
            cancel.clone(),
        );
        let stop = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            cancel.cancel(); // In your application: disconnect, shutdown or user cancellation.
        };
        let (cancelled, ()) = tokio::join!(call, stop);
        let cancelled = cancelled.expect_err("the call must be cancelled");
        assert_eq!(cancelled.diagnostics[0].code, "operation.cancelled");
        println!("Cancellation: {cancelled}");

        let mut request = StartRunRequest::new(plan, json!({}));
        request.options.timeout_ms = Some(20);
        let expired = app.execute(access, request, CancellationToken::new()).await;
        let expired = expired.expect_err("the call must reach its deadline");
        assert_eq!(expired.diagnostics[0].code, "operation.timeout");
        println!("Deadline: {expired}");
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
