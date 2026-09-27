//! Start and stop one host with an outstanding reservation, then use another
//! process to deliver its signal. The host supplies authentication/transport.
use serde_json::json;
use std::{sync::Arc, time::Duration};
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Usage: v2_signal DATABASE_PATH start|signal [RECEIPT_KEY]")?;
    let mode = std::env::args().nth(2).ok_or("Expected start or signal")?;
    if !matches!(mode.as_str(), "start" | "signal") {
        return Err("Expected start or signal".into());
    }
    let key = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "approval-demo-1".into());
    let store = Arc::new(modules::SqliteExecutionStore::open(
        path,
        modules::SqliteOptions::default(),
    )?);
    let runtime = EngineRuntime::boot(
        WorkflowBuilder::standard().execution_store(store).build()?,
        BootOptions {
            recovery: RecoveryPolicy::Resume,
            ..Default::default()
        },
    )
    .await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let result = async {
        let plan = app
            .prepare_json(
                access.clone(),
                include_bytes!("../../../examples/workflows/approval.v2.json"),
            )
            .await?;
        let mut request = StartRunRequest::new(plan, json!({"order_id":"order-42"}));
        request.options.receipt_key = Some(key);
        request.options.require_durable = true;
        let receipt = app.start(access.clone(), request).await?;
        let run = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let run = app.status(access.clone(), receipt.run_id.clone()).await?;
                if run.state == RunState::Waiting
                    || run.state.is_terminal()
                    || run.state == RunState::Blocked
                {
                    return Ok::<_, ForgeError>(run);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| ForgeError::new("example.timeout", "Reservation was not ready"))??;
        if mode == "start" {
            println!(
                "{}",
                json!({"receipt":receipt,"state":run.state,"waits":run.waits})
            );
        } else {
            let wait = run.waits.values().next().ok_or_else(|| {
                ForgeError::new("wait.not_found", "The run did not create a reservation")
            })?;
            let signal = app
                .signal(
                    access.clone(),
                    SignalCommand {
                        run_id: run.id.clone(),
                        wait_id: wait.id.clone(),
                        message_id: "approval-1".into(),
                        correlation: "order-42".into(),
                        payload: json!({"approved":true}),
                        artifacts: vec![],
                    },
                )
                .await?;
            let run = app.wait(access.clone(), run.id).await?;
            println!(
                "{}",
                json!({"receipt":receipt,"signal":signal,"state":run.state,"output":run.output})
            );
        }
        Ok::<_, ForgeError>(())
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
    result?;
    shutdown?;
    Ok(())
}
