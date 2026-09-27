use futures::{StreamExt, stream};
use serde_json::json;
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime =
        EngineRuntime::boot(WorkflowBuilder::standard().build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let outcome = async {
        let bytes = b"nombre\nRafael\n".to_vec();
        let content: ByteStream = Box::pin(stream::iter(vec![Ok(bytes.clone())]));
        let reference = app
            .write_artifact(access.clone(), content, "text/csv")
            .await?;
        let plan = app
            .prepare_json(
                access.clone(),
                include_bytes!("../../workflows/archivo.json"),
            )
            .await?;
        let mut request = StartRunRequest::new(plan, json!({"source":reference}));
        // El JSON solo transporta la referencia; esta lista la vincula al run.
        request.options.artifacts.push(reference.clone());
        let receipt = app.start(access.clone(), request).await?;
        app.wait(access.clone(), receipt.run_id.clone()).await?;
        let output = app.result(access.clone(), receipt.run_id).await?;
        let mut content = app.read_artifact(access, &reference).await?;
        let mut downloaded = Vec::new();
        while let Some(chunk) = content.next().await {
            let chunk = chunk?;
            if downloaded.len() + chunk.len() > 1024 {
                return Err(ForgeError::new(
                    "manual.limit",
                    "Archivo mayor al presupuesto del ejemplo",
                ));
            }
            downloaded.extend(chunk);
        }
        assert_eq!(downloaded, bytes);
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
