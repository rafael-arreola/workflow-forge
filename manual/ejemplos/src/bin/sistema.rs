//! An application facade reuses its plan and bounds work per batch.
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use workflow_forge::prelude::*;

#[derive(Clone)]
struct Normalizador {
    app: WorkflowApplication,
    plan: PreparedWorkflow,
}

impl Normalizador {
    async fn preparar(app: WorkflowApplication, access: AccessContext) -> Result<Self, ForgeError> {
        let plan = app
            .prepare_json(access, include_bytes!("../../workflows/normalizar.json"))
            .await?;
        Ok(Self { app, plan })
    }

    async fn normalizar(
        &self,
        access: AccessContext,
        nombre: &str,
        cancel: CancellationToken,
    ) -> Result<Value, ForgeError> {
        let mut request = StartRunRequest::new(self.plan.clone(), json!(nombre));
        request.options.timeout_ms = Some(1_000);
        self.app.execute(access, request, cancel).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime =
        EngineRuntime::boot(WorkflowBuilder::standard().build()?, BootOptions::default()).await?;
    let outcome = async {
        // Trusted context only for this local demonstration.
        let access = AccessContext::trusted("default");
        let normalizador = Normalizador::preparar(runtime.application(), access.clone()).await?;
        let host_cancel = CancellationToken::new();
        let results = stream::iter(
            ["  Ada  ", "  Rafael  ", "  Grace  "]
                .into_iter()
                .enumerate(),
        )
        .map(|(index, nombre)| {
            let handler = normalizador.clone();
            let access = access.clone();
            let cancel = host_cancel.child_token();
            async move { (index, handler.normalizar(access, nombre, cancel).await) }
        })
        .buffer_unordered(2)
        .collect::<Vec<_>>()
        .await;
        let mut ordered = results;
        ordered.sort_by_key(|(index, _)| *index);
        for ((_, result), expected) in ordered.into_iter().zip(["Ada", "Rafael", "Grace"]) {
            let output = result?;
            assert_eq!(output, json!(expected));
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
