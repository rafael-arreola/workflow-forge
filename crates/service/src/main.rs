use std::{process::ExitCode, sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_util::task::AbortOnDropHandle;
use workflow_forge::v2::*;
use workflow_forge_service::host::HostConfig;

/// Host-side observation is bounded and lossy. ExecutionEvent contains only
/// metadata; neither the engine's input nor credentials enter this channel.
struct ChannelObserver(mpsc::Sender<ExecutionEvent>);
impl ExecutionObserver for ChannelObserver {
    fn observe(&self, event: ExecutionEvent) -> PortFuture<'_, ()> {
        let _ = self.0.try_send(event);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if arguments.len() != 1 || arguments[0] == "--help" || arguments[0] == "-h" {
        eprintln!("Usage: workflow-forge-service <config.json>");
        return if arguments.len() == 1 {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(2)
        };
    }
    match run(&arguments[0]).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Do not print provider messages, configuration or credential values.
            eprintln!("workflow-forge-service failed ({})", error.code());
            ExitCode::FAILURE
        }
    }
}
async fn run(path: &std::ffi::OsStr) -> Result<(), ForgeError> {
    let config = HostConfig::load(std::path::Path::new(path)).await?;
    #[cfg(unix)]
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|_| ForgeError::new("service.signal", "Signal handler unavailable"))?;
    let (send, mut receive) = mpsc::channel(128);
    let mut service = config.boot(Arc::new(ChannelObserver(send))).await?;
    eprintln!("workflow-forge-service ready at {}", service.local_addr());
    let mut observer = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut output = tokio::io::stdout();
        while let Some(event) = receive.recv().await {
            let mut bytes = serde_json::to_vec(&event).expect("execution event serializes");
            bytes.push(b'\n');
            if output.write_all(&bytes).await.is_err() {
                break;
            }
        }
    }));
    let signal = async {
        #[cfg(unix)]
        tokio::select! { result=tokio::signal::ctrl_c()=>result, _=terminate.recv()=>Ok(()) }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await
    };
    let outcome = tokio::select! {
        signal=signal=>{
            let shutdown=service.shutdown().await;
            signal.map_err(|_|ForgeError::new("service.signal","Signal handler failed"))?;
            shutdown
        },
        result=service.wait()=>result,
    };
    if tokio::time::timeout(Duration::from_secs(1), &mut observer)
        .await
        .is_err()
    {
        observer.abort();
        let _ = observer.await;
    }
    let report = outcome?;
    eprintln!(
        "workflow-forge-service stopped (http_forced={}, engine_forced={}, pending={})",
        report.http_forced,
        report.engine.forced,
        report.engine.pending.len()
    );
    Ok(())
}
