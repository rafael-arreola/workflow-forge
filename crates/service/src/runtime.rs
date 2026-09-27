use crate::{RequestAuthenticator, jobs::Jobs, routes};
use std::{
    collections::BTreeMap,
    future::IntoFuture,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Semaphore},
    task::JoinHandle,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use workflow_forge::v2::*;

/// Host policy, independent from workflow execution budgets in `Limits`.
#[derive(Clone)]
pub struct ServiceOptions {
    pub listen: SocketAddr,
    pub scope: String,
    pub definitions: Vec<WorkflowDefinition>,
    pub max_prepared: usize,
    pub max_requests: usize,
    pub max_json_bytes: usize,
    pub max_response_bytes: usize,
    pub max_artifact_bytes: usize,
    pub request_timeout: Duration,
    pub http_shutdown_timeout: Duration,
    pub engine_shutdown_timeout: Duration,
}
impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 7070)),
            scope: "default".into(),
            definitions: Vec::new(),
            max_prepared: 1000,
            max_requests: 64,
            max_json_bytes: 2 * 1024 * 1024,
            max_response_bytes: 8 * 1024 * 1024,
            max_artifact_bytes: 8 * 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            http_shutdown_timeout: Duration::from_secs(30),
            engine_shutdown_timeout: Duration::from_secs(30),
        }
    }
}
impl ServiceOptions {
    fn validate(&self) -> Result<(), ForgeError> {
        if self.scope.is_empty()
            || self.scope.len() > 256
            || !(1..=10_000).contains(&self.max_prepared)
            || self.definitions.len() > self.max_prepared
            || !(1..=65_536).contains(&self.max_requests)
            || !(256..=64 * 1024 * 1024).contains(&self.max_json_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_response_bytes)
            || self.max_artifact_bytes == 0
            || self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(3600)
            || self.http_shutdown_timeout.is_zero()
            || self.http_shutdown_timeout > Duration::from_secs(3600)
            || self.engine_shutdown_timeout.is_zero()
            || self.engine_shutdown_timeout > Duration::from_secs(3600)
        {
            return Err(ForgeError::new(
                "service.config",
                "Invalid service limits or scope",
            ));
        }
        Ok(())
    }
}

pub(crate) struct Shared {
    pub app: WorkflowApplication,
    pub authenticator: Arc<dyn RequestAuthenticator>,
    pub options: ServiceOptions,
    pub instance: String,
    pub plans: Mutex<BTreeMap<(String, String), PreparedWorkflow>>,
    pub requests: Arc<Semaphore>,
    pub ready: AtomicBool,
    pub stop: CancellationToken,
    pub force: CancellationToken,
    pub jobs: Jobs,
}

#[derive(Debug)]
pub struct ServiceShutdownReport {
    pub http_forced: bool,
    pub engine: ShutdownReport,
}

/// Owns the listener, transfer jobs and engine. Explicit shutdown awaits cleanup;
/// dropping the host aborts its supervisor and cannot certify a graceful drain.
pub struct ServiceRuntime {
    shared: Arc<Shared>,
    address: SocketAddr,
    supervisor: Option<JoinHandle<Result<ServiceShutdownReport, ForgeError>>>,
    abort: tokio::task::AbortHandle,
}

struct StopOnDrop(Arc<Shared>);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.ready.store(false, Ordering::Release);
        self.0.stop.cancel();
        self.0.force.cancel();
        // Dropping JoinSet aborts all remaining owned transfer futures.
        drop(self.0.jobs.close());
    }
}

impl ServiceRuntime {
    pub async fn boot(
        assembly: EngineAssembly,
        authenticator: Arc<dyn RequestAuthenticator>,
        options: ServiceOptions,
    ) -> Result<Self, ForgeError> {
        options.validate()?;
        let engine = EngineRuntime::boot(
            assembly,
            BootOptions {
                definitions: options.definitions.clone(),
            },
        )
        .await?;
        let app = engine.application();
        let initialize = async {
            let access = AccessContext::trusted(&options.scope);
            app.capabilities(&access)?;
            let mut plans = BTreeMap::new();
            for definition in &options.definitions {
                let plan = app.prepare(access.clone(), definition.clone()).await?;
                plans.insert((definition.id.clone(), definition.revision.clone()), plan);
            }
            let listener = TcpListener::bind(options.listen).await.map_err(|_| {
                ForgeError::new("service.bind", "Could not bind the configured listener")
            })?;
            let address = listener.local_addr().map_err(|_| {
                ForgeError::new("service.bind", "Could not obtain listener address")
            })?;
            Ok::<_, ForgeError>((plans, listener, address))
        }
        .await;
        let (plans, listener, address) = match initialize {
            Ok(value) => value,
            Err(mut error) => {
                if let Err(cleanup) = engine
                    .shutdown(ShutdownOptions {
                        timeout: options.engine_shutdown_timeout,
                    })
                    .await
                {
                    error.diagnostics.extend(cleanup.diagnostics);
                }
                return Err(error);
            }
        };
        let shared = Arc::new(Shared {
            app,
            authenticator,
            requests: Arc::new(Semaphore::new(options.max_requests)),
            options,
            instance: uuid::Uuid::now_v7().to_string(),
            plans: Mutex::new(plans),
            ready: AtomicBool::new(true),
            stop: CancellationToken::new(),
            force: CancellationToken::new(),
            jobs: Jobs::new(),
        });
        let worker = shared.clone();
        let supervisor = tokio::spawn(supervise(worker, engine, listener));
        Ok(Self {
            shared,
            address,
            abort: supervisor.abort_handle(),
            supervisor: Some(supervisor),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub fn application(&self) -> WorkflowApplication {
        self.shared.app.clone()
    }
    pub fn is_ready(&self) -> bool {
        self.shared.ready.load(Ordering::Acquire) && self.shared.app.is_ready()
    }

    pub async fn wait(&mut self) -> Result<ServiceShutdownReport, ForgeError> {
        let task = self
            .supervisor
            .as_mut()
            .ok_or_else(|| ForgeError::new("service.stopped", "Service has already joined"))?;
        let outcome = task
            .await
            .map_err(|_| ForgeError::new("service.failed", "Service supervisor failed"));
        self.supervisor = None;
        outcome?
    }

    pub async fn shutdown(mut self) -> Result<ServiceShutdownReport, ForgeError> {
        self.shared.stop.cancel();
        self.wait().await
    }
}
impl Drop for ServiceRuntime {
    fn drop(&mut self) {
        self.shared.ready.store(false, Ordering::Release);
        self.shared.force.cancel();
        self.shared.stop.cancel();
        drop(self.shared.jobs.close());
        self.abort.abort();
    }
}

async fn supervise(
    shared: Arc<Shared>,
    engine: EngineRuntime,
    listener: TcpListener,
) -> Result<ServiceShutdownReport, ForgeError> {
    let _guard = StopOnDrop(shared.clone());
    let router = routes::router(shared.clone());
    let transport_failed = CancellationToken::new();
    let listener = crate::listener::ManagedListener {
        listener,
        force: shared.force.clone(),
        failed: transport_failed.clone(),
    };
    let mut server = AbortOnDropHandle::new(tokio::spawn(
        axum::serve(listener, router)
            .with_graceful_shutdown(shared.stop.clone().cancelled_owned())
            .into_future(),
    ));
    let mut server_finished = false;
    let mut failure = None;
    tokio::select! {
        biased;
        _ = shared.stop.cancelled() => {},
        _ = transport_failed.cancelled() => { failure = Some(ForgeError::new("service.failed", "Listener failed")); },
        _ = async {
            while shared.app.is_ready() { tokio::time::sleep(Duration::from_millis(10)).await; }
        } => { failure = Some(ForgeError::new("runtime.failed", "Engine lost readiness")); },
        _ = &mut server => {
            server_finished = true;
            failure = Some(ForgeError::new("service.failed", "HTTP server stopped unexpectedly"));
        }
    }
    shared.ready.store(false, Ordering::Release);
    // Closing logical admission precedes graceful listener shutdown.
    let drain = async {
        shared.app.close_admission().await;
        shared.stop.cancel();
        if !server_finished {
            match (&mut server).await {
                Ok(Ok(())) => {}
                _ => {
                    failure = Some(ForgeError::new(
                        "service.failed",
                        "HTTP server failed during drain",
                    ));
                }
            }
            server_finished = true;
        }
        let mut jobs = shared.jobs.close();
        while jobs.join_next().await.is_some() {}
    };
    let http_forced = tokio::time::timeout(shared.options.http_shutdown_timeout, drain)
        .await
        .is_err();
    if http_forced {
        shared.force.cancel();
        shared.stop.cancel();
        let mut jobs = shared.jobs.close();
        jobs.abort_all();
        while jobs.join_next().await.is_some() {}
        if !server_finished
            && tokio::time::timeout(Duration::from_secs(1), &mut server)
                .await
                .is_err()
        {
            server.abort();
        }
    }
    let engine_result = engine
        .shutdown(ShutdownOptions {
            timeout: shared.options.engine_shutdown_timeout,
        })
        .await;
    if let Some(mut error) = failure {
        if let Err(cleanup) = engine_result {
            error.diagnostics.extend(cleanup.diagnostics);
        }
        return Err(error);
    }
    Ok(ServiceShutdownReport {
        http_forced,
        engine: engine_result?,
    })
}
