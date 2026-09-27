//! Transport comparison; each process measures one surface and one storage profile.
//! Usage: service_measure rust|http BYTES SAMPLES CONCURRENCY [NEW_SQLITE_PATH]
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use workflow_forge::v2::*;
use workflow_forge_service::{
    BearerIdentity, ServiceOptions, ServiceRuntime, StaticBearerAuth, dto::PrepareResponse,
};

type Error = Box<dyn std::error::Error + Send + Sync>;

enum Host {
    Rust(EngineRuntime),
    Http(ServiceRuntime),
}
impl Host {
    fn application(&self) -> WorkflowApplication {
        match self {
            Self::Rust(runtime) => runtime.application(),
            Self::Http(service) => service.application(),
        }
    }
    async fn shutdown(self) -> Result<(), Error> {
        let forced = match self {
            Self::Rust(runtime) => runtime.shutdown(ShutdownOptions::default()).await?.forced,
            Self::Http(service) => {
                let report = service.shutdown().await?;
                report.http_forced || report.engine.forced
            }
        };
        if forced {
            return Err("Measurement host required a forced shutdown".into());
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Http {
    client: reqwest::Client,
    base: String,
    token: String,
}
#[derive(Clone)]
struct Surface {
    app: WorkflowApplication,
    access: AccessContext,
    http: Option<Http>,
    plan: Option<PreparedWorkflow>,
    definition: WorkflowDefinition,
    durable: bool,
}
impl Surface {
    async fn prepare(&self) -> Result<Option<PreparedWorkflow>, Error> {
        if let Some(http) = &self.http {
            let response = http
                .client
                .post(format!("{}/v2/workflows/prepare", http.base))
                .bearer_auth(&http.token)
                .json(&json!({"definition":self.definition}))
                .send()
                .await?
                .error_for_status()?;
            let prepared: PrepareResponse = response.json().await?;
            if prepared.workflow.id != self.definition.id
                || prepared.workflow.revision != self.definition.revision
            {
                return Err("Preparation returned a different revision".into());
            }
            Ok(None)
        } else {
            Ok(Some(
                self.app
                    .prepare(self.access.clone(), self.definition.clone())
                    .await?,
            ))
        }
    }

    async fn run(&self, input: Value) -> Result<Observation, Error> {
        let started = Instant::now();
        let receipt: StartReceipt = if let Some(http) = &self.http {
            let response = http.client.post(format!("{}/v2/runs", http.base))
                .bearer_auth(&http.token)
                .json(&json!({"workflow":{"id":self.definition.id,"revision":self.definition.revision},"input":input,"options":{"require_durable":self.durable}}))
                .send().await?;
            if response.status() != reqwest::StatusCode::ACCEPTED {
                return Err(format!("Start rejected: {}", response.status()).into());
            }
            response.json().await?
        } else {
            let mut request = StartRunRequest::new(
                self.plan.clone().ok_or("Missing prepared plan")?,
                input.clone(),
            );
            request.options.require_durable = self.durable;
            self.app.start(self.access.clone(), request).await?
        };
        let accept_ns = started.elapsed().as_nanos();
        if receipt.duplicate || receipt.durable != self.durable {
            return Err("Receipt violated expected identity/durability".into());
        }
        let mut queries = 0;
        let output = if let Some(http) = &self.http {
            loop {
                if started.elapsed() > Duration::from_secs(30) {
                    return Err("Result polling exceeded 30 seconds".into());
                }
                queries += 1;
                let response = http
                    .client
                    .get(format!("{}/v2/runs/{}/result", http.base, receipt.run_id))
                    .bearer_auth(&http.token)
                    .send()
                    .await?;
                let status = response.status();
                if status == reqwest::StatusCode::OK {
                    let result: Value = response.json().await?;
                    if result["run_id"] != json!(receipt.run_id) {
                        return Err("Result belongs to a different run".into());
                    }
                    break result.get("output").ok_or("Result has no output")?.clone();
                }
                let error: ForgeError = response.json().await?;
                if status != reqwest::StatusCode::CONFLICT || error.code() != "not_ready" {
                    return Err(error.into());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        } else {
            let result = self
                .app
                .wait(self.access.clone(), receipt.run_id.clone())
                .await?;
            if result.state != RunState::Succeeded {
                return Err("Run failed or blocked".into());
            }
            result.output.ok_or("Run has no output")?
        };
        if output != input {
            return Err("Measurement returned incorrect data".into());
        }
        Ok(Observation {
            id: receipt.run_id,
            accept_ns,
            complete_ns: started.elapsed().as_nanos(),
            queries,
        })
    }
}
struct Observation {
    id: RunId,
    accept_ns: u128,
    complete_ns: u128,
    queries: usize,
}

fn percentile(mut values: Vec<u128>) -> Value {
    values.sort_unstable();
    let last = values.len() - 1;
    json!({"p50_us":values[last/2] as f64/1000.0,"p95_us":values[last*95/100] as f64/1000.0,"p99_us":values[last*99/100] as f64/1000.0})
}

async fn measure(
    mut surface: Surface,
    bytes: usize,
    samples: usize,
    concurrency: usize,
) -> Result<Value, Error> {
    let payload = json!("x".repeat(bytes - 2));
    let mut cold_prepare = 0;
    let mut preparations = Vec::with_capacity(samples);
    for i in 0..100 + samples {
        let started = Instant::now();
        surface.plan = surface.prepare().await?;
        let elapsed = started.elapsed().as_nanos();
        if i == 0 {
            cold_prepare = elapsed;
        }
        if i >= 100 {
            preparations.push(elapsed);
        }
    }
    let mut ids = BTreeSet::new();
    let mut measured = Vec::with_capacity(samples);
    let mut seconds = 0.0;
    // The entire warmup finishes before the measurement clock starts.
    for (count, warmup) in [(100, true), (samples, false)] {
        let started = Instant::now();
        for first in (0..count).step_by(concurrency) {
            let outcomes = stream::iter((0..(count - first).min(concurrency)).map(|_| {
                let surface = &surface;
                let input = payload.clone();
                async move { surface.run(input).await }
            }))
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
            for outcome in outcomes {
                let result = outcome?;
                if !ids.insert(result.id.clone()) {
                    return Err("A run ID was accepted twice".into());
                }
                if !warmup {
                    measured.push(result);
                }
            }
        }
        if !warmup {
            seconds = started.elapsed().as_secs_f64();
        }
    }
    Ok(json!({
        "surface":if surface.http.is_some(){"http-loopback"}else{"rust"},
        "profile":if surface.durable{"sqlite-wal-full"}else{"default-memory"},
        "nodes":1,"json_bytes":bytes,"warmup":100,"samples":samples,"concurrency":concurrency,
        "cold_prepare_us":cold_prepare as f64/1000.0,"prepare":percentile(preparations),
        "accept":percentile(measured.iter().map(|o|o.accept_ns).collect()),
        "complete":percentile(measured.iter().map(|o|o.complete_ns).collect()),
        "runs_per_second":samples as f64/seconds,
        "result_queries_per_run":measured.iter().map(|o|o.queries).sum::<usize>() as f64/samples as f64,
        "poll_delay_ms":if surface.http.is_some(){1}else{0},
        "verified_runs":ids.len(),"failed_runs":0,
        "limits":Limits::default()
    }))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(4..=5).contains(&args.len()) || !matches!(args[0].as_str(), "rust" | "http") {
        return Err(
            "Usage: service_measure rust|http BYTES SAMPLES CONCURRENCY [NEW_SQLITE_PATH]".into(),
        );
    }
    let bytes: usize = args[1].parse()?;
    let samples: usize = args[2].parse()?;
    let concurrency: usize = args[3].parse()?;
    if !(2..=65536).contains(&bytes)
        || !(1000..=10000).contains(&samples)
        || !(1..=32).contains(&concurrency)
    {
        return Err(
            "Expected 2..65536 bytes, 1000..10000 samples and 1..32 concurrent runs".into(),
        );
    }
    let durable = args.len() == 5;
    let mut builder = WorkflowBuilder::standard();
    if let Some(path) = args.get(4) {
        if Path::new(path).exists() {
            return Err("SQLite measurement path must be new".into());
        }
        let store = Arc::new(modules::SqliteExecutionStore::open(
            path,
            modules::SqliteOptions::default(),
        )?);
        builder = builder.execution_store(store.clone()).artifact_store(store);
    }
    let access = AccessContext::trusted("default");
    let definition =
        serde_json::from_slice(include_bytes!("../../../examples/service/echo.v2.json"))?;
    let client = if args[0] == "http" {
        Some(
            reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .timeout(Duration::from_secs(30))
                .build()?,
        )
    } else {
        None
    };
    let token = format!("measurement-{}", uuid::Uuid::now_v7());
    let assembly = builder.build()?;
    let host = if args[0] == "http" {
        let auth = Arc::new(StaticBearerAuth::new(vec![BearerIdentity {
            token: SecretValue::new(token.clone()),
            access: access.clone(),
        }])?);
        Host::Http(
            ServiceRuntime::boot(
                assembly,
                auth,
                ServiceOptions {
                    listen: ([127, 0, 0, 1], 0).into(),
                    ..Default::default()
                },
            )
            .await?,
        )
    } else {
        Host::Rust(EngineRuntime::boot(assembly, BootOptions::default()).await?)
    };
    let http = match &host {
        Host::Http(service) => Some(Http {
            client: client.expect("HTTP client is built before boot"),
            base: format!("http://{}", service.local_addr()),
            token,
        }),
        Host::Rust(_) => None,
    };
    let surface = Surface {
        app: host.application(),
        access,
        http,
        plan: None,
        durable,
        definition,
    };
    let result = measure(surface, bytes, samples, concurrency).await;
    let shutdown = host.shutdown().await;
    let report = result?;
    shutdown?;
    println!("{report}");
    Ok(())
}
