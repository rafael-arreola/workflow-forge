//! Reproducible local baseline; arguments: nodes, JSON bytes, samples, concurrency.
//! Optional: --sqlite PATH, --sqlite-bytes N, --latency-ms N, --terminal-runs N,
//! --expect-resource-limit.
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use workflow_forge::prelude::*;

#[path = "support/measurement_store.rs"]
mod measurement_store;

#[derive(Default)]
struct Options {
    sqlite: Option<PathBuf>,
    sqlite_bytes: Option<u64>,
    latency_ms: u64,
    terminal_runs: Option<usize>,
    expect_resource_limit: bool,
}
impl Options {
    fn parse(args: &[String]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut options = Self::default();
        let mut seen = BTreeSet::new();
        let mut flags = args.iter();
        while let Some(flag) = flags.next() {
            if !seen.insert(flag) {
                return Err("Duplicate measurement option".into());
            }
            match flag.as_str() {
                "--sqlite" => {
                    options.sqlite = Some(flags.next().ok_or("Missing database path")?.into())
                }
                "--sqlite-bytes" => {
                    options.sqlite_bytes =
                        Some(flags.next().ok_or("Missing SQLite budget")?.parse()?);
                }
                "--latency-ms" => {
                    options.latency_ms = flags.next().ok_or("Missing latency")?.parse()?
                }
                "--terminal-runs" => {
                    options.terminal_runs =
                        Some(flags.next().ok_or("Missing retention count")?.parse()?);
                }
                "--expect-resource-limit" => options.expect_resource_limit = true,
                _ => {
                    return Err(
                        "Options: --sqlite PATH, --sqlite-bytes N, --latency-ms N, --terminal-runs N, --expect-resource-limit".into(),
                    );
                }
            }
        }
        if options.latency_ms > 1000 {
            return Err("Simulated latency must be 0..1000 ms".into());
        }
        Ok(options)
    }
}
struct SimulatedRead {
    descriptor: OperationDescriptor,
    delay: Duration,
}
impl Operation for SimulatedRead {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            Ok(OperationOutput::json(invocation.input))
        })
    }
}
fn simulated_read(latency_ms: u64) -> OperationBundle {
    let mut descriptor = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    descriptor.revision =
        OperationRevision::new("measure.read", "1", &format!("delay-{latency_ms}ms"));
    descriptor.effect = EffectKind::Read;
    descriptor.description = "Local simulated read for measurement".into();
    OperationBundle {
        module: ModuleDescriptor {
            id: "measure.read".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![descriptor.revision.clone()],
        },
        operations: vec![Arc::new(SimulatedRead {
            descriptor,
            delay: Duration::from_millis(latency_ms),
        })],
        inspectors: vec![],
    }
}

fn percentile(values: &mut [u128]) -> Value {
    values.sort_unstable();
    let n = values.len() - 1;
    json!({"p50_us":values[n/2] as f64/1000.0,"p95_us":values[n*95/100] as f64/1000.0,"p99_us":values[n*99/100] as f64/1000.0})
}
fn definition(count: usize) -> WorkflowDefinition {
    let nodes = (0..count)
        .map(|i| NodeDefinition {
            id: format!("n{i}"),
            instruction: Instruction::Operation {
                operation: OperationRevision::new("forge.data.identity", "1", "r1"),
                config: json!({}),
                retry: RetryPolicy::default(),
            },
            input: if i == 0 {
                Binding::Select(Selection {
                    source: DataSource::Input,
                    node: None,
                    pointer: String::new(),
                    fallback: None,
                })
            } else {
                Binding::Select(Selection {
                    source: DataSource::Node,
                    node: Some(format!("n{}", i - 1)),
                    pointer: String::new(),
                    fallback: None,
                })
            },
        })
        .collect();
    let edges = (1..count)
        .map(|i| Edge {
            from: format!("n{}", i - 1),
            to: format!("n{i}"),
        })
        .collect();
    WorkflowDefinition {
        format: WORKFLOW_FORMAT.into(),
        id: "measure.sequence".into(),
        revision: "1".into(),
        schema_dialect: SCHEMA_DIALECT.into(),
        input_schema: json!(true),
        output_schema: json!(true),
        entry: "n0".into(),
        nodes,
        edges,
        output: Binding::Select(Selection {
            source: DataSource::Node,
            node: Some(format!("n{}", count - 1)),
            pointer: String::new(),
            fallback: None,
        }),
        presentation: BTreeMap::new(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw: Vec<_> = std::env::args().skip(1).collect();
    let options = Options::parse(raw.get(4..).unwrap_or_default())?;
    let path = options.sqlite.as_deref();
    let args: Vec<_> = raw
        .iter()
        .take(4)
        .map(|s| s.parse::<usize>())
        .collect::<Result<_, _>>()?;
    let count = args.first().copied().unwrap_or(1);
    let bytes = args.get(1).copied().unwrap_or(1024);
    let samples = args.get(2).copied().unwrap_or(1000);
    let concurrency = args.get(3).copied().unwrap_or(1);
    if !(1..=256).contains(&count)
        || !(2..=1024 * 1024).contains(&bytes)
        || samples < 1000
        || !(1..=32).contains(&concurrency)
    {
        return Err("Expected 1..256 nodes, 2..1048576 bytes, at least 1000 samples and 1..32 concurrent runs".into());
    }
    let providers = match options.sqlite_bytes {
        Some(bytes) => measurement_store::MeasurementStore::with_sqlite_limit(path, Some(bytes))?,
        None => measurement_store::MeasurementStore::new(path)?,
    };
    let mut limits = Limits::default();
    if let Some(retained) = options.terminal_runs {
        if !(concurrency..=1000).contains(&retained) {
            return Err("Terminal retention must be between concurrency and 1000 runs".into());
        }
        limits.terminal_runs = retained;
    }
    let mut builder = WorkflowBuilder::standard()
        .limits(limits.clone())
        .execution_store(providers.execution)
        .artifact_store(providers.artifacts);
    let mut definition = definition(count);
    if options.latency_ms != 0 {
        let bundle = simulated_read(options.latency_ms);
        definition.nodes[0].instruction = Instruction::Operation {
            operation: bundle.module.exports[0].clone(),
            config: json!({}),
            retry: RetryPolicy::default(),
        };
        builder.register_bundle(bundle)?;
    }
    let expectation = Expectation {
        durable: path.is_some(),
        resource_limit: options.expect_resource_limit,
        run_bytes: limits.run_bytes,
    };
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let measurement: Result<(), Box<dyn std::error::Error>> = async {
        let payload = json!("x".repeat(bytes - 2));
        let mut preparation = Vec::with_capacity(samples);
        let mut plan = None;
        for i in 0..samples + 100 {
            let start = Instant::now();
            let next = app.prepare(access.clone(), definition.clone()).await?;
            if i >= 100 {
                preparation.push(start.elapsed().as_nanos());
            }
            plan = Some(next);
        }
        let plan = plan.unwrap();
        let mut execution = Vec::with_capacity(samples);
        let mut failed = 0;
        let mut resource_rejections = 0;
        let mut identities = BTreeSet::new();
        let mut max_retained_bytes = 0;
        // Finish the warmup before measuring, including when concurrency does not divide 100.
        for batch in (0..100).step_by(concurrency) {
            for sample in run_batch(
                &app,
                &access,
                &plan,
                &payload,
                (100 - batch).min(concurrency),
                expectation,
            )
            .await? {
            let unique = identities.insert(sample.id);
            if !sample.matched || !unique {
                return Err(format!("Warmup mismatch: state={:?}, error={:?}, retained_bytes={}, resource_rejection={}, unique_id={unique}", sample.state, sample.error_code, sample.retained_bytes, sample.resource_rejection).into());
                }
            }
        }
        let total_start = Instant::now();
        for batch in (0..samples).step_by(concurrency) {
            for sample in run_batch(
                &app,
                &access,
                &plan,
                &payload,
                (samples - batch).min(concurrency),
                expectation,
            )
            .await?
            {
                execution.push(sample.elapsed);
                failed += usize::from(!sample.matched || !identities.insert(sample.id));
                resource_rejections += usize::from(sample.resource_rejection);
                max_retained_bytes = max_retained_bytes.max(sample.retained_bytes);
            }
        }
        let seconds = total_start.elapsed().as_secs_f64();
        println!(
            "{}",
            json!({"nodes":count,"json_bytes":bytes,"warmup":100,"samples":samples,"concurrency":concurrency,"prepare":percentile(&mut preparation),"execute":percentile(&mut execution),"runs_per_second":samples as f64/seconds,"failed_runs":failed,"profile":providers.profile,"provider_options":providers.provider_options,"simulated_read_ms":options.latency_ms,"expected_outcome":if expectation.resource_limit {"resource.limit"} else {"succeeded"},"resource_rejections":resource_rejections,"verified_unique_runs":identities.len(),"max_retained_data_bytes":max_retained_bytes,"limits":limits})
        );
        if failed != 0 {
            return Err("Measurement contains failed or incorrect runs".into());
        }
        Ok(())
    }.await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
    measurement?;
    shutdown?;
    Ok(())
}

#[derive(Clone, Copy)]
struct Expectation {
    durable: bool,
    resource_limit: bool,
    run_bytes: usize,
}
struct Sample {
    id: RunId,
    state: RunState,
    error_code: Option<String>,
    elapsed: u128,
    matched: bool,
    resource_rejection: bool,
    retained_bytes: usize,
}

async fn run_batch(
    app: &WorkflowApplication,
    access: &AccessContext,
    plan: &PreparedWorkflow,
    payload: &Value,
    count: usize,
    expectation: Expectation,
) -> Result<Vec<Sample>, Box<dyn std::error::Error>> {
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..count {
        let app = app.clone();
        let access = access.clone();
        let plan = plan.clone();
        let input = payload.clone();
        jobs.spawn(async move {
            let start = Instant::now();
            let expected = input.clone();
            let mut request = StartRunRequest::new(plan, input);
            request.options.require_durable = expectation.durable;
            let accepted = app.start(access.clone(), request).await?;
            let durable = accepted.durable;
            let result = app.wait(access, accepted.run_id).await?;
            let elapsed = start.elapsed().as_nanos();
            let retained_bytes = result.retained_data_bytes();
            let resource_rejection = result.state == RunState::Failed
                && result
                    .error
                    .as_ref()
                    .is_some_and(|error| error.code() == "resource.limit")
                && result.output.is_none();
            let correct = if expectation.resource_limit {
                resource_rejection
            } else {
                result.state == RunState::Succeeded && result.output.as_ref() == Some(&expected)
            };
            Ok::<_, ForgeError>(Sample {
                id: result.id,
                state: result.state,
                error_code: result.error.as_ref().map(|error| error.code().to_owned()),
                elapsed,
                matched: correct
                    && durable == expectation.durable
                    && retained_bytes <= expectation.run_bytes,
                resource_rejection,
                retained_bytes,
            })
        });
    }
    let mut results = Vec::with_capacity(count);
    while let Some(outcome) = jobs.join_next().await {
        results.push(outcome??);
    }
    Ok(results)
}
