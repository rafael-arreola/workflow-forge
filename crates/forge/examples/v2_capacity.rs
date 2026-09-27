//! Public-API capacity probes. See docs/ACCEPTANCE.md for the measurement protocol.
//! saturation CONCURRENCY [--sqlite NEW_PATH]
//! release CONCURRENCY JSON_BYTES [--cycles 10..100] [--sqlite NEW_PATH]
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, task::JoinSet};
use workflow_forge::v2::*;

#[path = "support/measurement_heap.rs"]
mod measurement_heap;
#[path = "support/measurement_store.rs"]
mod measurement_store;

type Measurement<T> = Result<T, Box<dyn std::error::Error>>;

struct Options {
    saturation: bool,
    concurrency: usize,
    bytes: usize,
    sqlite: Option<PathBuf>,
    cycles: usize,
}
impl Options {
    fn parse() -> Measurement<Self> {
        let args: Vec<_> = std::env::args().skip(1).collect();
        let saturation = match args.first().map(String::as_str) {
            Some("saturation") => true,
            Some("release") => false,
            _ => return Err("Expected saturation C or release C JSON_BYTES".into()),
        };
        let concurrency = args.get(1).ok_or("Missing concurrency")?.parse()?;
        let bytes = if saturation {
            1024
        } else {
            args.get(2).ok_or("Missing JSON byte count")?.parse()?
        };
        if !(1..=32).contains(&concurrency) || !(2..=1024 * 1024).contains(&bytes) {
            return Err("Expected 1..32 concurrent runs and 2..1048576 JSON bytes".into());
        }
        let mut sqlite = None;
        let mut cycles = 10;
        let mut seen = BTreeSet::new();
        let mut flags = args[if saturation { 2 } else { 3 }..].iter();
        while let Some(flag) = flags.next() {
            if !seen.insert(flag) {
                return Err("Duplicate measurement option".into());
            }
            match flag.as_str() {
                "--sqlite" => {
                    sqlite = Some(PathBuf::from(flags.next().ok_or("Missing SQLite path")?))
                }
                "--cycles" if !saturation => {
                    cycles = flags.next().ok_or("Missing release cycle count")?.parse()?
                }
                _ => {
                    return Err(
                        "Expected --sqlite NEW_PATH or --cycles 10..100 (release only)".into(),
                    );
                }
            }
        }
        if !(10..=100).contains(&cycles) {
            return Err("Release requires 10..100 cycles".into());
        }
        Ok(Self {
            saturation,
            concurrency,
            bytes,
            sqlite,
            cycles,
        })
    }
}

struct Gate {
    permits: Semaphore,
    active: AtomicUsize,
    peak: AtomicUsize,
    calls: Mutex<BTreeMap<RunId, usize>>,
}
impl Gate {
    fn new() -> Self {
        Self {
            permits: Semaphore::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            calls: Mutex::new(BTreeMap::new()),
        }
    }
}
struct ActiveCall<'a>(&'a AtomicUsize);
impl Drop for ActiveCall<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct ControlledRead {
    descriptor: OperationDescriptor,
    gate: Arc<Gate>,
}
impl Operation for ControlledRead {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> OperationFuture<'a> {
        Box::pin(async move {
            let active = self.gate.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _guard = ActiveCall(&self.gate.active);
            self.gate.peak.fetch_max(active, Ordering::SeqCst);
            *self
                .gate
                .calls
                .lock()
                .expect("measurement mutex")
                .entry(context.run_id)
                .or_default() += 1;
            self.gate
                .permits
                .acquire()
                .await
                .map_err(|_| OperationError {
                    code: "measurement.closed".into(),
                    class: ErrorClass::Internal,
                    certainty: EffectCertainty::NotApplied,
                    message: "Measurement gate closed".into(),
                })?
                .forget();
            Ok(OperationOutput::json(invocation.input))
        })
    }
}
fn controlled_read(gate: Arc<Gate>) -> OperationBundle {
    let mut descriptor = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    descriptor.revision = OperationRevision::new("measure.capacity", "1", "r1");
    descriptor.effect = EffectKind::Read;
    descriptor.repetition = Repetition::Safe;
    descriptor.description = "Host-controlled read for capacity measurement".into();
    OperationBundle {
        module: ModuleDescriptor {
            id: "measure.capacity".into(),
            version: "1".into(),
            protocol_version: PROTOCOL_VERSION,
            exports: vec![descriptor.revision.clone()],
        },
        operations: vec![Arc::new(ControlledRead { descriptor, gate })],
        inspectors: vec![],
    }
}
fn definition(saturation: bool) -> Measurement<WorkflowDefinition> {
    Ok(serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT, "id":"measure.capacity", "revision":"1",
        "schema_dialect":SCHEMA_DIALECT, "input_schema":true, "output_schema":true,
        "entry":"one", "nodes":[{
            "id":"one", "kind":"operation",
            "operation":{"id":if saturation {"measure.capacity"} else {"forge.data.identity"}, "contract":"1", "implementation":"r1"},
            "config":{}, "input":{"select":{"source":"input", "pointer":""}}
        }], "edges":[], "output":{"select":{"source":"node", "node":"one", "pointer":""}}
    }))?)
}

struct Harness {
    app: WorkflowApplication,
    plan: PreparedWorkflow,
    store: Arc<dyn ExecutionStore>,
    payload: Value,
    durable: bool,
    run_bytes: usize,
}
fn access() -> AccessContext {
    AccessContext::trusted("default")
}
fn invalid(message: &str) -> ForgeError {
    ForgeError::new("measurement.invalid", message)
}

struct Accepted {
    id: RunId,
    started: Instant,
    acceptance_ns: u128,
}
struct Completed {
    id: RunId,
    acceptance_ns: u128,
    complete_ns: u128,
}
async fn accept_one(
    app: &WorkflowApplication,
    request: StartRunRequest,
    durable: bool,
) -> Result<Accepted, ForgeError> {
    let started = Instant::now();
    let receipt = app.start(access(), request).await?;
    let acceptance_ns = started.elapsed().as_nanos();
    if receipt.durable != durable || receipt.duplicate {
        return Err(invalid("Wrong durability or duplicate receipt"));
    }
    Ok(Accepted {
        id: receipt.run_id,
        started,
        acceptance_ns,
    })
}
async fn finish_one(
    app: &WorkflowApplication,
    accepted: Accepted,
    expected: &Value,
    budget: usize,
) -> Result<Completed, ForgeError> {
    let result = app.wait(access(), accepted.id.clone()).await?;
    let complete_ns = accepted.started.elapsed().as_nanos();
    if result.id != accepted.id
        || result.state != RunState::Succeeded
        || result.output.as_ref() != Some(expected)
        || result.retained_data_bytes() > budget
    {
        return Err(invalid(
            "Run failed, returned wrong output or exceeded its budget",
        ));
    }
    Ok(Completed {
        id: result.id,
        acceptance_ns: accepted.acceptance_ns,
        complete_ns,
    })
}
impl Harness {
    async fn accept(&self, count: usize) -> Measurement<Vec<Accepted>> {
        let mut jobs = JoinSet::new();
        for _ in 0..count {
            let app = self.app.clone();
            let mut request = StartRunRequest::new(self.plan.clone(), self.payload.clone());
            request.options.require_durable = self.durable;
            let durable = self.durable;
            jobs.spawn(async move { accept_one(&app, request, durable).await });
        }
        let mut accepted = Vec::with_capacity(count);
        while let Some(result) = jobs.join_next().await {
            accepted.push(result??);
        }
        Ok(accepted)
    }
    async fn finish(&self, accepted: Vec<Accepted>) -> Measurement<Vec<Completed>> {
        let mut jobs = JoinSet::new();
        for accepted in accepted {
            let app = self.app.clone();
            let expected = self.payload.clone();
            let budget = self.run_bytes;
            jobs.spawn(async move { finish_one(&app, accepted, &expected, budget).await });
        }
        let mut completed = Vec::with_capacity(jobs.len());
        while let Some(result) = jobs.join_next().await {
            completed.push(result??);
        }
        Ok(completed)
    }
    async fn complete(&self, count: usize) -> Measurement<Vec<Completed>> {
        let mut jobs = JoinSet::new();
        for _ in 0..count {
            let app = self.app.clone();
            let mut request = StartRunRequest::new(self.plan.clone(), self.payload.clone());
            request.options.require_durable = self.durable;
            let durable = self.durable;
            let expected = self.payload.clone();
            let budget = self.run_bytes;
            jobs.spawn(async move {
                let accepted = accept_one(&app, request, durable).await?;
                // Observe this run immediately: another start in the batch may be slow
                // enough for an already completed result to expire under the short TTL.
                finish_one(&app, accepted, &expected, budget).await
            });
        }
        let mut completed = Vec::with_capacity(count);
        while let Some(result) = jobs.join_next().await {
            completed.push(result??);
        }
        Ok(completed)
    }
    async fn reject(&self, count: usize) -> Measurement<Vec<u128>> {
        let mut jobs = JoinSet::new();
        for _ in 0..count {
            let app = self.app.clone();
            let mut request = StartRunRequest::new(self.plan.clone(), self.payload.clone());
            request.options.require_durable = self.durable;
            jobs.spawn(async move {
                let started = Instant::now();
                let result = app.start(access(), request).await;
                let elapsed = started.elapsed().as_nanos();
                match result {
                    Err(error) if error.code() == "admission.full" => Ok(elapsed),
                    _ => Err(invalid("Full admission did not reject with admission.full")),
                }
            });
        }
        let mut timings = Vec::with_capacity(count);
        while let Some(result) = jobs.join_next().await {
            timings.push(result??);
        }
        Ok(timings)
    }
    async fn empty_and_ready(&self) -> Measurement<()> {
        if !self.app.is_ready() || !self.store.unfinished_heads().await?.is_empty() {
            return Err("Instance lost readiness or retained unfinished work".into());
        }
        Ok(())
    }
}

#[derive(Default)]
struct Samples {
    identities: BTreeSet<RunId>,
    acceptance: Vec<u128>,
    complete: Vec<u128>,
    rejection: Vec<u128>,
}
impl Samples {
    fn record(&mut self, completed: Vec<Completed>, measured: bool) -> Measurement<Vec<RunId>> {
        let mut ids = Vec::with_capacity(completed.len());
        for sample in completed {
            if !self.identities.insert(sample.id.clone()) {
                return Err("RunId repeated".into());
            }
            if measured {
                self.acceptance.push(sample.acceptance_ns);
                self.complete.push(sample.complete_ns);
            }
            ids.push(sample.id);
        }
        Ok(ids)
    }
    fn report(&mut self) -> Value {
        json!({"warmup":100, "samples":self.complete.len(),
            "verified_unique_runs":self.identities.len(), "incorrect_runs":0,
            "accept":percentiles(&mut self.acceptance), "complete":percentiles(&mut self.complete),
            "rejected_requests":self.rejection.len(), "reject":percentiles(&mut self.rejection)})
    }
}
fn percentiles(values: &mut [u128]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_unstable();
    let n = values.len() - 1;
    json!({"p50_us":values[n/2] as f64/1000.0,
        "p95_us":values[n*95/100] as f64/1000.0, "p99_us":values[n*99/100] as f64/1000.0})
}

async fn verify_full(
    harness: &Harness,
    gate: &Gate,
    accepted: &[Accepted],
    concurrency: usize,
) -> Measurement<()> {
    let expected: BTreeSet<_> = accepted.iter().map(|run| run.id.clone()).collect();
    let actual: BTreeSet<_> = harness
        .store
        .unfinished_heads()
        .await?
        .into_iter()
        .map(|run| run.id)
        .collect();
    let calls = gate.calls.lock().expect("measurement mutex");
    if actual != expected
        || expected.len() != 3 * concurrency
        || gate.active.load(Ordering::SeqCst) != concurrency
        || gate.peak.load(Ordering::SeqCst) > concurrency
        || calls.len() != concurrency
        || calls.values().any(|&count| count != 1)
    {
        return Err("Admission or operation concurrency escaped the configured bound".into());
    }
    Ok(())
}

async fn gated_batch(
    harness: &Harness,
    gate: &Gate,
    count: usize,
    concurrency: usize,
    measured: bool,
) -> Measurement<(Vec<Completed>, Vec<u128>, Duration)> {
    let accepted = harness.accept(count).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.active.load(Ordering::SeqCst) != count.min(concurrency)
            || gate.calls.lock().expect("measurement mutex").len() != count.min(concurrency)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let mut rejected = Vec::new();
    let mut held = Duration::ZERO;
    if measured {
        verify_full(harness, gate, &accepted, concurrency).await?;
        let hold_started = Instant::now();
        rejected.extend(harness.reject(count).await?);
        tokio::time::sleep(Duration::from_millis(100)).await;
        verify_full(harness, gate, &accepted, concurrency).await?;
        rejected.extend(harness.reject(count).await?);
        verify_full(harness, gate, &accepted, concurrency).await?;
        held = hold_started.elapsed();
    }
    gate.permits.add_permits(count);
    let completed = harness.finish(accepted).await?;
    let calls = std::mem::take(&mut *gate.calls.lock().expect("measurement mutex"));
    if calls.len() != completed.len()
        || completed.iter().any(|run| calls.get(&run.id) != Some(&1))
        || gate.active.load(Ordering::SeqCst) != 0
        || gate.permits.available_permits() != 0
    {
        return Err("Accepted runs were lost, repeated or remained active".into());
    }
    harness.empty_and_ready().await?;
    Ok((completed, rejected, held))
}

async fn saturation(harness: &Harness, gate: &Gate, concurrency: usize) -> Measurement<Value> {
    let capacity = 3 * concurrency;
    let mut samples = Samples::default();
    for start in (0..100).step_by(capacity) {
        let (completed, _, _) = gated_batch(
            harness,
            gate,
            (100 - start).min(capacity),
            concurrency,
            false,
        )
        .await?;
        samples.record(completed, false)?;
    }
    let started = Instant::now();
    let mut held = Duration::ZERO;
    let mut cycles = 0;
    while samples.complete.len() < 1000 || held < Duration::from_secs(30) {
        let (completed, rejected, duration) =
            gated_batch(harness, gate, capacity, concurrency, true).await?;
        samples.record(completed, true)?;
        samples.rejection.extend(rejected);
        held += duration;
        cycles += 1;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let throughput = samples.complete.len() as f64 / elapsed;
    let mut report = samples.report();
    report["cycles"] = json!(cycles);
    report["seconds"] = json!(elapsed);
    report["verified_full_seconds"] = json!(held.as_secs_f64());
    report["minimum_hold_ms_per_cycle"] = json!(100);
    report["runs_per_second_including_gate"] = json!(throughput);
    report["max_operation_concurrency"] = json!(gate.peak.load(Ordering::SeqCst));
    report["admission_capacity"] = json!(capacity);
    Ok(report)
}

fn resident_bytes() -> Measurement<u64> {
    if !cfg!(any(target_os = "macos", target_os = "linux")) {
        return Err("RSS probe is defined for macOS/Linux only".into());
    }
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    if !output.status.success() {
        return Err("Operating system denied the RSS probe".into());
    }
    let kib: u64 = std::str::from_utf8(&output.stdout)?.trim().parse()?;
    let bytes = kib.checked_mul(1024).ok_or("RSS overflow")?;
    if bytes == 0 {
        return Err("Operating system returned empty RSS".into());
    }
    Ok(bytes)
}
async fn expire(harness: &Harness, ids: &[RunId]) -> Measurement<()> {
    tokio::time::sleep(Duration::from_millis(2050)).await;
    for id in ids {
        match harness.app.status(access(), id.clone()).await {
            Err(error) if error.code() == "not_found" => (),
            _ => return Err("A result remained available after retention expired".into()),
        }
    }
    harness.empty_and_ready().await
}
async fn release_cycle(
    harness: &Harness,
    concurrency: usize,
    samples: &mut Samples,
    measured: bool,
) -> Measurement<Vec<RunId>> {
    let mut ids = Vec::with_capacity(100);
    for start in (0..100).step_by(concurrency) {
        let completed = harness.complete((100 - start).min(concurrency)).await?;
        ids.extend(samples.record(completed, measured)?);
    }
    Ok(ids)
}
async fn release(harness: &Harness, concurrency: usize, cycles: usize) -> Measurement<Value> {
    let mut samples = Samples::default();
    let warmup_ids = release_cycle(harness, concurrency, &mut samples, false).await?;
    expire(harness, &warmup_ids).await?;
    drop(warmup_ids);
    let baseline_heap = measurement_heap::statistics();
    let baseline = resident_bytes()?;
    let mut series = Vec::with_capacity(cycles);
    let mut execution_seconds = 0.0;
    for cycle in 1..=cycles {
        let started = Instant::now();
        let ids = release_cycle(harness, concurrency, &mut samples, true).await?;
        let seconds = started.elapsed().as_secs_f64();
        execution_seconds += seconds;
        let retained_heap = measurement_heap::statistics();
        let retained = resident_bytes()?;
        expire(harness, &ids).await?;
        let expired = ids.len();
        drop(ids);
        let expired_heap = measurement_heap::statistics();
        series.push(json!({"cycle":cycle,"verified_expired_runs":expired,
            "execution_seconds":seconds,"before_expiry_rss_bytes":retained,
            "after_expiry_rss_bytes":resident_bytes()?,
            "before_expiry_heap":retained_heap,"after_expiry_heap":expired_heap}));
    }
    let after: Vec<_> = series
        .iter()
        .map(|row| row["after_expiry_rss_bytes"].as_u64().expect("RSS integer"))
        .collect();
    let low = *after.iter().min().expect("at least ten cycles");
    let high = *after.iter().max().expect("at least ten cycles");
    let mut report = samples.report();
    report["cycles"] = json!(cycles);
    report["result_observation_method"] =
        json!("per-task start -> wait -> validate; join completed runs after observation");
    report["runs_per_second_excluding_expiry_probes"] =
        json!((cycles * 100) as f64 / execution_seconds);
    report["rss_method"] = json!("ps -o rss= -p SELF_PID; KiB * 1024; runtime remains alive");
    report["baseline_after_warmup_rss_bytes"] = json!(baseline);
    report["baseline_after_warmup_heap"] = baseline_heap;
    report["heap_method"] = json!(
        "macOS: malloc_zone_statistics(NULL, stats), all zones; other platforms: null; not RSS"
    );
    report["rss_range_growth_percent"] = json!((high as f64 / low as f64 - 1.0) * 100.0);
    report["rss_final_vs_warmup_percent"] =
        json!((*after.last().expect("at least ten cycles") as f64 / baseline as f64 - 1.0) * 100.0);
    report["rss_series"] = json!(series);
    report["verified_expired_runs"] = json!(samples.identities.len());
    report["ready_after_expiry"] = json!(harness.app.is_ready());
    // The measurement itself retains IDs and latency vectors across cycles.
    // Release those before the final live-instance probe; the compact report stays.
    drop(samples);
    report["after_measurement_metadata_release_heap"] = measurement_heap::statistics();
    report["after_measurement_metadata_release_rss_bytes"] = json!(resident_bytes()?);
    Ok(report)
}

#[tokio::main]
async fn main() -> Measurement<()> {
    let options = Options::parse()?;
    let providers = measurement_store::MeasurementStore::new(options.sqlite.as_deref())?;
    let limits = Limits {
        active_runs: options.concurrency,
        pending_runs: 2 * options.concurrency,
        concurrent_attempts: options.concurrency,
        terminal_runs: if options.saturation { 128 } else { 64 },
        retention_ms: if options.saturation { 3_600_000 } else { 2000 },
        ..Limits::default()
    };
    let mut builder = WorkflowBuilder::standard()
        .limits(limits.clone())
        .execution_store(providers.execution.clone())
        .artifact_store(providers.artifacts);
    let gate = Arc::new(Gate::new());
    if options.saturation {
        builder.register_bundle(controlled_read(gate.clone()))?;
    }
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let outcome: Measurement<Value> = async {
        let plan = app
            .prepare(access(), definition(options.saturation)?)
            .await?;
        let harness = Harness {
            app,
            plan,
            store: providers.execution,
            payload: json!("x".repeat(options.bytes - 2)),
            durable: options.sqlite.is_some(),
            run_bytes: limits.run_bytes,
        };
        let measurements = if options.saturation {
            saturation(&harness, &gate, options.concurrency).await?
        } else {
            release(&harness, options.concurrency, options.cycles).await?
        };
        Ok(
            json!({"mode":if options.saturation {"saturation"} else {"release"},
            "profile":providers.profile,"provider_options":providers.provider_options,
            "concurrency":options.concurrency,"json_bytes":options.bytes,
            "limits":limits,"measurements":measurements}),
        )
    }
    .await;
    // Release blocked test operations even if a probe failed, then drain the same runtime.
    gate.permits.add_permits(3 * options.concurrency);
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
    let report = outcome?;
    let shutdown = shutdown?;
    if shutdown.forced || !shutdown.pending.is_empty() {
        return Err("Measurement failed to drain".into());
    }
    println!("{report}");
    Ok(())
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "../tests/support/directory.rs"]
mod test_directory;

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;

    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn sqlite_release_batch_observes_results_before_expiry() {
        let directory = Directory(test_directory::create("workflow-forge-capacity-release"));
        let path = directory.0.join("state.sqlite");
        let providers = measurement_store::MeasurementStore::new(Some(&path)).unwrap();
        let limits = Limits {
            active_runs: 8,
            pending_runs: 16,
            concurrent_attempts: 8,
            terminal_runs: 64,
            retention_ms: 2000,
            ..Limits::default()
        };
        let composition = WorkflowBuilder::standard()
            .limits(limits.clone())
            .execution_store(providers.execution.clone())
            .artifact_store(providers.artifacts)
            .build()
            .unwrap();
        let runtime = EngineRuntime::boot(composition, BootOptions::default())
            .await
            .unwrap();
        let app = runtime.application();
        let outcome = tokio::time::timeout(Duration::from_secs(10), async {
            let plan = app.prepare(access(), definition(false)?).await?;
            let harness = Harness {
                app,
                plan,
                store: providers.execution,
                payload: json!({"probe":"release","value":42}),
                durable: true,
                run_bytes: limits.run_bytes,
            };
            // Exercise the release path with real durable acceptance and result reads.
            // This smoke test does not force skew between individual acceptances.
            let completed = harness.complete(8).await?;
            assert_eq!(completed.len(), 8);
            let mut samples = Samples::default();
            let ids = samples.record(completed, false)?;
            assert_eq!(samples.identities.len(), 8);
            harness.empty_and_ready().await?;
            expire(&harness, &ids).await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await;
        let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
        outcome
            .expect("SQLite release batch exceeded ten seconds")
            .expect("SQLite release batch must complete and expire correctly");
        let shutdown = shutdown.unwrap();
        assert!(!shutdown.forced);
        assert!(shutdown.pending.is_empty());
    }
}
