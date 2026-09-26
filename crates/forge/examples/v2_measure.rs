//! Reproducible local baseline; arguments: nodes, JSON bytes, samples, concurrency.
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Instant};
use workflow_forge::v2::*;

fn percentile(values: &mut [u128]) -> Value {
    values.sort_unstable();
    let n = values.len() - 1;
    json!({"p50_us":values[n/2] as f64/1000.0,"p95_us":values[n*95/100] as f64/1000.0,"p99_us":values[n*99/100] as f64/1000.0})
}
fn definition(count: usize) -> WorkflowDefinition {
    let nodes = (0..count)
        .map(|i| NodeDefinition {
            id: format!("n{i}"),
            kind: "operation".into(),
            operation: OperationRevision::new("forge.data.identity", "1", "r1"),
            config: json!({}),
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
    let args: Vec<_> = std::env::args()
        .skip(1)
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
    let runtime =
        EngineRuntime::boot(WorkflowBuilder::standard().build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let definition = definition(count);
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
    // Finish the warmup before measuring, including when concurrency does not divide 100.
    for batch in (0..100).step_by(concurrency) {
        run_batch(
            &app,
            &access,
            &plan,
            &payload,
            (100 - batch).min(concurrency),
        )
        .await?;
    }
    let total_start = Instant::now();
    for batch in (0..samples).step_by(concurrency) {
        for (elapsed, succeeded) in run_batch(
            &app,
            &access,
            &plan,
            &payload,
            (samples - batch).min(concurrency),
        )
        .await?
        {
            execution.push(elapsed);
            failed += usize::from(!succeeded);
        }
    }
    let seconds = total_start.elapsed().as_secs_f64();
    println!(
        "{}",
        json!({"nodes":count,"json_bytes":bytes,"warmup":100,"samples":samples,"concurrency":concurrency,"prepare":percentile(&mut preparation),"execute":percentile(&mut execution),"runs_per_second":samples as f64/seconds,"failed_runs":failed,"profile":"default-memory"})
    );
    runtime.shutdown(ShutdownOptions::default()).await?;
    Ok(())
}

async fn run_batch(
    app: &WorkflowApplication,
    access: &AccessContext,
    plan: &PreparedWorkflow,
    payload: &Value,
    count: usize,
) -> Result<Vec<(u128, bool)>, Box<dyn std::error::Error>> {
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..count {
        let app = app.clone();
        let access = access.clone();
        let plan = plan.clone();
        let input = payload.clone();
        jobs.spawn(async move {
            let start = Instant::now();
            let expected = input.clone();
            let accepted = app
                .start(access.clone(), StartRunRequest::new(plan, input))
                .await?;
            let result = app.wait(access, accepted.run_id).await?;
            Ok::<_, ForgeError>((
                start.elapsed().as_nanos(),
                result.state == RunState::Succeeded && result.output.as_ref() == Some(&expected),
            ))
        });
    }
    let mut results = Vec::with_capacity(count);
    while let Some(outcome) = jobs.join_next().await {
        results.push(outcome??);
    }
    Ok(results)
}
