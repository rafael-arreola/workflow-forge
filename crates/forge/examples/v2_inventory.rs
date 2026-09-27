//! C-02 host and measurement: rows (1..10000), independent samples (1..10).
//! Optional: --sqlite DIRECTORY; creates a new sample-N.sqlite per composition.
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{sync::Arc, time::Instant};
use workflow_forge::v2::*;
use workflow_forge_reference_module::inventory::{MemoryInventory, inventory_operations};

#[path = "support/measurement_store.rs"]
mod measurement_store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw: Vec<_> = std::env::args().skip(1).collect();
    let directory = measurement_store::sqlite_path(&raw, 2)?;
    let args: Vec<_> = raw
        .iter()
        .take(2)
        .map(|s| s.parse::<usize>())
        .collect::<Result<_, _>>()?;
    let rows = args.first().copied().unwrap_or(100);
    let samples = args.get(1).copied().unwrap_or(3);
    if !(1..=10000).contains(&rows) || !(1..=10).contains(&samples) {
        return Err("Expected 1..10000 rows and 1..10 independent samples".into());
    }
    let mut observations = Vec::with_capacity(samples);
    for sample in 0..samples {
        let path = directory.map(|dir| dir.join(format!("sample-{sample}.sqlite")));
        observations.push(measure(rows, sample, path.as_deref()).await?);
    }
    println!(
        "{}",
        json!({"rows":rows,"batch_size":100,"row_concurrency":4,"samples":observations,"warmup":0,"profile":{"store":if directory.is_some(){"sqlite-wal-full"}else{"memory"},"artifacts":if directory.is_some(){"sqlite"}else{"memory"},"activations":12000,"run_timeout_ms":900000},"note":"Independent compositions; no percentile or production target inferred from a small sample"})
    );
    Ok(())
}

async fn measure(
    rows: usize,
    sample: usize,
    path: Option<&std::path::Path>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut csv = "sku,quantity\n".to_owned();
    for index in 0..rows {
        csv.push_str(&format!("SKU-{index},{}\n", index + 1));
    }
    let source_bytes = csv.len();
    let providers = measurement_store::MeasurementStore::new(path)?;
    let artifacts = providers.artifacts;
    let destination = Arc::new(MemoryInventory::default());
    let mut builder = WorkflowBuilder::standard()
        .execution_store(providers.execution)
        .artifact_store(artifacts.clone())
        .limits(Limits {
            activations: 12_000,
            run_timeout_ms: 900_000,
            ..Default::default()
        });
    builder.register_bundle(inventory_operations(destination.clone()))?;
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let result = async {
        let source = artifacts.write("default",Box::pin(stream::once(async move { Ok(csv.into_bytes()) })),"text/csv").await?;
        let preparation = Instant::now();
        let plan = app.prepare_json(access.clone(),include_bytes!("../../../examples/workflows/inventory_import.v2.json")).await?;
        let prepare_us = preparation.elapsed().as_secs_f64()*1_000_000.0;
        let execution = Instant::now();
        let mut request = StartRunRequest::new(plan,json!({"source":source}));
        request.options.artifacts = vec![source];
        request.options.require_durable = path.is_some();
        let accepted = app.start(access.clone(),request).await?;
        let run = app.wait(access,accepted.run_id).await?;
        let seconds = execution.elapsed().as_secs_f64();
        if run.state!=RunState::Succeeded {
            return Err(run.error.unwrap_or_else(||ForgeError::new("measurement.failed","Import did not succeed")));
        }
        let output = run.output.ok_or_else(||ForgeError::new("measurement.failed","Import has no result"))?;
        if output["rows"]!=json!(rows) || output["succeeded"]!=json!(rows) || output["failed"]!=json!(0) {
            return Err(ForgeError::new("measurement.failed","Summary does not match the input"));
        }
        let report:ArtifactRef = serde_json::from_value(output["report"].clone()).map_err(|_|ForgeError::new("measurement.failed","Report reference is invalid"))?;
        let report_bytes = report.bytes;
        let mut content = artifacts.read(&report).await?;
        let mut buffer = Vec::new();
        let mut verified_rows = 0;
        while let Some(chunk) = content.next().await {
            buffer.extend(chunk?);
            while let Some(end) = buffer.iter().position(|b|*b==b'\n') {
                let row:Value = serde_json::from_slice(&buffer[..end]).map_err(|_|ForgeError::new("measurement.failed","Report row is invalid"))?;
                if row["index"]!=json!(verified_rows) || row["output"]["index"]!=json!(verified_rows) || row["status"]!="succeeded" {
                    return Err(ForgeError::new("measurement.failed","Report row order or result is incorrect"));
                }
                verified_rows += 1;
                buffer.drain(..=end);
            }
        }
        let observed = destination.snapshot()?;
        if !buffer.is_empty() || verified_rows!=rows || observed.effects.len()!=rows || observed.attempts!=rows {
            return Err(ForgeError::new("measurement.failed","Rows or destination effects do not match"));
        }
        Ok::<_,ForgeError>(json!({"sample":sample,"profile":providers.profile,"source_bytes":source_bytes,"report_bytes":report_bytes,"prepare_us":prepare_us,"execute_seconds":seconds,"rows_per_second":rows as f64/seconds,"activations":run.invocations.len(),"transitions":run.revision,"verified_rows":verified_rows,"destination_effects":observed.effects.len()}))
    }.await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
    let result = result?;
    shutdown?;
    Ok(result)
}
