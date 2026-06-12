//! Execution-level deadline and cooperative cancellation via `run_with`.
//! Both are opt-in: `run` (and `run_with` with default options) is unlimited.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use workflow_forge_core::error::codes;
use workflow_forge_core::runtime::{CancellationToken, RunOptions, WorkflowExecutor};
use workflow_forge_core::task::{TaskRegistry, WorkflowData};

/// start → slow → end, where `slow` sleeps for `sleep_ms`.
fn registry_with_slow(sleep_ms: u64) -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    registry.register_fn("test.slow", move |_ctx, input| async move {
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        Ok(input)
    });
    registry
}

fn workflow() -> Value {
    json!({
        "name": "slow", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "slow",  "kind": "task", "task": "test.slow" },
            { "id": "end",   "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "slow" },
            { "from": "slow",  "to": "end" }
        ]
    })
}

fn executor(sleep_ms: u64) -> WorkflowExecutor {
    WorkflowExecutor::new(
        serde_json::from_value(workflow()).unwrap(),
        registry_with_slow(sleep_ms),
    )
    .unwrap()
}

#[tokio::test]
async fn unlimited_by_default() {
    // A short task completes normally with plain `run` (no limits).
    let out = executor(10)
        .run(WorkflowData(json!({ "ok": 1 })))
        .await
        .unwrap();
    assert_eq!(out.0, json!({ "ok": 1 }));
}

#[tokio::test]
async fn deadline_aborts_a_long_run() {
    let started = Instant::now();
    let err = executor(10_000)
        .run_with(
            WorkflowData(json!({})),
            RunOptions::default().deadline(Duration::from_millis(50)),
        )
        .await
        .expect_err("debe vencer el deadline");

    assert_eq!(err.code, codes::EXECUTION_TIMEOUT);
    // Returned promptly, not after the 10s task.
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn cancellation_token_aborts_a_long_run() {
    let token = CancellationToken::new();
    let trigger_cancel = token.clone();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        trigger_cancel.cancel();
    });

    let err = executor(10_000)
        .run_with(WorkflowData(json!({})), RunOptions::default().cancel(token))
        .await
        .expect_err("debe cancelarse");

    canceller.await.unwrap();
    assert_eq!(err.code, codes::EXECUTION_CANCELLED);
}
