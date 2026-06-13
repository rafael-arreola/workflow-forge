use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_json::json;
use std::sync::Arc;
use workflow_forge_core::prelude::*;

fn bench_linear_workflow(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "linear", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [{ "from": "start", "to": "end" }]
    }))
    .unwrap();

    let registry = Arc::new(TaskRegistry::new());
    let executor = WorkflowExecutor::new(workflow, registry).unwrap();

    c.bench_function("linear_noop_workflow", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = executor
                .run(WorkflowData(black_box(json!({"n": 1}))))
                .await
                .unwrap();
        });
    });
}

criterion_group!(benches, bench_linear_workflow);
criterion_main!(benches);
