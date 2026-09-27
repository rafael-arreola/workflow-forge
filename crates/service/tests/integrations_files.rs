mod support;
use serde_json::{Value, json};
use std::sync::Arc;
use support::*;
use workflow_forge::v2::*;
use workflow_forge_service::ServiceRuntime;

fn operation_definition(id: &str, operation: OperationRevision) -> WorkflowDefinition {
    single(
        id,
        json!({"id":"operation","kind":"operation","operation":operation,"config":{},"input":{"select":{"source":"input","pointer":""}}}),
    )
}
async fn execute(
    service: &ServiceRuntime,
    definition: &WorkflowDefinition,
    input: Value,
) -> RunSnapshot {
    let mut body = start_body(definition, input, false);
    body["options"]["receipt_key"] = json!(uuid::Uuid::now_v7().to_string());
    let receipt: StartReceipt =
        serde_json::from_value(post(service, "/v2/runs", body, 202).await).unwrap();
    wait(service, receipt.run_id).await
}

#[tokio::test]
async fn file_artifact_and_csv_batches_preserve_quoting_lines_and_generic_fields() {
    let directory = Directory::new();
    std::fs::write(
        directory.0.join("input.csv"),
        b"sku,quantity\n\"A,1\",2\n\"multi\nline\",3\nbad,2,extra\n",
    )
    .unwrap();
    let file = modules::file_operations(vec![modules::FileReadProfile {
        name: "source".into(),
        root: directory.0.clone(),
        media_type: "text/csv".into(),
        ..Default::default()
    }])
    .unwrap();
    let csv = modules::csv_operations(Default::default()).unwrap();
    let read = operation_definition("file", file.operations[0].descriptor().revision.clone());
    let batch = operation_definition("csv", csv.operations[0].descriptor().revision.clone());
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(file).unwrap();
    builder.register_bundle(csv).unwrap();
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(vec![read.clone(), batch.clone()]),
    )
    .await
    .unwrap();
    let loaded = execute(&service, &read, json!({"path":"input.csv"})).await;
    assert_eq!(loaded.state, RunState::Succeeded, "{:?}", loaded.error);
    let source = loaded.output.unwrap();
    let first = execute(&service, &batch, json!({"source":source,"size":2})).await;
    assert_eq!(first.state, RunState::Succeeded, "{:?}", first.error);
    let first = first.output.unwrap();
    assert_eq!(first["headers"], json!(["sku", "quantity"]));
    assert_eq!(
        first["rows"][0],
        json!({"index":0,"line":2,"fields":["A,1","2"],"valid_columns":true})
    );
    assert_eq!(
        first["rows"][1],
        json!({"index":1,"line":3,"fields":["multi\nline","3"],"valid_columns":true})
    );
    assert_eq!(first["next_offset"], 2);
    let second = execute(
        &service,
        &batch,
        json!({"source":source,"size":2,"offset":2}),
    )
    .await
    .output
    .unwrap();
    assert_eq!(
        second["rows"][0],
        json!({"index":2,"line":5,"fields":["bad","2","extra"],"valid_columns":false})
    );
    assert_eq!(second["next_offset"], Value::Null);
    // The original file can change after publication; confirmed artifact bytes do not.
    std::fs::write(directory.0.join("input.csv"), b"different\nsource\n").unwrap();
    assert_eq!(
        execute(&service, &batch, json!({"source":source,"size":2}))
            .await
            .output
            .unwrap(),
        first
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_scope_and_byte_limits_reject_escape_without_publishing_partial_content() {
    let directory = Directory::new();
    let outside = Directory::new();
    std::fs::write(directory.0.join("large"), vec![0u8; 33]).unwrap();
    std::fs::write(outside.0.join("outside"), b"private host bytes").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(outside.0.join("outside"), directory.0.join("link")).unwrap();
    let profile = modules::FileReadProfile {
        name: "source".into(),
        root: directory.0.clone(),
        max_bytes: 32,
        ..Default::default()
    };
    let bundle = modules::file_operations(vec![profile.clone()]).unwrap();
    let revision = bundle.operations[0].descriptor().revision.clone();
    let mut changed = profile;
    changed.root = outside.0.clone();
    assert_ne!(
        revision,
        modules::file_operations(vec![changed]).unwrap().operations[0]
            .descriptor()
            .revision
    );
    let definition = operation_definition("file", revision);
    let store = directory.store(modules::SqliteOptions {
        max_artifacts: 1,
        ..Default::default()
    });
    let mut builder = WorkflowBuilder::standard()
        .execution_store(store.clone())
        .artifact_store(store);
    builder.register_bundle(bundle).unwrap();
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    let mut paths = vec![
        "../outside".to_owned(),
        outside.0.join("outside").to_str().unwrap().into(),
        "large".into(),
        ".".into(),
    ];
    #[cfg(unix)]
    paths.push("link".into());
    for path in paths {
        let run = execute(&service, &definition, json!({"path":path})).await;
        assert_eq!(run.state, RunState::Failed);
        assert!(run.output.is_none());
        assert!(
            !serde_json::to_string(&run.error)
                .unwrap()
                .contains("private host bytes")
        );
    }
    std::fs::write(directory.0.join("allowed"), b"ok").unwrap();
    let run = execute(&service, &definition, json!({"path":"allowed"})).await;
    assert_eq!(run.state, RunState::Succeeded, "{:?}", run.error);
    assert_eq!(run.output.unwrap()["bytes"], 2);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn csv_rejects_invalid_encoding_headers_and_quotas_before_returning_a_batch() {
    let artifacts = Arc::new(modules::MemoryArtifacts::default());
    let csv = modules::csv_operations(modules::CsvOptions {
        max_rows: 2,
        max_columns: 2,
        max_field_bytes: 16,
        max_bytes: 64,
        max_batch: 2,
    })
    .unwrap();
    let definition = operation_definition("csv", csv.operations[0].descriptor().revision.clone());
    let mut builder = WorkflowBuilder::standard().artifact_store(artifacts);
    builder.register_bundle(csv).unwrap();
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    for bytes in [
        vec![],
        b"a,a\n1,2\n".to_vec(),
        b"a,b,c\n1,2,3\n".to_vec(),
        b"a,b\n\xff,1\n".to_vec(),
        b"a,b\n1,1\n2,2\n3,3\n".to_vec(),
        vec![b'a'; 65],
        format!("a,b\n{},2\n", "x".repeat(17)).into_bytes(),
    ] {
        let source = service
            .application()
            .write_artifact(
                access(),
                Box::pin(futures::stream::once(async { Ok(bytes) })),
                "text/csv",
            )
            .await
            .unwrap();
        let run = execute(&service, &definition, json!({"source":source,"size":1})).await;
        assert_eq!(run.state, RunState::Failed, "{:?}", run.output);
    }
    let source = service
        .application()
        .write_artifact(
            access(),
            Box::pin(futures::stream::once(async { Ok(b"a,b\n".to_vec()) })),
            "text/csv",
        )
        .await
        .unwrap();
    let empty = execute(&service, &definition, json!({"source":source})).await;
    assert_eq!(
        empty.output,
        Some(json!({"headers":["a","b"],"rows":[],"next_offset":null}))
    );
    let past = execute(&service, &definition, json!({"source":source,"offset":1})).await;
    assert_eq!(past.state, RunState::Failed);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_file_and_csv_operations_do_not_mix_concurrent_workflow_data() {
    let directory = Directory::new();
    for index in 0..16 {
        std::fs::write(
            directory.0.join(format!("{index}.csv")),
            format!("id\n{index}\n"),
        )
        .unwrap();
    }
    let files = modules::file_operations(vec![modules::FileReadProfile {
        name: "source".into(),
        root: directory.0.clone(),
        ..Default::default()
    }])
    .unwrap();
    let csv = modules::csv_operations(Default::default()).unwrap();
    let file_revision = files.operations[0].descriptor().revision.clone();
    let csv_revision = csv.operations[0].descriptor().revision.clone();
    let definition:WorkflowDefinition=serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":"file.csv.concurrent","revision":"r1","input_schema":true,"output_schema":true,"entry":"read",
        "nodes":[
            {"id":"read","kind":"operation","operation":file_revision,"config":{},"input":{"select":{"source":"input","pointer":""}}},
            {"id":"csv","kind":"operation","operation":csv_revision,"config":{},"input":{"object":{"source":{"select":{"source":"node","node":"read","pointer":""}}}}}
        ],"edges":[{"from":"read","to":"csv"}],"output":{"select":{"source":"node","node":"csv","pointer":"/rows/0/fields/0"}}
    })).unwrap();
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(files).unwrap();
    builder.register_bundle(csv).unwrap();
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    let runs = (0..16).map(|index| {
        let (service, definition) = (&service, &definition);
        async move {
            (
                index,
                execute(service, definition, json!({"path":format!("{index}.csv")})).await,
            )
        }
    });
    for (index, run) in futures::future::join_all(runs).await {
        assert_eq!(
            run.output,
            Some(json!(index.to_string())),
            "{:?}",
            run.error
        );
    }
    service.shutdown().await.unwrap();
}
