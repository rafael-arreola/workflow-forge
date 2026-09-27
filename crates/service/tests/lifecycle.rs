mod support;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use workflow_forge::v2::*;
use workflow_forge_service::ServiceRuntime;
#[path = "support/failing_store.rs"]
mod failing_store;

#[tokio::test]
async fn engine_failure_removes_readiness_and_supervises_transport_cleanup() {
    let store = Arc::new(failing_store::FailingStore::default());
    let mut service = ServiceRuntime::boot(
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap(),
        auth(),
        options(vec![]),
    )
    .await
    .unwrap();
    let app = service.application();
    let address = service.local_addr();
    get(&service, "/health/ready", 200).await;
    store.fail.store(true, Ordering::SeqCst);
    let error = tokio::time::timeout(Duration::from_secs(3), service.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.diagnostics.iter().any(|d| d.code == "runtime.failed"));
    assert!(!service.is_ready() && !app.is_ready());
    assert!(TcpStream::connect(address).await.is_err());
    store.inner.claim("probe").await.unwrap();
    store.inner.release("probe").await.unwrap();
}

#[tokio::test]
async fn bind_and_scope_failure_release_the_store_and_old_handles_stay_stopped() {
    let store = Arc::new(modules::MemoryExecutionStore::default());
    let build = || {
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap()
    };
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut opts = options(vec![]);
    opts.listen = occupied.local_addr().unwrap();
    assert_eq!(
        ServiceRuntime::boot(build(), auth(), opts)
            .await
            .err()
            .unwrap()
            .code(),
        "service.bind"
    );
    let mut opts = options(vec![]);
    opts.scope = "wrong".into();
    assert_eq!(
        ServiceRuntime::boot(build(), auth(), opts)
            .await
            .err()
            .unwrap()
            .code(),
        "access.denied"
    );
    let service = ServiceRuntime::boot(build(), auth(), options(vec![]))
        .await
        .unwrap();
    let old = service.application();
    assert!(old.is_ready());
    service.shutdown().await.unwrap();
    let new = ServiceRuntime::boot(build(), auth(), options(vec![]))
        .await
        .unwrap();
    assert!(!old.is_ready());
    assert_eq!(
        old.prepare(access(), echo()).await.err().unwrap().code(),
        "runtime.unavailable"
    );
    assert!(new.is_ready());
    new.shutdown().await.unwrap();
}

struct Paused {
    descriptor: OperationDescriptor,
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
}
impl Operation for Paused {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
            Ok(OperationOutput::json(invocation.input))
        })
    }
}
#[tokio::test]
async fn disconnect_after_acceptance_does_not_cancel_or_duplicate_execution() {
    let mut descriptor = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    descriptor.revision = OperationRevision::new("test.paused", "1", "r1");
    let operation = Arc::new(Paused {
        descriptor,
        entered: Notify::new(),
        release: Notify::new(),
        calls: AtomicUsize::new(0),
    });
    let mut builder = WorkflowBuilder::standard();
    builder
        .register_bundle(OperationBundle {
            module: ModuleDescriptor {
                id: "test.paused".into(),
                version: "1".into(),
                protocol_version: PROTOCOL_VERSION,
                exports: vec![operation.descriptor.revision.clone()],
            },
            operations: vec![operation.clone()],
            inspectors: vec![],
        })
        .unwrap();
    let definition = single(
        "disconnect",
        json!({"id":"wait","kind":"operation","operation":{"id":"test.paused","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}}}),
    );
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    let body = start_body(&definition, json!({"answer":42}), false);
    let bytes = serde_json::to_vec(&body).unwrap();
    let mut connection = TcpStream::connect(service.local_addr()).await.unwrap();
    let head = format!(
        "POST /v2/runs HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        bytes.len()
    );
    connection.write_all(head.as_bytes()).await.unwrap();
    connection.write_all(&bytes).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), operation.entered.notified())
        .await
        .unwrap();
    drop(connection); // Never read the acceptance acknowledgement.
    let receipt: StartReceipt =
        serde_json::from_value(post(&service, "/v2/runs", body, 202).await).unwrap();
    assert!(receipt.duplicate);
    operation.release.notify_one();
    let run = wait(&service, receipt.run_id).await;
    assert_eq!(run.output, Some(json!({"answer":42})));
    assert!(!run.cancel_requested);
    assert_eq!(operation.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_host_stops_readiness_and_listener_and_releases_store() {
    let store = Arc::new(modules::MemoryExecutionStore::default());
    let build = || {
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .build()
            .unwrap()
    };
    let service = ServiceRuntime::boot(build(), auth(), options(vec![]))
        .await
        .unwrap();
    let old = service.application();
    let address = service.local_addr();
    get(&service, "/v2/catalog", 200).await; // Ensure supervisor has started.
    drop(service);
    tokio::time::timeout(Duration::from_secs(3), async {
        while old.is_ready() {
            tokio::task::yield_now().await;
        }
        loop {
            match EngineRuntime::boot(build(), BootOptions::default()).await {
                Ok(engine) => {
                    engine.shutdown(ShutdownOptions::default()).await.unwrap();
                    break;
                }
                Err(error) => {
                    assert_eq!(error.code(), "state.conflict");
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(TcpStream::connect(address).await.is_err());
}
