mod support;
use futures::{StreamExt, stream};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use support::*;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Notify};
use workflow_forge::v2::*;
use workflow_forge_service::ServiceRuntime;

struct ObservedArtifacts {
    inner: Arc<modules::SqliteExecutionStore>,
    first_chunk: Arc<Notify>,
    failed: Notify,
}
impl ArtifactStore for ObservedArtifacts {
    fn durable(&self) -> bool {
        true
    }
    fn host_access(&self) -> bool {
        true
    }
    fn artifact_domain(&self) -> Option<&str> {
        ArtifactStore::artifact_domain(&*self.inner)
    }
    fn write<'a>(
        &'a self,
        scope: &'a str,
        content: ByteStream,
        media: &'a str,
    ) -> PortFuture<'a, ArtifactRef> {
        self.inner.write(scope, content, media)
    }
    fn read<'a>(&'a self, item: &'a ArtifactRef) -> PortFuture<'a, ByteStream> {
        self.inner.read(item)
    }
    fn read_for_host<'a>(
        &'a self,
        owner: &'a str,
        item: &'a ArtifactRef,
    ) -> PortFuture<'a, ByteStream> {
        self.inner.read_for_host(owner, item)
    }
    fn write_for_host<'a>(
        &'a self,
        owner: &'a str,
        scope: &'a str,
        content: ByteStream,
        media: &'a str,
    ) -> PortFuture<'a, ArtifactRef> {
        Box::pin(async move {
            let first = self.first_chunk.clone();
            let content = Box::pin(content.inspect(move |_| first.notify_one()));
            let result = self
                .inner
                .write_for_host(owner, scope, content, media)
                .await;
            if result.is_err() {
                self.failed.notify_one();
            }
            result
        })
    }
}
async fn notified(notification: &Notify) {
    tokio::time::timeout(Duration::from_secs(5), notification.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_removes_partial_uploads_after_disconnect_timeout_and_excess_without_restart() {
    for mode in ["disconnect", "timeout", "oversize"] {
        let directory = Directory::new();
        let store = directory.store(modules::SqliteOptions {
            max_artifacts: 1,
            max_artifact_bytes: 32,
            ..Default::default()
        });
        let artifacts = Arc::new(ObservedArtifacts {
            inner: store.clone(),
            first_chunk: Arc::new(Notify::new()),
            failed: Notify::new(),
        });
        let mut opts = options(vec![]);
        opts.max_artifact_bytes = 16;
        opts.request_timeout = Duration::from_millis(200);
        let service = ServiceRuntime::boot(
            WorkflowBuilder::standard()
                .execution_store(store)
                .artifact_store(artifacts.clone())
                .build()
                .unwrap(),
            auth(),
            opts,
        )
        .await
        .unwrap();
        if mode == "oversize" {
            // Chunked transfer has no content-length and must still be limited.
            let chunks = stream::iter([Ok::<_, std::io::Error>(vec![b'a'; 8]), Ok(vec![b'b'; 9])]);
            value(
                client()
                    .post(url(&service, "/v2/artifacts"))
                    .bearer_auth(TOKEN)
                    .body(reqwest::Body::wrap_stream(chunks))
                    .send()
                    .await
                    .unwrap(),
                413,
            )
            .await;
        } else {
            let mut connection = TcpStream::connect(service.local_addr()).await.unwrap();
            let head = format!(
                "POST /v2/artifacts HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\nContent-Length: 12\r\n\r\nabc"
            );
            connection.write_all(head.as_bytes()).await.unwrap();
            notified(&artifacts.first_chunk).await;
            if mode == "disconnect" {
                drop(connection);
            } else {
                use tokio::io::AsyncReadExt;
                let mut bytes = [0u8; 1024];
                let n = tokio::time::timeout(Duration::from_secs(2), connection.read(&mut bytes))
                    .await
                    .unwrap()
                    .unwrap();
                let response = String::from_utf8_lossy(&bytes[..n]);
                assert!(response.starts_with("HTTP/1.1 503"), "{response}");
            }
        }
        notified(&artifacts.failed).await;
        // max_artifacts=1: a leaked staging row would make this fail.
        let artifact: ArtifactRef = serde_json::from_value(
            value(
                client()
                    .post(url(&service, "/v2/artifacts"))
                    .bearer_auth(TOKEN)
                    .body("ok")
                    .send()
                    .await
                    .unwrap(),
                201,
            )
            .await,
        )
        .unwrap();
        assert_eq!(artifact.bytes, 2);
        let read = client()
            .post(url(&service, "/v2/artifacts/read"))
            .bearer_auth(TOKEN)
            .json(&json!({"artifact":artifact}))
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), 200);
        assert_eq!(read.text().await.unwrap(), "ok");
        service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn host_artifact_handles_cannot_cross_sqlite_owners_and_metadata_is_checked() {
    let directory = Directory::new();
    let store = directory.store(Default::default());
    let build = || {
        WorkflowBuilder::standard()
            .execution_store(store.clone())
            .artifact_store(store.clone())
            .build()
            .unwrap()
    };
    let service = ServiceRuntime::boot(build(), auth(), options(vec![]))
        .await
        .unwrap();
    let old = service.application();
    let reference = old
        .write_artifact(
            access(),
            Box::pin(stream::iter([Ok(vec![7; 100_000])])),
            "application/octet-stream",
        )
        .await
        .unwrap();
    let mut stream = old.read_artifact(access(), &reference).await.unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    service.shutdown().await.unwrap();
    let next = ServiceRuntime::boot(build(), auth(), options(vec![]))
        .await
        .unwrap();
    assert_eq!(
        old.read_artifact(access(), &reference)
            .await
            .err()
            .unwrap()
            .code(),
        "runtime.unavailable"
    );
    assert!(stream.next().await.unwrap().is_err());
    let body = client()
        .post(url(&next, "/v2/artifacts/read"))
        .bearer_auth(TOKEN)
        .json(&json!({"artifact":reference}))
        .send()
        .await
        .unwrap();
    assert_eq!(body.status(), 200);
    assert_eq!(body.bytes().await.unwrap().len(), 100_000);
    let mut incorrect = reference.clone();
    incorrect.bytes += 1;
    post(
        &next,
        "/v2/artifacts/read",
        json!({"artifact":incorrect}),
        404,
    )
    .await;
    let mut extra = json!({"artifact":reference});
    extra["artifact"]["actor"] = json!("host");
    post(&next, "/v2/artifacts/read", extra, 400).await;
    next.shutdown().await.unwrap();
}

struct StalledDownload;
impl ArtifactStore for StalledDownload {
    fn durable(&self) -> bool {
        false
    }
    fn write<'a>(&'a self, _: &'a str, _: ByteStream, _: &'a str) -> PortFuture<'a, ArtifactRef> {
        Box::pin(async {
            Err(ForgeError::new(
                "capability.unsupported",
                "read only fixture",
            ))
        })
    }
    fn read<'a>(&'a self, _: &'a ArtifactRef) -> PortFuture<'a, ByteStream> {
        Box::pin(async {
            Ok(
                Box::pin(stream::once(async { Ok(vec![b'a']) }).chain(stream::pending()))
                    as ByteStream,
            )
        })
    }
}

#[tokio::test]
async fn streaming_response_retains_request_permit_and_forced_shutdown_closes_io() {
    let mut opts = options(vec![]);
    opts.max_requests = 1;
    opts.http_shutdown_timeout = Duration::from_millis(30);
    let service = ServiceRuntime::boot(
        WorkflowBuilder::standard()
            .artifact_store(Arc::new(StalledDownload))
            .build()
            .unwrap(),
        auth(),
        opts,
    )
    .await
    .unwrap();
    let reference = ArtifactRef {
        id: "slow".into(),
        scope: "default".into(),
        bytes: 100,
        media_type: "application/octet-stream".into(),
    };
    let mut response = client()
        .post(url(&service, "/v2/artifacts/read"))
        .bearer_auth(TOKEN)
        .json(&json!({"artifact":reference}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.chunk().await.unwrap().is_some());
    get(&service, "/v2/catalog", 429).await;
    let report = tokio::time::timeout(Duration::from_secs(2), service.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(report.http_forced);
    assert!(response.bytes().await.is_err());
}
