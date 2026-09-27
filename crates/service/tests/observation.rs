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
use workflow_forge::v2::*;
use workflow_forge_service::ServiceRuntime;

struct Faulty {
    mode: &'static str,
    called: AtomicUsize,
}
impl ExecutionObserver for Faulty {
    fn observe(&self, _: ExecutionEvent) -> PortFuture<'_, ()> {
        self.called.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            match self.mode {
                "slow" => std::future::pending().await,
                "panic" => panic!("injected observer panic"),
                _ => Err(ForgeError::new(
                    "test.observer",
                    "injected observer failure",
                )),
            }
        })
    }
}
#[tokio::test]
async fn observer_failure_timeout_and_panic_do_not_change_execution_or_shutdown() {
    for mode in ["slow", "error", "panic"] {
        let observer = Arc::new(Faulty {
            mode,
            called: AtomicUsize::new(0),
        });
        let definition = echo();
        let service = ServiceRuntime::boot(
            WorkflowBuilder::standard()
                .observer(observer.clone())
                .build()
                .unwrap(),
            auth(),
            options(vec![definition.clone()]),
        )
        .await
        .unwrap();
        let starts = (0..32).map(|index| {
            let mut body = start_body(&definition, json!(index), false);
            body["options"]["receipt_key"] = json!(format!("request-{index}"));
            let service = &service;
            async move {
                (
                    index,
                    serde_json::from_value::<StartReceipt>(
                        post(service, "/v2/runs", body, 202).await,
                    )
                    .unwrap(),
                )
            }
        });
        let receipts = futures::future::join_all(starts).await;
        for (index, receipt) in receipts {
            assert_eq!(
                wait(&service, receipt.run_id).await.output,
                Some(json!(index))
            );
        }
        assert!(observer.called.load(Ordering::SeqCst) > 0);
        let report = tokio::time::timeout(Duration::from_secs(2), service.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(!report.http_forced && !report.engine.forced);
    }
}
