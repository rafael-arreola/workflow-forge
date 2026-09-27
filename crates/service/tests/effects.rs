mod support;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use support::*;
use workflow_forge::v2::*;
use workflow_forge_service::{ServiceRuntime, dto::*};

struct Uncertain {
    descriptor: OperationDescriptor,
    calls: AtomicUsize,
}
impl Operation for Uncertain {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, _: Invocation) -> OperationFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(OperationError {
                code: "test.uncertain".into(),
                class: ErrorClass::Transient,
                certainty: EffectCertainty::Unknown,
                message: "private adapter payload must not reach HTTP".into(),
            })
        })
    }
}
impl EffectInspector for Uncertain {
    fn operation(&self) -> &OperationRevision {
        &self.descriptor.revision
    }
    fn inspect<'a>(
        &'a self,
        _: OperationContext,
        invocation: Invocation,
    ) -> PortFuture<'a, EffectInspection> {
        Box::pin(async move {
            Ok(EffectInspection::Applied {
                output: invocation.input,
                evidence: EffectEvidence {
                    authority: "test.ledger".into(),
                    reference: invocation.effect_key.unwrap(),
                    note: "Operator evidence".into(),
                },
            })
        })
    }
}

#[tokio::test]
async fn uncertain_effect_is_inspected_and_reconciled_once_with_redacted_metadata() {
    let mut descriptor = modules::data_operations().operations[0]
        .descriptor()
        .clone();
    descriptor.revision = OperationRevision::new("test.uncertain", "1", "r1");
    descriptor.effect = EffectKind::Write;
    descriptor.repetition = Repetition::Unsafe;
    descriptor.reconciliation = true;
    let operation = Arc::new(Uncertain {
        descriptor,
        calls: AtomicUsize::new(0),
    });
    let mut builder = WorkflowBuilder::standard();
    builder
        .register_bundle(OperationBundle {
            module: ModuleDescriptor {
                id: "test.uncertain".into(),
                version: "1".into(),
                protocol_version: PROTOCOL_VERSION,
                exports: vec![operation.descriptor.revision.clone()],
            },
            operations: vec![operation.clone()],
            inspectors: vec![operation.clone()],
        })
        .unwrap();
    let definition = single(
        "uncertain",
        json!({"id":"write","kind":"operation","operation":{"id":"test.uncertain","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}}}),
    );
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        auth(),
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    let receipt: StartReceipt = serde_json::from_value(
        post(
            &service,
            "/v2/runs",
            start_body(&definition, json!({"output":42}), false),
            202,
        )
        .await,
    )
    .unwrap();
    let blocked = state(&service, &receipt.run_id, RunState::Blocked).await;
    let path = format!("/v2/runs/{}", receipt.run_id.0);
    let metadata = get(&service, &path, 200).await;
    assert!(!metadata.to_string().contains("private adapter payload"));
    let records: Page<InvocationStatus> =
        serde_json::from_value(get(&service, &format!("{path}/invocations"), 200).await).unwrap();
    let record = &records.items[0];
    assert_eq!(record.certainty, EffectCertainty::Unknown);
    assert_eq!(
        record.error.as_ref().unwrap().diagnostics[0]
            .operation_error
            .as_ref()
            .unwrap()
            .code,
        "test.uncertain"
    );
    let inspected: EffectInspection = serde_json::from_value(
        post(
            &service,
            &format!("{path}/effects/inspect"),
            json!({"invocation_id":record.id}),
            200,
        )
        .await,
    )
    .unwrap();
    let EffectInspection::Applied {
        output,
        mut evidence,
    } = inspected
    else {
        panic!("ledger should confirm effect");
    };
    evidence.note = "private operator note".into();
    let command = ReconcileCommand {
        command_id: "resolve-1".into(),
        run_id: receipt.run_id.clone(),
        invocation_id: record.id.clone(),
        expected_revision: blocked.revision,
        observed_attempt: record.attempt_id.clone(),
        resolution: EffectResolution::ConfirmApplied { output, evidence },
    };
    let response = post(
        &service,
        &format!("{path}/effects/reconcile"),
        json!(command),
        200,
    )
    .await;
    assert_eq!(
        wait(&service, receipt.run_id.clone()).await.output,
        Some(json!({"output":42}))
    );
    assert_eq!(
        post(
            &service,
            &format!("{path}/effects/reconcile"),
            json!(command),
            200
        )
        .await,
        response
    );
    let audit = get(&service, &format!("{path}/audit"), 200).await;
    assert!(!audit.to_string().contains("private operator note"));
    let page: Page<AuditStatus> = serde_json::from_value(audit).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].command_id.as_deref(), Some("resolve-1"));
    assert_eq!(operation.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.unwrap();
}
