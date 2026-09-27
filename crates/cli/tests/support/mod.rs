#![allow(dead_code)]
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::{Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::process::Command;
use workflow_forge::v2::*;
use workflow_forge_service::{BearerIdentity, ServiceOptions, ServiceRuntime, StaticBearerAuth};

pub const TOKEN: &str = "0123456789abcdef0123456789abcdef";
pub fn access() -> AccessContext {
    AccessContext::trusted("default")
}

pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!("forge-cli-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub fn json(&self, name: &str, value: &impl serde::Serialize) -> String {
        let path = self.0.join(name);
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        path.to_str().unwrap().into()
    }
    pub fn sqlite(&self) -> Arc<modules::SqliteExecutionStore> {
        Arc::new(
            modules::SqliteExecutionStore::open(self.0.join("state.sqlite"), Default::default())
                .unwrap(),
        )
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn command(server: &str, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_forge"));
    command
        .args(["--server", server, "--token-env", "FORGE_CLI_TEST_TOKEN"])
        .args(args)
        .env("FORGE_CLI_TEST_TOKEN", TOKEN)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}
pub async fn execute(server: &str, args: &[&str]) -> Output {
    tokio::time::timeout(Duration::from_secs(15), command(server, args).output())
        .await
        .unwrap()
        .unwrap()
}
pub fn success(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
pub fn failure(output: Output, exit: i32, code: &str) -> ForgeError {
    assert_eq!(output.status.code(), Some(exit), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    let line = output
        .stderr
        .split(|b| *b == b'\n')
        .rfind(|line| !line.is_empty())
        .unwrap();
    let error: ForgeError = serde_json::from_slice(line).unwrap();
    assert_eq!(error.code(), code, "{error:?}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains(TOKEN));
    error
}
pub fn url(service: &ServiceRuntime) -> String {
    format!("http://{}", service.local_addr())
}
pub async fn boot(
    builder: WorkflowBuilder,
    definitions: Vec<WorkflowDefinition>,
) -> ServiceRuntime {
    ServiceRuntime::boot(
        builder.build().unwrap(),
        Arc::new(
            StaticBearerAuth::new(vec![BearerIdentity {
                token: SecretValue::new(TOKEN.into()),
                access: access(),
            }])
            .unwrap(),
        ),
        ServiceOptions {
            listen: ([127, 0, 0, 1], 0).into(),
            definitions,
            engine_shutdown_timeout: Duration::from_secs(1),
            http_shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
pub fn single(id: &str, node: Value) -> WorkflowDefinition {
    serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":id,"revision":"r1",
        "input_schema":true,"output_schema":true,"entry":node["id"],"nodes":[node],"edges":[],
        "output":{"select":{"source":"node","node":node["id"],"pointer":""}}
    }))
    .unwrap()
}
pub fn echo(operation: &str) -> WorkflowDefinition {
    single(
        "cli.echo",
        json!({"id":"echo","kind":"operation","operation":{"id":operation,"contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}}}),
    )
}
pub fn approval() -> WorkflowDefinition {
    serde_json::from_str(include_str!(
        "../../../../examples/workflows/approval.v2.json"
    ))
    .unwrap()
}
pub async fn state(service: &ServiceRuntime, id: &RunId, wanted: RunState) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = service
                .application()
                .status(access(), id.clone())
                .await
                .unwrap();
            if snapshot.state == wanted {
                return snapshot;
            }
            assert!(!snapshot.state.is_terminal(), "{snapshot:?}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap()
}

#[derive(Clone, Copy)]
pub enum Outcome {
    Echo,
    Fail,
    Uncertain,
}
pub struct Probe {
    pub descriptor: OperationDescriptor,
    pub calls: AtomicUsize,
    pub outcome: Outcome,
}
impl Probe {
    pub fn new(id: &str, outcome: Outcome) -> Arc<Self> {
        let mut descriptor = modules::data_operations().operations[0]
            .descriptor()
            .clone();
        descriptor.revision = OperationRevision::new(id, "1", "r1");
        if matches!(outcome, Outcome::Uncertain) {
            descriptor.effect = EffectKind::Write;
            descriptor.repetition = Repetition::Unsafe;
            descriptor.reconciliation = true;
        }
        Arc::new(Self {
            descriptor,
            calls: AtomicUsize::new(0),
            outcome,
        })
    }
}
impl Operation for Probe {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            match self.outcome {
                Outcome::Echo => Ok(OperationOutput::json(invocation.input)),
                Outcome::Fail | Outcome::Uncertain => Err(OperationError {
                    code: "test.failure".into(),
                    class: ErrorClass::InvalidInput,
                    certainty: if matches!(self.outcome, Outcome::Uncertain) {
                        EffectCertainty::Unknown
                    } else {
                        EffectCertainty::NotApplied
                    },
                    message: "private adapter payload".into(),
                }),
            }
        })
    }
}
impl EffectInspector for Probe {
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
                    note: "Confirmed externally".into(),
                },
            })
        })
    }
}
pub fn builder(operations: Vec<Arc<Probe>>) -> WorkflowBuilder {
    let inspectors = operations
        .iter()
        .filter(|op| matches!(op.outcome, Outcome::Uncertain))
        .cloned()
        .map(|op| op as Arc<dyn EffectInspector>)
        .collect();
    let mut builder = WorkflowBuilder::standard();
    builder
        .register_bundle(OperationBundle {
            module: ModuleDescriptor {
                id: "test.cli".into(),
                version: "1".into(),
                protocol_version: PROTOCOL_VERSION,
                exports: operations
                    .iter()
                    .map(|op| op.descriptor.revision.clone())
                    .collect(),
            },
            operations: operations
                .into_iter()
                .map(|op| op as Arc<dyn Operation>)
                .collect(),
            inspectors,
        })
        .unwrap();
    builder
}
