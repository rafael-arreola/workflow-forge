#![cfg(unix)]
mod support;
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use support::*;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, ChildStderr, ChildStdout, Command},
};
use workflow_forge::v2::StartReceipt;

const ENV_TOKEN: &str = "WORKFLOW_FORGE_BOOTSTRAP_FIXTURE_TOKEN";
struct Process {
    child: Child,
    error: BufReader<ChildStderr>,
    output: BufReader<ChildStdout>,
    base: String,
}
impl Process {
    async fn start(path: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_workflow-forge-service"))
            .arg(path)
            .env(ENV_TOKEN, TOKEN)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut error = BufReader::new(child.stderr.take().unwrap());
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(5), error.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let address = line
            .trim()
            .strip_prefix("workflow-forge-service ready at ")
            .unwrap_or_else(|| panic!("{line}"));
        Self {
            child,
            error,
            output,
            base: format!("http://{address}"),
        }
    }
    async fn stop(mut self) -> String {
        assert!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().unwrap().to_string()])
                .status()
                .await
                .unwrap()
                .success()
        );
        let status = tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        let mut error = String::new();
        self.error.read_to_string(&mut error).await.unwrap();
        assert!(status.success(), "{error}");
        assert!(
            error.contains("http_forced=false, engine_forced=false, pending=0"),
            "{error}"
        );
        let mut output = String::new();
        self.output.read_to_string(&mut output).await.unwrap();
        output
    }
}
fn config() -> Value {
    json!({
        "listen":"127.0.0.1:0","identities":[{"token_env":ENV_TOKEN,"actor":"fixture","permissions":["start","read"],"resources":["*"]}],
        "workflows":["echo.json"],"csv":{}
    })
}
fn setup(directory: &Directory, config: &Value) -> std::path::PathBuf {
    std::fs::write(
        directory.0.join("echo.json"),
        include_bytes!("../../../examples/service/echo.v2.json"),
    )
    .unwrap();
    let path = directory.0.join("config.json");
    std::fs::write(&path, serde_json::to_vec(config).unwrap()).unwrap();
    path
}
async fn post_to(process: &Process, body: &Value, status: u16) -> Value {
    value(
        client()
            .post(format!("{}/v2/runs", process.base))
            .bearer_auth(TOKEN)
            .json(body)
            .send()
            .await
            .unwrap(),
        status,
    )
    .await
}
async fn result(process: &Process, receipt: &StartReceipt) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = client()
                .get(format!(
                    "{}/v2/runs/{}/result",
                    process.base, receipt.run_id.0
                ))
                .bearer_auth(TOKEN)
                .send()
                .await
                .unwrap();
            if response.status() == 200 {
                return value(response, 200).await["output"].clone();
            }
            let error = value(response, 409).await;
            assert_eq!(error["diagnostics"][0]["code"], "not_ready");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn executable_defaults_to_sqlite_preserves_receipts_and_serves_runs_after_restart() {
    let directory = Directory::new();
    let path = setup(&directory, &config());
    let mut process = Process::start(&path).await;
    assert!(directory.0.join("workflow-forge.sqlite").exists());
    assert_eq!(
        value(
            client()
                .get(format!("{}/health/ready", process.base))
                .send()
                .await
                .unwrap(),
            200
        )
        .await,
        json!({"ready":true})
    );
    let body = json!({"workflow":{"id":"example.echo","revision":"r1"},"input":{"data":"business-content-do-not-log"},"options":{"receipt_key":"boot-1"}});
    let receipt: StartReceipt =
        serde_json::from_value(post_to(&process, &body, 202).await).unwrap();
    assert!(receipt.durable && !receipt.duplicate);
    assert_eq!(result(&process, &receipt).await, body["input"]);
    let mut event = String::new();
    tokio::time::timeout(Duration::from_secs(3), process.output.read_line(&mut event))
        .await
        .unwrap()
        .unwrap();
    let event_value: Value = serde_json::from_str(&event).unwrap();
    assert_eq!(event_value["run_id"], json!(receipt.run_id));
    let output = event + &process.stop().await;
    assert!(!output.contains(TOKEN) && !output.contains("business-content-do-not-log"));
    for line in output.lines() {
        let row: Value = serde_json::from_str(line).unwrap();
        assert!(row.get("state").is_some() && row.get("input").is_none());
    }
    let process = Process::start(&path).await;
    let duplicate: StartReceipt =
        serde_json::from_value(post_to(&process, &body, 202).await).unwrap();
    assert!(duplicate.duplicate && duplicate.durable);
    assert_eq!(duplicate.run_id, receipt.run_id);
    assert_eq!(result(&process, &receipt).await, body["input"]);
    process.stop().await;
    let mut config = config();
    config["workflows"] = json!([]);
    setup(&directory, &config);
    let process = Process::start(&path).await;
    assert_eq!(result(&process, &receipt).await, body["input"]);
    assert_eq!(
        post_to(&process, &body, 404).await["diagnostics"][0]["code"],
        "not_found"
    );
    process.stop().await;
}

#[tokio::test]
async fn ephemeral_profile_must_be_selected_and_explicitly_accepted_by_the_client() {
    let directory = Directory::new();
    let mut config = config();
    config["storage"] = json!({"kind":"memory"});
    let process = Process::start(&setup(&directory, &config)).await;
    let mut body = json!({"workflow":{"id":"example.echo","revision":"r1"},"input":42});
    assert_eq!(
        post_to(&process, &body, 422).await["diagnostics"][0]["code"],
        "capability.unsupported"
    );
    body["options"] = json!({"require_durable":false});
    let receipt: StartReceipt =
        serde_json::from_value(post_to(&process, &body, 202).await).unwrap();
    assert!(!receipt.durable);
    assert_eq!(result(&process, &receipt).await, json!(42));
    assert!(!directory.0.join("workflow-forge.sqlite").exists());
    process.stop().await;
}

#[tokio::test]
async fn invalid_storage_or_missing_credential_is_fatal_without_ephemeral_fallback() {
    let directory = Directory::new();
    let mut config = config();
    config["storage"] = json!({"kind":"sqlite","path":"missing-parent/state.sqlite"});
    let path = setup(&directory, &config);
    let output = Command::new(env!("CARGO_BIN_EXE_workflow-forge-service"))
        .arg(&path)
        .env(ENV_TOKEN, TOKEN)
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("ready at"));
    config["storage"] = json!({"kind":"memory"});
    setup(&directory, &config);
    let output = Command::new(env!("CARGO_BIN_EXE_workflow-forge-service"))
        .arg(&path)
        .env_remove(ENV_TOKEN)
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("service.config"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(TOKEN));
}
