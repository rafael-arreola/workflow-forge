mod support;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use support::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use workflow_forge_protocol::{ArtifactRef, ForgeError};

// Deliberately malformed peers exercise the process boundary, not private client helpers.
struct Peer {
    url: String,
    paths: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    async fn new(reply: impl FnOnce(&str) -> Vec<u8>, delay: Duration) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let reply = reply(&url);
        let paths = Arc::new(Mutex::new(Vec::new()));
        let captured = paths.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    let size = socket.read(&mut buffer).await.unwrap();
                    if size == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..size]);
                    assert!(request.len() < 1024 * 1024);
                    if let Some(end) = request.windows(4).position(|s| s == b"\r\n\r\n") {
                        let header = std::str::from_utf8(&request[..end]).unwrap();
                        let length: usize = header
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().unwrap())
                            })
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                if request.is_empty() {
                    continue;
                }
                let first = request.split(|b| *b == b'\n').next().unwrap();
                captured.lock().unwrap().push(
                    std::str::from_utf8(first)
                        .unwrap()
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .into(),
                );
                tokio::time::sleep(delay).await;
                let _ = socket.write_all(&reply).await;
                let _ = socket.shutdown().await;
            }
        });
        Self { url, paths, task }
    }
    fn requests(&self) -> Vec<String> {
        self.paths.lock().unwrap().clone()
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = format!("HTTP/1.1 {status}\r\nConnection: close\r\n{headers}\r\n").into_bytes();
    bytes.extend_from_slice(body);
    bytes
}
fn json_response(status: &str, code: &str) -> Vec<u8> {
    let body = serde_json::to_vec(&ForgeError::new(code, "Controlled rejection")).unwrap();
    response(
        status,
        &format!("Content-Length: {}\r\n", body.len()),
        &body,
    )
}

#[tokio::test]
async fn redirects_retries_and_request_timeouts_preserve_one_command_one_request() {
    let peer = Peer::new(
        |url| {
            let body = serde_json::to_vec(&ForgeError::new("test.redirect", "Moved")).unwrap();
            response(
                "302 Found",
                &format!(
                    "Location: {url}/must-not-follow\r\nContent-Length: {}\r\n",
                    body.len()
                ),
                &body,
            )
        },
        Duration::ZERO,
    )
    .await;
    failure(
        execute(&format!("{}/prefix/", peer.url), &["catalog"]).await,
        1,
        "test.redirect",
    );
    assert_eq!(peer.requests(), ["/prefix/v2/catalog"]);
    let peer = Peer::new(
        |_| json_response("503 Service Unavailable", "test.unavailable"),
        Duration::ZERO,
    )
    .await;
    failure(
        execute(&peer.url, &["start", "workflow", "r1", "--input", "{}"]).await,
        1,
        "test.unavailable",
    );
    assert_eq!(peer.requests(), ["/v2/runs"]);
    let peer = Peer::new(
        |_| json_response("503 Service Unavailable", "test.unavailable"),
        Duration::from_millis(200),
    )
    .await;
    failure(
        execute(&peer.url, &["catalog", "--request-timeout-ms", "20"]).await,
        1,
        "cli.transport",
    );
    assert_eq!(peer.requests(), ["/v2/catalog"]);
}

#[tokio::test]
async fn real_received_bytes_and_malformed_documents_are_bounded_without_echoing_data() {
    let peer = Peer::new(
        |_| {
            response(
                "200 OK",
                "Transfer-Encoding: chunked\r\n",
                format!("200\r\n{}\r\n0\r\n\r\n", "X".repeat(512)).as_bytes(),
            )
        },
        Duration::ZERO,
    )
    .await;
    failure(
        execute(&peer.url, &["catalog", "--max-response-bytes", "256"]).await,
        1,
        "cli.response_too_large",
    );
    assert_eq!(peer.requests().len(), 1);
    let secret = "private-server-body-should-not-appear";
    let peer = Peer::new(
        |_| {
            response(
                "500 Internal Server Error",
                &format!("Content-Length: {}\r\n", secret.len()),
                secret.as_bytes(),
            )
        },
        Duration::ZERO,
    )
    .await;
    let output = execute(&peer.url, &["catalog"]).await;
    assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    failure(output, 1, "cli.json");
    let count = peer.requests().len();
    let directory = Directory::new();
    let invalid = directory.0.join("invalid.json");
    std::fs::write(&invalid, b"{private-input-should-not-appear").unwrap();
    let output = execute(&peer.url, &["validate", invalid.to_str().unwrap()]).await;
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private-input"));
    failure(output, 1, "cli.json");
    failure(
        execute(
            &peer.url,
            &[
                "start",
                "workflow",
                "r1",
                "--input",
                &json!("X".repeat(300)).to_string(),
                "--max-json-bytes",
                "256",
            ],
        )
        .await,
        1,
        "cli.body_too_large",
    );
    // Input alone fits, but the complete start envelope exceeds the same bound.
    failure(
        execute(
            &peer.url,
            &[
                "start",
                "workflow",
                "r1",
                "--input",
                &json!("X".repeat(150)).to_string(),
                "--max-json-bytes",
                "256",
            ],
        )
        .await,
        1,
        "cli.body_too_large",
    );
    failure(
        execute(&peer.url, &["status", "../catalog"]).await,
        1,
        "cli.run_id",
    );
    assert_eq!(peer.requests().len(), count);
    assert_eq!(
        execute(&peer.url, &["catalog", "--request-timeout-ms", "0"])
            .await
            .status
            .code(),
        Some(2)
    );
}

#[tokio::test]
async fn incomplete_and_oversized_artifact_streams_never_publish_a_destination() {
    let directory = Directory::new();
    let reference = directory.json(
        "reference.json",
        &ArtifactRef {
            id: "test-artifact".into(),
            scope: "default".into(),
            bytes: 8,
            media_type: "application/octet-stream".into(),
        },
    );
    let output = directory.0.join("result.bin");
    let args = ["download", &reference, "--output", output.to_str().unwrap()];
    for (wire, code) in [
        (
            response("200 OK", "Content-Length: 8\r\n", b"short"),
            "cli.transport",
        ),
        (
            response(
                "200 OK",
                "Transfer-Encoding: chunked\r\n",
                b"9\r\n123456789\r\n0\r\n\r\n",
            ),
            "cli.protocol",
        ),
        (
            response("200 OK", "Content-Length: 9\r\n", b"123456789"),
            "cli.protocol",
        ),
    ] {
        let peer = Peer::new(|_| wire, Duration::ZERO).await;
        failure(execute(&peer.url, &args).await, 1, code);
        assert!(!output.exists());
        assert!(!std::fs::read_dir(&directory.0).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".part")
        }));
        assert_eq!(peer.requests(), ["/v2/artifacts/read"]);
    }
}
