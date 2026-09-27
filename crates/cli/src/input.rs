use crate::error::{Failure, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    io::{IsTerminal, Read, Write},
    path::Path,
};

pub fn read_json<T: DeserializeOwned>(path: &Path, limit: usize) -> Result<T> {
    let file = std::fs::File::open(path)
        .map_err(|_| Failure::new("cli.file", "Input file is unavailable"))?;
    parse(&read(file, limit)?)
}

pub async fn trigger(argument: Option<String>, limit: usize) -> Result<Value> {
    if let Some(argument) = argument {
        if let Some(path) = argument.strip_prefix('@') {
            return read_json(Path::new(path), limit);
        }
        if argument.len() > limit {
            return Err(too_large());
        }
        return parse(argument.as_bytes());
    }
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        return Ok(serde_json::json!({}));
    }
    // A dedicated reader can be abandoned on interruption. Tokio's blocking
    // pool would keep runtime shutdown waiting for an open stdin pipe to close.
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("forge-stdin".into())
        .spawn(move || {
            let _ = sender.send(read(stdin.lock(), limit));
        })
        .map_err(|_| Failure::new("cli.file", "Input reader is unavailable"))?;
    let bytes = receiver
        .await
        .map_err(|_| Failure::new("cli.file", "Input reader stopped"))??;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(serde_json::json!({}));
    }
    parse(&bytes)
}

pub fn parse<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| {
        Failure::new(
            "cli.json",
            "JSON is invalid or does not match the requested contract",
        )
    })
}

fn read(reader: impl Read, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::new("cli.file", "Input could not be read"))?;
    if bytes.len() > limit {
        return Err(too_large());
    }
    Ok(bytes)
}
fn too_large() -> Failure {
    Failure::new("cli.body_too_large", "Input exceeds the client byte budget")
}

pub fn output(value: &impl serde::Serialize, stderr: bool) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|_| Failure::new("cli.json", "Output could not be serialized"))?;
    bytes.push(b'\n');
    let result = if stderr {
        std::io::stderr().lock().write_all(&bytes)
    } else {
        std::io::stdout().lock().write_all(&bytes)
    };
    result.map_err(|_| Failure::new("cli.output", "Output stream is unavailable"))
}
