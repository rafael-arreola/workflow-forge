//! Ready-to-use [`ExecutionObserver`] adapters as a durable log.
//!
//! The executor only knows the [`ExecutionObserver`] trait; these are
//! "batteries included" so a host does not write persistence from scratch:
//!
//! - [`TracingObserver`] — emits each event through the `tracing` crate, so
//!   it flows to whatever subscriber the host already has (stdout, journald,
//!   OpenTelemetry via `tracing-opentelemetry`, ...).
//! - [`JsonlObserver`] — appends each event as a JSON line to any writer or
//!   file. An append-only audit log, simple and greppable.
//!
//! Both are synchronous (as required by the trait). [`JsonlObserver`] flushes
//! on every line to avoid losing data on a crash; for very high throughput,
//! wrap your own observer backed by a channel.

use parking_lot::Mutex;
use std::io::Write;

use super::{ExecutionEvent, ExecutionObserver};

/// Observer that logs each event via the `tracing` crate.
///
/// Failures (`workflow_failed`, `node_failed`, `task_attempt_failed`) are
/// emitted at `error` level, the rest at `info`, each with `execution_id`,
/// `seq`, and the event `type`.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingObserver;

impl TracingObserver {
    /// A new tracing observer.
    pub fn new() -> Self {
        Self
    }
}

impl ExecutionObserver for TracingObserver {
    fn on_event(&self, event: &ExecutionEvent) {
        let kind = serde_json::to_value(&event.kind)
            .ok()
            .and_then(|v| v["type"].as_str().map(str::to_string))
            .unwrap_or_default();
        let failed = matches!(
            event.kind,
            super::EventKind::WorkflowFailed { .. }
                | super::EventKind::NodeFailed { .. }
                | super::EventKind::TaskAttemptFailed { .. }
        );
        if failed {
            tracing::error!(
                execution_id = %event.execution_id,
                seq = event.seq,
                elapsed_ms = event.elapsed_ms,
                node_id = event.kind.node_id().unwrap_or(""),
                "{kind}"
            );
        } else {
            tracing::info!(
                execution_id = %event.execution_id,
                seq = event.seq,
                elapsed_ms = event.elapsed_ms,
                node_id = event.kind.node_id().unwrap_or(""),
                "{kind}"
            );
        }
    }
}

/// Observer that appends each event as a JSON line (JSON Lines / `.jsonl`)
/// to a writer. The event is serialized in full, so the log is reproducible
/// (replayable).
pub struct JsonlObserver<W: Write + Send> {
    writer: Mutex<W>,
}

impl<W: Write + Send> JsonlObserver<W> {
    /// Writes events to an arbitrary writer (e.g. a `Vec<u8>` in memory for
    /// tests, or a `File`).
    pub fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }
}

impl JsonlObserver<std::io::BufWriter<std::fs::File>> {
    /// Appends events to a file at `path` (creates it if it doesn't exist).
    pub fn to_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self::new(std::io::BufWriter::new(file)))
    }
}

impl<W: Write + Send> ExecutionObserver for JsonlObserver<W> {
    fn on_event(&self, event: &ExecutionEvent) {
        let mut line = match serde_json::to_vec(event) {
            Ok(line) => line,
            Err(e) => {
                tracing::error!(error = %e, "JsonlObserver: could not serialize event");
                return;
            }
        };
        line.push(b'\n');
        let mut writer = self.writer.lock();
        if let Err(e) = writer.write_all(&line).and_then(|()| writer.flush()) {
            tracing::error!(error = %e, "JsonlObserver: could not write event");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::EventKind;
    use serde_json::json;

    fn event(seq: u64, kind: EventKind) -> ExecutionEvent {
        ExecutionEvent {
            execution_id: "exec-1".into(),
            parent_execution_id: None,
            seq,
            elapsed_ms: 0,
            kind,
        }
    }

    #[test]
    fn jsonl_writes_one_line_per_event() {
        // Shared buffer to be able to read what was written.
        let buf = std::sync::Arc::new(Mutex::new(Vec::<u8>::new()));
        {
            let observer = JsonlObserver::new(SharedBuf(buf.clone()));
            observer.on_event(&event(
                0,
                EventKind::WorkflowCompleted {
                    output: json!({ "ok": true }),
                    duration_ms: 5,
                },
            ));
            observer.on_event(&event(
                1,
                EventKind::NodeCompleted {
                    node_id: "n".into(),
                    output: json!(1),
                    duration_ms: 1,
                },
            ));
        }
        let written = buf.lock();
        let text = String::from_utf8(written.clone()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        // Each line is a self-contained, parseable event.
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["type"], "workflow_completed");
        assert_eq!(first["seq"], 0);
    }

    // A `Write` over a shared buffer, for the test above.
    struct SharedBuf(std::sync::Arc<Mutex<Vec<u8>>>);
    impl Write for SharedBuf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
