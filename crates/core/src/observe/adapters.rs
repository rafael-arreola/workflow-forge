//! Adaptadores [`ExecutionObserver`] listos para usar como bitácora durable.
//!
//! El executor solo conoce el trait [`ExecutionObserver`]; estos son
//! "baterías incluidas" para que un host no escriba la persistencia desde
//! cero:
//!
//! - [`TracingObserver`] — emite cada evento por el crate `tracing`, de modo
//!   que fluye hacia el subscriber que el host ya tenga (stdout, journald,
//!   OpenTelemetry vía `tracing-opentelemetry`, …).
//! - [`JsonlObserver`] — anexa cada evento como una línea JSON a cualquier
//!   writer o archivo. Un audit log append-only, simple y greppeable.
//!
//! Ambos son síncronos (como exige el trait). [`JsonlObserver`] hace flush en
//! cada línea para no perder nada ante un crash; para throughput muy alto,
//! envuelve tu propio observer respaldado por un canal.

use std::io::Write;
use std::sync::Mutex;

use super::{ExecutionEvent, ExecutionObserver};

/// Observer que loggea cada evento vía el crate `tracing`.
///
/// Los fallos (`workflow_failed`, `node_failed`, `task_attempt_failed`) se
/// emiten a nivel `error`, el resto a `info`, cada uno con `execution_id`,
/// `seq` y el `type` del evento.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingObserver;

impl TracingObserver {
    /// Un observer de tracing nuevo.
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

/// Observer que anexa cada evento como una línea JSON (JSON Lines / `.jsonl`)
/// a un writer. El evento se serializa completo, así que el log es
/// reproducible (replayable).
pub struct JsonlObserver<W: Write + Send> {
    writer: Mutex<W>,
}

impl<W: Write + Send> JsonlObserver<W> {
    /// Escribe los eventos a un writer arbitrario (p. ej. un `Vec<u8>` en
    /// memoria para tests, o un `File`).
    pub fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }
}

impl JsonlObserver<std::io::BufWriter<std::fs::File>> {
    /// Anexa los eventos a un archivo en `path` (se crea si no existe).
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
                tracing::error!(error = %e, "JsonlObserver: no se pudo serializar el evento");
                return;
            }
        };
        line.push(b'\n');
        let mut writer = match self.writer.lock() {
            Ok(w) => w,
            Err(_) => {
                tracing::error!("JsonlObserver: writer lock poisoned");
                return;
            }
        };
        if let Err(e) = writer.write_all(&line).and_then(|()| writer.flush()) {
            tracing::error!(error = %e, "JsonlObserver: no se pudo escribir el evento");
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
    fn jsonl_escribe_una_linea_por_evento() {
        // Buffer compartido para poder leer lo escrito.
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
        let written = buf.lock().unwrap();
        let text = String::from_utf8(written.clone()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        // Cada línea es un evento autocontenido y parseable.
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["type"], "workflow_completed");
        assert_eq!(first["seq"], 0);
    }

    // Un `Write` sobre un buffer compartido, para el test de arriba.
    struct SharedBuf(std::sync::Arc<Mutex<Vec<u8>>>);
    impl Write for SharedBuf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
