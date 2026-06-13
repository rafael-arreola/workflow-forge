//! Política de ejecución de invocaciones de tarea: validación de schemas,
//! timeout por intento, captura de panics y reintentos con backoff.
//!
//! Es el camino compartido entre nodos `task` y elementos de `foreach`:
//! cualquier invocación de una extensión pasa por [`WorkflowExecutor::execute_with_policy`].

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use serde_json::Value;
use tracing::warn;

use crate::error::{WorkflowError, codes};
use crate::observe::EventKind;
use crate::runtime::context::WorkflowContext;
use crate::runtime::executor::WorkflowExecutor;
use crate::runtime::schemas::validate_compiled;
use crate::spec::node::NodeId;
use crate::spec::node::task::{Backoff, RetryPolicy};
use crate::task::{Task, TaskId, WorkflowData};

/// Política de ejecución de una invocación de tarea (nodo task o elemento
/// de foreach): reintentos, timeout y si se emiten eventos de attempt.
pub(crate) struct ExecPolicy<'a> {
    pub(crate) retry: Option<&'a RetryPolicy>,
    pub(crate) timeout_ms: Option<u64>,
    pub(crate) emit_attempts: bool,
}

/// Convierte el payload de un panic capturado en un `WorkflowError`.
pub(crate) fn panic_error(
    task_id: &TaskId,
    payload: Box<dyn std::any::Any + Send>,
) -> WorkflowError {
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "payload no textual".to_string());
    WorkflowError::new(
        codes::TASK_PANIC,
        format!("La tarea '{task_id}' panickeó: {message}"),
    )
}

/// Espera entre reintentos según la estrategia de backoff.
/// `attempt` es el intento que acaba de fallar (1-indexado).
pub(crate) fn backoff_delay(retry: &RetryPolicy, attempt: u32) -> Duration {
    let base = retry.initial_ms;
    let ms = match retry.backoff {
        Backoff::Exponential => base.saturating_mul(2u64.saturating_pow(attempt - 1)),
        Backoff::Linear => base.saturating_mul(attempt as u64),
        Backoff::Fixed => base,
    };
    Duration::from_millis(ms)
}

/// Espera real a aplicar: con `jitter`, uniforme en `[0, delay]` (full
/// jitter); sin él, `delay` tal cual.
pub(crate) fn jittered(delay: Duration, jitter: bool) -> Duration {
    if !jitter || delay.is_zero() {
        return delay;
    }
    Duration::from_millis(fastrand::u64(0..=delay.as_millis() as u64))
}

/// Aplica el piso de `retry_after_ms` (pista del error, p. ej. el header HTTP
/// `Retry-After`) sobre la espera ya calculada: nunca se reintenta antes de
/// lo que el destino pidió, pero un backoff mayor sí se respeta.
pub(crate) fn delay_with_floor(computed: Duration, retry_after_ms: Option<u64>) -> Duration {
    match retry_after_ms {
        Some(ms) => computed.max(Duration::from_millis(ms)),
        None => computed,
    }
}

impl WorkflowExecutor {
    /// Invoca una tarea con validación de schemas, timeout y reintentos.
    /// Es el camino compartido entre nodos task y elementos de foreach.
    pub(crate) async fn execute_with_policy(
        &self,
        task: &Arc<dyn Task>,
        node_id: &NodeId,
        input: Value,
        policy: ExecPolicy<'_>,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let task_id = task.task_id().clone();
        let (input_validator, output_validator) = self
            .schemas
            .tasks
            .get(&task_id)
            .map(|(i, o)| (i.as_ref(), o.as_ref()))
            .unwrap_or((None, None));

        if let Some(validator) = input_validator {
            validate_compiled(validator, &input).map_err(|e| {
                WorkflowError::new(
                    codes::TASK_INPUT_INVALID,
                    format!("El input de la tarea '{task_id}' no cumple su schema: {e}"),
                )
                .with_source_task(node_id.to_string())
            })?;
        }

        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            if policy.emit_attempts {
                self.emit(
                    ctx,
                    EventKind::TaskAttemptStarted {
                        node_id: node_id.0.clone(),
                        attempt,
                        input: (attempt == 1).then(|| input.clone()),
                    },
                );
            }
            // AssertUnwindSafe: tras un panic el resultado se descarta y el
            // estado del engine solo muta vía executor después de un éxito
            let execution =
                AssertUnwindSafe(task.execute(ctx, WorkflowData(input.clone()))).catch_unwind();
            let result = match policy.timeout_ms {
                Some(ms) => {
                    match tokio::time::timeout(Duration::from_millis(ms), execution).await {
                        Ok(Ok(result)) => result,
                        Ok(Err(payload)) => Err(panic_error(&task_id, payload)),
                        Err(_) => Err(WorkflowError::new(
                            codes::TASK_TIMEOUT,
                            format!("La tarea '{task_id}' superó el timeout de {ms}ms"),
                        )),
                    }
                }
                None => match execution.await {
                    Ok(result) => result,
                    Err(payload) => Err(panic_error(&task_id, payload)),
                },
            };

            match result {
                Ok(output) => {
                    if let Some(validator) = output_validator {
                        validate_compiled(validator, &output.0).map_err(|e| {
                            WorkflowError::new(
                                codes::TASK_OUTPUT_INVALID,
                                format!(
                                    "El output de la tarea '{task_id}' no cumple su schema: {e}"
                                ),
                            )
                            .with_source_task(node_id.to_string())
                        })?;
                    }
                    return Ok(output.0);
                }
                Err(mut err) => {
                    if err.source_task.is_none() {
                        err.source_task = Some(node_id.to_string());
                    }
                    // Un panic es un bug, no un fallo transitorio: jamás reintenta
                    let retries_left = err.code != codes::TASK_PANIC
                        && policy.retry.is_some_and(|r| attempt <= r.max);
                    let delay = retries_left.then(|| {
                        let retry = policy.retry.expect("retries_left lo implica");
                        // El jitter se aplica aquí (no en backoff_delay, que es
                        // pura) para que el evento reporte la espera real; el
                        // Retry-After del destino actúa como piso de esa espera
                        let computed = jittered(backoff_delay(retry, attempt), retry.jitter);
                        delay_with_floor(computed, err.retry_after_ms)
                    });
                    if policy.emit_attempts {
                        self.emit(
                            ctx,
                            EventKind::TaskAttemptFailed {
                                node_id: node_id.0.clone(),
                                attempt,
                                error: err.clone(),
                                will_retry: retries_left,
                                next_delay_ms: delay.map(|d| d.as_millis() as u64),
                            },
                        );
                    }
                    let Some(delay) = delay else {
                        return Err(err);
                    };
                    warn!(
                        node_id = %node_id,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        code = %err.code,
                        "Tarea falló; reintentando"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry(backoff: Backoff, initial_ms: u64) -> RetryPolicy {
        RetryPolicy {
            max: 5,
            backoff,
            initial_ms,
            jitter: false,
        }
    }

    #[test]
    fn backoff_exponencial_duplica_por_intento() {
        let policy = retry(Backoff::Exponential, 100);
        assert_eq!(backoff_delay(&policy, 1), Duration::from_millis(100));
        assert_eq!(backoff_delay(&policy, 2), Duration::from_millis(200));
        assert_eq!(backoff_delay(&policy, 4), Duration::from_millis(800));
    }

    #[test]
    fn backoff_lineal_y_fijo() {
        let lineal = retry(Backoff::Linear, 100);
        assert_eq!(backoff_delay(&lineal, 3), Duration::from_millis(300));

        let fijo = retry(Backoff::Fixed, 250);
        assert_eq!(backoff_delay(&fijo, 1), Duration::from_millis(250));
        assert_eq!(backoff_delay(&fijo, 9), Duration::from_millis(250));
    }

    #[test]
    fn backoff_no_desborda() {
        let policy = retry(Backoff::Exponential, u64::MAX / 2);
        // saturating: no panic por overflow en intentos altos
        let _ = backoff_delay(&policy, 60);
    }

    #[test]
    fn jitter_acota_la_espera_y_sin_el_es_identidad() {
        fastrand::seed(7);
        let delay = Duration::from_millis(1_000);
        for _ in 0..100 {
            assert!(jittered(delay, true) <= delay);
        }
        assert_eq!(jittered(delay, false), delay);
        assert_eq!(jittered(Duration::ZERO, true), Duration::ZERO);
    }

    #[test]
    fn el_retry_after_es_piso_no_techo() {
        let backoff = Duration::from_millis(100);
        // Retry-After mayor que el backoff: gana el Retry-After
        assert_eq!(
            delay_with_floor(backoff, Some(5_000)),
            Duration::from_millis(5_000)
        );
        // Retry-After menor: gana el backoff (no aceleramos por debajo)
        assert_eq!(delay_with_floor(backoff, Some(10)), backoff);
        // Sin pista: el backoff tal cual
        assert_eq!(delay_with_floor(backoff, None), backoff);
    }

    #[test]
    fn panic_error_extrae_el_mensaje() {
        let task_id = TaskId::from("test.boom");
        let err = panic_error(&task_id, Box::new("se rompió"));
        assert_eq!(err.code, codes::TASK_PANIC);
        assert!(err.message.contains("se rompió"));

        let err = panic_error(&task_id, Box::new(String::from("otro fallo")));
        assert!(err.message.contains("otro fallo"));

        let err = panic_error(&task_id, Box::new(42_u8));
        assert!(err.message.contains("payload no textual"));
    }
}
