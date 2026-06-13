//! Execution policy for task invocations: schema validation,
//! per-attempt timeout, panic capture, and retries with backoff.
//!
//! This is the shared path between `task` nodes and `foreach` elements:
//! any extension invocation goes through [`WorkflowExecutor::execute_with_policy`].

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

/// Execution policy for a task invocation (task node or foreach element):
/// retries, timeout, and whether attempt events are emitted.
pub(crate) struct ExecPolicy<'a> {
    pub(crate) retry: Option<&'a RetryPolicy>,
    pub(crate) timeout_ms: Option<u64>,
    pub(crate) emit_attempts: bool,
}

/// Converts a captured panic payload into a `WorkflowError`.
pub(crate) fn panic_error(
    task_id: &TaskId,
    payload: Box<dyn std::any::Any + Send>,
) -> WorkflowError {
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-textual payload".to_string());
    WorkflowError::new(
        codes::TASK_PANIC,
        format!("Task '{task_id}' panicked: {message}"),
    )
}

/// Wait between retries according to the backoff strategy.
/// `attempt` is the attempt that just failed (1-indexed).
pub(crate) fn backoff_delay(retry: &RetryPolicy, attempt: u32) -> Duration {
    let base = retry.initial_ms;
    let ms = match retry.backoff {
        Backoff::Exponential => base.saturating_mul(2u64.saturating_pow(attempt - 1)),
        Backoff::Linear => base.saturating_mul(attempt as u64),
        Backoff::Fixed => base,
    };
    Duration::from_millis(ms)
}

/// Actual wait to apply: with `jitter`, uniform over `[0, delay]` (full
/// jitter); without it, `delay` as-is.
pub(crate) fn jittered(delay: Duration, jitter: bool) -> Duration {
    if !jitter || delay.is_zero() {
        return delay;
    }
    Duration::from_millis(fastrand::u64(0..=delay.as_millis() as u64))
}

/// Applies the `retry_after_ms` floor (error hint, e.g. the HTTP header
/// `Retry-After`) on the already computed wait: never retry sooner than
/// what the target requested, but a larger backoff is still honored.
pub(crate) fn delay_with_floor(computed: Duration, retry_after_ms: Option<u64>) -> Duration {
    match retry_after_ms {
        Some(ms) => computed.max(Duration::from_millis(ms)),
        None => computed,
    }
}

impl WorkflowExecutor {
    /// Invokes a task with schema validation, timeout, and retries.
    /// This is the shared path between task nodes and foreach elements.
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
                    format!("Task '{task_id}' input does not match its schema: {e}"),
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
            // AssertUnwindSafe: after a panic the result is discarded and the
            // engine state only mutates via executor after a success
            let execution =
                AssertUnwindSafe(task.execute(ctx, WorkflowData(input.clone()))).catch_unwind();
            let result = match policy.timeout_ms {
                Some(ms) => {
                    match tokio::time::timeout(Duration::from_millis(ms), execution).await {
                        Ok(Ok(result)) => result,
                        Ok(Err(payload)) => Err(panic_error(&task_id, payload)),
                        Err(_) => Err(WorkflowError::new(
                            codes::TASK_TIMEOUT,
                            format!("Task '{task_id}' exceeded the {ms}ms timeout"),
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
                                    "Task '{task_id}' output does not match its schema: {e}"
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
                    // A panic is a bug, not a transient failure: never retry
                    let retries_left = err.code != codes::TASK_PANIC
                        && policy.retry.is_some_and(|r| attempt <= r.max);
                    let delay = retries_left.then(|| {
                        let retry = policy.retry.expect("retries_left implies it");
                        // Jitter is applied here (not in backoff_delay, which is
                        // pure) so the event reports the actual wait; the
                        // target's Retry-After acts as the floor for that wait
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
                        "Task failed; retrying"
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
    fn exponential_backoff_doubles_per_attempt() {
        let policy = retry(Backoff::Exponential, 100);
        assert_eq!(backoff_delay(&policy, 1), Duration::from_millis(100));
        assert_eq!(backoff_delay(&policy, 2), Duration::from_millis(200));
        assert_eq!(backoff_delay(&policy, 4), Duration::from_millis(800));
    }

    #[test]
    fn linear_and_fixed_backoff() {
        let linear = retry(Backoff::Linear, 100);
        assert_eq!(backoff_delay(&linear, 3), Duration::from_millis(300));

        let fixed = retry(Backoff::Fixed, 250);
        assert_eq!(backoff_delay(&fixed, 1), Duration::from_millis(250));
        assert_eq!(backoff_delay(&fixed, 9), Duration::from_millis(250));
    }

    #[test]
    fn backoff_does_not_overflow() {
        let policy = retry(Backoff::Exponential, u64::MAX / 2);
        // saturating: no panic on overflow at high attempt counts
        let _ = backoff_delay(&policy, 60);
    }

    #[test]
    fn jitter_bounds_the_wait_and_without_it_is_identity() {
        fastrand::seed(7);
        let delay = Duration::from_millis(1_000);
        for _ in 0..100 {
            assert!(jittered(delay, true) <= delay);
        }
        assert_eq!(jittered(delay, false), delay);
        assert_eq!(jittered(Duration::ZERO, true), Duration::ZERO);
    }

    #[test]
    fn retry_after_is_floor_not_ceiling() {
        let backoff = Duration::from_millis(100);
        // Retry-After greater than backoff: Retry-After wins
        assert_eq!(
            delay_with_floor(backoff, Some(5_000)),
            Duration::from_millis(5_000)
        );
        // Retry-After smaller: backoff wins (we don't accelerate below it)
        assert_eq!(delay_with_floor(backoff, Some(10)), backoff);
        // No hint: backoff as-is
        assert_eq!(delay_with_floor(backoff, None), backoff);
    }

    #[test]
    fn panic_error_extracts_the_message() {
        let task_id = TaskId::from("test.boom");
        let err = panic_error(&task_id, Box::new("it broke"));
        assert_eq!(err.code, codes::TASK_PANIC);
        assert!(err.message.contains("it broke"));

        let err = panic_error(&task_id, Box::new(String::from("another failure")));
        assert!(err.message.contains("another failure"));

        let err = panic_error(&task_id, Box::new(42_u8));
        assert!(err.message.contains("non-textual payload"));
    }
}
