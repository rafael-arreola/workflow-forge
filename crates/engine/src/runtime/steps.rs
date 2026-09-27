use super::*;
use crate::{
    compiler::PreparedOperation,
    effects::{AttemptFailure, FailureDecision, RetryBudget, classify_failure},
};

#[derive(Clone, Debug)]
pub(super) enum StepError {
    Suspended,
    Execution(ForgeError),
    Infrastructure(ForgeError),
}
impl From<ForgeError> for StepError {
    fn from(error: ForgeError) -> Self {
        Self::Infrastructure(error)
    }
}
pub(super) fn expected(error: ForgeError) -> StepError {
    StepError::Execution(error)
}
pub(super) fn transition_error(error: ForgeError) -> StepError {
    if matches!(
        error.code(),
        "resource.limit" | "operation.cancelled" | "operation.timeout"
    ) {
        expected(error)
    } else {
        error.into()
    }
}
fn merged(previous: EffectCertainty, next: EffectCertainty) -> EffectCertainty {
    match (previous, next) {
        (EffectCertainty::Applied, _) | (_, EffectCertainty::Applied) => EffectCertainty::Applied,
        (EffectCertainty::Unknown, _) | (_, EffectCertainty::Unknown) => EffectCertainty::Unknown,
        _ => EffectCertainty::NotApplied,
    }
}

pub(super) async fn execute_operation(
    shared: &Arc<Shared>,
    id: &RunId,
    key: &str,
    node: &PreparedOperation,
    input: Value,
    cancellation: &CancellationToken,
) -> Result<Value, StepError> {
    let c = &shared.composition;
    let descriptor = &node.operation.descriptor;
    let logical = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        &serde_json::to_vec(&(id, key)).expect("identity serializes"),
    )
    .to_string();
    loop {
        let view = state::view(shared, id, Some(key)).await?;
        let run = &view.head;
        let previous = view.invocation.as_ref();
        if let Some(record) = previous {
            match record.state {
                InvocationState::Succeeded => {
                    return record.output.clone().ok_or_else(|| {
                        ForgeError::new("store.failed", "Confirmed invocation has no output").into()
                    });
                }
                InvocationState::Unknown => return Err(expected(state::unknown_error())),
                InvocationState::Failed | InvocationState::Cancelled => {
                    return Err(expected(record.error.clone().unwrap_or_else(|| {
                        ForgeError::new("operation.failed", "Invocation did not succeed")
                    })));
                }
                InvocationState::Running => {
                    // Re-entry after lost local ownership is potentially dispatched.
                    let certainty = if descriptor.effect == EffectKind::Write {
                        EffectCertainty::Unknown
                    } else {
                        EffectCertainty::NotApplied
                    };
                    record_failure(
                        shared,
                        id,
                        key,
                        &record.attempt_id,
                        descriptor,
                        FailureReport {
                            error: state::operation_failure(
                                "operation.interrupted",
                                ErrorClass::Transient,
                                certainty,
                                "Unfinished attempt requires recovery",
                            ),
                            invalid_output: false,
                            cancelled: cancellation.is_cancelled(),
                        },
                    )
                    .await?;
                    continue;
                }
                InvocationState::RetryScheduled => {
                    if run.cancel_requested
                        || cancellation.is_cancelled()
                        || now_ms() >= run.deadline_at_ms
                    {
                        let error = record.error.clone().unwrap_or_else(state::unknown_error);
                        record_failure(
                            shared,
                            id,
                            key,
                            &record.attempt_id,
                            descriptor,
                            FailureReport {
                                error,
                                invalid_output: false,
                                cancelled: cancellation.is_cancelled(),
                            },
                        )
                        .await?;
                        continue;
                    }
                    if let Some(due) = record.next_attempt_at_ms.filter(|due| *due > now_ms()) {
                        tokio::select! { _=cancellation.cancelled()=>(),_=tokio::time::sleep(Duration::from_millis(due.min(run.deadline_at_ms).saturating_sub(now_ms())))=>() }
                        continue;
                    }
                }
                InvocationState::Pending => (),
            }
        }
        if run.cancel_requested || cancellation.is_cancelled() {
            return Err(expected(ForgeError::new(
                "operation.cancelled",
                "Run was cancelled before dispatch",
            )));
        }
        if now_ms() >= run.deadline_at_ms {
            return Err(expected(ForgeError::new(
                "operation.timeout",
                "Run reached its deadline before dispatch",
            )));
        }
        if previous.is_some_and(|r| r.attempts >= node.retry.max_attempts) {
            return Err(expected(ForgeError::new(
                "operation.failed",
                "Attempt budget is exhausted",
            )));
        }
        let permit = tokio::select! {
            p=shared.attempts.clone().acquire_owned()=>p.map_err(|_|StepError::Infrastructure(unavailable()))?,
            _=cancellation.cancelled()=>return Err(expected(ForgeError::new("operation.cancelled","Run was cancelled before dispatch"))),
            _=tokio::time::sleep(Duration::from_millis(run.deadline_at_ms.saturating_sub(now_ms())))=>return Err(expected(ForgeError::new("operation.timeout","Run reached its deadline while waiting for capacity"))),
        };
        let attempt_id = uuid::Uuid::now_v7().to_string();
        let effect_key = (descriptor.effect == EffectKind::Write).then(|| logical.clone());
        let record = InvocationRecord {
            id: logical.clone(),
            attempt_id: attempt_id.clone(),
            attempts: previous.map_or(1, |r| r.attempts.saturating_add(1)),
            state: InvocationState::Running,
            input: previous.map_or_else(|| input.clone(), |r| r.input.clone()),
            output: None,
            error: None,
            operation: Some(descriptor.revision.clone()),
            config: node.config.clone(),
            effect_key: effect_key.clone(),
            retry: node.retry.clone(),
            next_attempt_at_ms: None,
            certainty: previous.map_or(EffectCertainty::NotApplied, |r| r.certainty),
            control: None,
        };
        let intent = state::update_node(shared, id, key, true, |run, current| {
            if run.cancel_requested || cancellation.is_cancelled() {
                return Err(ForgeError::new(
                    "operation.cancelled",
                    "Run was cancelled before dispatch",
                ));
            }
            if now_ms() >= run.deadline_at_ms {
                return Err(ForgeError::new(
                    "operation.timeout",
                    "Run reached its deadline before dispatch",
                ));
            }
            *current = Some(record.clone());
            Ok(true)
        })
        .await
        .map_err(transition_error)?;
        if !intent
            .invocation
            .as_ref()
            .is_some_and(|r| r.attempt_id == attempt_id && r.state == InvocationState::Running)
        {
            return Err(expected(state::unknown_error()));
        }
        let intent = intent.head;
        let deadline = intent
            .deadline_at_ms
            .min(now_ms().saturating_add(c.limits.attempt_timeout_ms));
        let context = OperationContext::new(
            ArtifactAccess {
                runtime_owner: c.id.clone(),
                run_id: id.clone(),
            },
            deadline,
            cancellation.child_token(),
            intent.scope.clone(),
            descriptor.required_resources.clone(),
            c.secrets.clone(),
            c.artifacts.clone(),
        );
        let command = Invocation {
            id: logical.clone(),
            attempt_id: attempt_id.clone(),
            operation: descriptor.revision.clone(),
            config: record.config.clone(),
            input: record.input.clone(),
            effect_key,
        };
        let outcome = invocation::invoke(
            node.operation.operation.clone(),
            context,
            command,
            descriptor.effect,
        )
        .await;
        if let Some(call) = outcome.late {
            shared
                .late
                .send(invocation::LateAttempt {
                    call,
                    run_id: id.clone(),
                    invocation_id: logical.clone(),
                    attempt_id: attempt_id.clone(),
                    permit,
                })
                .await
                .map_err(|_| StepError::Infrastructure(unavailable()))?;
        } else {
            drop(permit);
        }
        let mut invalid_output = false;
        let result = outcome.result.and_then(|value| {
            node.operation
                .output
                .validate(&value, &c.limits)
                .map_err(|mut error| {
                    invalid_output = true;
                    for d in &mut error.diagnostics {
                        d.location.field = "/output".into();
                    }
                    error
                })?;
            Ok(value)
        });
        let error = match result {
            Ok(value) => {
                let committed = state::update_node(shared, id, key, true, |_, current| {
                    let Some(current) = current.as_mut().filter(|r| {
                        r.attempt_id == attempt_id && r.state == InvocationState::Running
                    }) else {
                        return Ok(false);
                    };
                    current.output = Some(value.clone());
                    current.state = InvocationState::Succeeded;
                    current.input = Value::Null;
                    current.certainty = if descriptor.effect == EffectKind::Write {
                        EffectCertainty::Applied
                    } else {
                        EffectCertainty::NotApplied
                    };
                    Ok(true)
                })
                .await;
                match committed {
                    Ok(view)
                        if view.invocation.as_ref().is_some_and(|r| {
                            r.attempt_id == attempt_id && r.state == InvocationState::Succeeded
                        }) =>
                    {
                        return Ok(value);
                    }
                    Ok(_) => return Err(expected(state::unknown_error())),
                    Err(error) if error.code() == "resource.limit" => {
                        invalid_output = true;
                        error
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => error,
        };
        record_failure(
            shared,
            id,
            key,
            &attempt_id,
            descriptor,
            FailureReport {
                error,
                invalid_output,
                cancelled: cancellation.is_cancelled(),
            },
        )
        .await?;
    }
}

pub(super) struct FailureReport {
    pub error: ForgeError,
    pub invalid_output: bool,
    pub cancelled: bool,
}

pub(super) async fn record_failure(
    shared: &Shared,
    id: &RunId,
    key: &str,
    attempt: &str,
    descriptor: &OperationDescriptor,
    failure: FailureReport,
) -> Result<(), ForgeError> {
    let FailureReport {
        error,
        invalid_output,
        cancelled,
    } = failure;
    let c = &shared.composition;
    state::update_node(shared, id, key, false, |run, current| {
        let Some(record) = current.as_mut().filter(|r| {
            r.attempt_id == attempt
                && matches!(
                    r.state,
                    InvocationState::Running | InvocationState::RetryScheduled
                )
        }) else {
            return Ok(false);
        };
        let cause = error
            .diagnostics
            .iter()
            .find_map(|d| d.operation_error.as_ref());
        let class = cause.map_or(ErrorClass::Internal, |e| e.class);
        let certainty = if descriptor.effect != EffectKind::Write {
            EffectCertainty::NotApplied
        } else if invalid_output {
            EffectCertainty::Applied
        } else {
            merged(
                record.certainty,
                cause.map_or(EffectCertainty::Unknown, |e| e.certainty),
            )
        };
        let mut decision = classify_failure(
            descriptor.effect,
            descriptor.repetition,
            AttemptFailure {
                class,
                certainty,
                invalid_output,
            },
            RetryBudget {
                completed_attempts: record.attempts,
                max_attempts: record.retry.max_attempts,
                now_ms: now_ms(),
                deadline_at_ms: run.deadline_at_ms,
                cancel_requested: cancelled || run.cancel_requested,
            },
        );
        let mut due = None;
        if decision == FailureDecision::Retry {
            let delay = c
                .backoff
                .delay_ms(BackoffRequest {
                    policy: &record.retry,
                    completed_attempt: record.attempts,
                    entropy: uuid::Uuid::now_v7().as_u128() as u64,
                })
                .min(record.retry.max_delay_ms);
            let at = now_ms().saturating_add(delay);
            if at >= run.deadline_at_ms {
                decision = if certainty == EffectCertainty::NotApplied {
                    FailureDecision::Fail
                } else {
                    FailureDecision::Block
                };
            } else {
                due = Some(at);
            }
        }
        let mut error = error.clone();
        for diagnostic in &mut error.diagnostics {
            diagnostic.location.node = Some(key.into());
            diagnostic.retryable = Some(decision == FailureDecision::Retry);
            if let Some(cause) = &mut diagnostic.operation_error {
                cause.certainty = certainty;
            }
        }
        record.error = Some(error);
        record.certainty = certainty;
        record.next_attempt_at_ms = due;
        record.state = match decision {
            FailureDecision::Retry => InvocationState::RetryScheduled,
            FailureDecision::Block => InvocationState::Unknown,
            FailureDecision::Fail => InvocationState::Failed,
            FailureDecision::Cancel => InvocationState::Cancelled,
        };
        Ok(true)
    })
    .await
    .map(|_| ())
}
