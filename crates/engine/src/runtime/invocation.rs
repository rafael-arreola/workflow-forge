use super::*;

/// Dropping a supervised call always aborts its local task; no detached future.
pub(super) struct RunningCall {
    task: JoinHandle<Result<Value, ForgeError>>,
    certainty: EffectCertainty,
}
impl Drop for RunningCall {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl RunningCall {
    async fn result(&mut self) -> Result<Value, ForgeError> {
        (&mut self.task).await.unwrap_or_else(|_| {
            Err(state::operation_failure(
                "operation.failed",
                ErrorClass::Internal,
                self.certainty,
                "Attempt task was lost",
            ))
        })
    }
}
pub(super) struct AttemptOutcome {
    pub result: Result<Value, ForgeError>,
    pub late: Option<RunningCall>,
}
pub(super) struct LateAttempt {
    pub call: RunningCall,
    pub run_id: RunId,
    pub invocation_id: String,
    pub attempt_id: String,
    pub permit: tokio::sync::OwnedSemaphorePermit,
}

/// Invokes one authorized attempt. It cannot select successors or commit run state.
pub(super) async fn invoke(
    operation: Arc<dyn Operation>,
    context: OperationContext,
    command: Invocation,
    effect: EffectKind,
) -> AttemptOutcome {
    let cancellation = context.cancellation.clone();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    let deadline = context.deadline_at_ms;
    let certainty = if effect == EffectKind::Write {
        EffectCertainty::Unknown
    } else {
        EffectCertainty::NotApplied
    };
    let task = tokio::spawn(async move {
        match AssertUnwindSafe(async move { operation.execute(context, command).await })
            .catch_unwind()
            .await
        {
            Err(_) => Err(state::operation_failure(
                "operation.failed",
                ErrorClass::Internal,
                certainty,
                "Operation panicked",
            )),
            Ok(Err(mut error)) => {
                error.message = error.message.chars().take(1024).collect();
                error.code = error
                    .code
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
                    .take(128)
                    .collect();
                let mut diagnostic = Diagnostic::new("operation.failed", error.message.clone());
                diagnostic.operation_error = Some(error);
                Err(diagnostic.into())
            }
            Ok(Ok(output)) => Ok(output.value),
        }
    });
    let mut call = RunningCall { task, certainty };
    let outcome = tokio::select! {
        biased;
        _=cancellation.cancelled()=>None,
        result=tokio::time::timeout(Duration::from_millis(deadline.saturating_sub(now_ms())),call.result())=>Some(result),
    };
    let result = match outcome {
        Some(Ok(result)) => {
            cancellation.cancel();
            return AttemptOutcome { result, late: None };
        }
        Some(Err(_)) => Err(state::operation_failure(
            "operation.timeout",
            ErrorClass::Transient,
            certainty,
            "Operation reached its deadline",
        )),
        None => Err(state::operation_failure(
            "operation.cancelled",
            ErrorClass::Cancelled,
            certainty,
            "Operation was cancelled",
        )),
    };
    cancellation.cancel();
    AttemptOutcome {
        result,
        late: Some(call),
    }
}

pub(super) async fn observe_late(
    shared: Arc<Shared>,
    mut late: LateAttempt,
) -> Result<(), ForgeError> {
    let result = tokio::select! {
        _=shared.cancel.cancelled()=>None,
        result=tokio::time::timeout(Duration::from_millis(shared.composition.limits.late_response_grace_ms),late.call.result())=>result.ok(),
    };
    let Some(result) = result else {
        return Ok(());
    };
    let limits = &shared.composition.limits;
    let (output, error) = match result {
        Ok(value) => {
            match crate::schema::check_value(&value, limits.evidence_bytes, limits.json_depth) {
                Ok(_) => (Some(value), None),
                Err(error) => (None, Some(error)),
            }
        }
        Err(error) => (None, Some(error)),
    };
    let entry = AuditEntry::Late(LateObservation {
        invocation_id: late.invocation_id,
        attempt_id: late.attempt_id,
        at_ms: now_ms(),
        output,
        error,
    });
    let updated = state::update(&shared, &late.run_id, true, |run| {
        if run.audit.len() >= limits.audit_entries
            || state::retained_bytes(run).saturating_add(state::json_bytes(&entry))
                > limits.run_bytes
        {
            return Ok(false);
        }
        run.audit.push(entry.clone());
        Ok(true)
    })
    .await;
    drop(late.permit);
    // Retention can expire a terminal run while its local response is being collected.
    match updated {
        Err(error) if error.code() == "not_found" => Ok(()),
        other => other.map(|_| ()),
    }
}
