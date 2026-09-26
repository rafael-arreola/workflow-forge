use super::*;

/// A recovered intention may already have reached the destination. Reuse the same
/// classification and delay strategy as live attempts, including remaining budget.
pub(super) async fn classify_unfinished(
    shared: &Shared,
    run: &RunSnapshot,
) -> Result<(), ForgeError> {
    for (key, record) in run
        .invocations
        .iter()
        .filter(|(_, record)| record.state == InvocationState::Running)
    {
        let Some(operation) = record
            .operation
            .as_ref()
            .and_then(|revision| shared.composition.operations.get(revision))
        else {
            continue;
        };
        let certainty = if operation.descriptor.effect == EffectKind::Write {
            EffectCertainty::Unknown
        } else {
            EffectCertainty::NotApplied
        };
        steps::record_failure(
            shared,
            &run.id,
            key,
            &record.attempt_id,
            &operation.descriptor,
            steps::FailureReport {
                error: state::operation_failure(
                    "operation.interrupted",
                    ErrorClass::Transient,
                    certainty,
                    "An unconfirmed attempt was recovered",
                ),
                invalid_output: false,
                cancelled: run.cancel_requested,
            },
        )
        .await?;
    }
    Ok(())
}
