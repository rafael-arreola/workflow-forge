//! Pure effect classification. Calling a backoff strategy comes AFTER this decision.
//! These rules are shared by dispatch failure, timeout, cancellation and recovery.
use workflow_forge_protocol::{EffectCertainty, EffectKind, ErrorClass, Repetition};

#[derive(Clone, Copy, Debug)]
pub struct AttemptFailure {
    pub class: ErrorClass,
    pub certainty: EffectCertainty,
    /// An output contract failed AFTER dispatch. A write may already have applied.
    pub invalid_output: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryBudget {
    pub completed_attempts: u32,
    pub max_attempts: u32,
    pub now_ms: u64,
    pub deadline_at_ms: u64,
    pub cancel_requested: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureDecision {
    Fail,
    Cancel,
    Retry,
    Block,
}

/// An authorization decision, not a dispatch. The coordinator must commit its
/// intent, enforce the due time/deadline and re-check cancellation before calling.
pub fn classify_failure(
    effect: EffectKind,
    repetition: Repetition,
    failure: AttemptFailure,
    budget: RetryBudget,
) -> FailureDecision {
    let write = effect == EffectKind::Write;
    let unresolved =
        write && (failure.certainty != EffectCertainty::NotApplied || failure.invalid_output);
    // A known effect without a conforming result needs reconciliation, even with
    // an idempotent destination. Retrying validation cannot manufacture a result.
    if write && (failure.certainty == EffectCertainty::Applied || failure.invalid_output) {
        return FailureDecision::Block;
    }
    if budget.cancel_requested || failure.class == ErrorClass::Cancelled {
        return if unresolved {
            FailureDecision::Block
        } else {
            FailureDecision::Cancel
        };
    }
    let transient = matches!(failure.class, ErrorClass::Transient | ErrorClass::Resource);
    let repeatable = !unresolved || matches!(repetition, Repetition::Safe | Repetition::Keyed);
    if transient
        && !failure.invalid_output
        && repeatable
        && budget.completed_attempts < budget.max_attempts
        && budget.now_ms < budget.deadline_at_ms
    {
        FailureDecision::Retry
    } else if unresolved {
        FailureDecision::Block
    } else {
        FailureDecision::Fail
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effect_matrix_preserves_uncertainty_before_considering_delay() {
        let budget = RetryBudget {
            completed_attempts: 1,
            max_attempts: 3,
            now_ms: 100,
            deadline_at_ms: 1000,
            cancel_requested: false,
        };
        let cases = [
            (
                EffectKind::Pure,
                Repetition::Safe,
                EffectCertainty::Unknown,
                ErrorClass::Transient,
                false,
                FailureDecision::Retry,
            ),
            (
                EffectKind::Read,
                Repetition::Safe,
                EffectCertainty::Unknown,
                ErrorClass::Transient,
                false,
                FailureDecision::Retry,
            ),
            (
                EffectKind::Write,
                Repetition::Unsafe,
                EffectCertainty::NotApplied,
                ErrorClass::Transient,
                false,
                FailureDecision::Retry,
            ),
            (
                EffectKind::Write,
                Repetition::Unsafe,
                EffectCertainty::Unknown,
                ErrorClass::Transient,
                false,
                FailureDecision::Block,
            ),
            (
                EffectKind::Write,
                Repetition::Keyed,
                EffectCertainty::Unknown,
                ErrorClass::Transient,
                false,
                FailureDecision::Retry,
            ),
            (
                EffectKind::Write,
                Repetition::Safe,
                EffectCertainty::Unknown,
                ErrorClass::Resource,
                false,
                FailureDecision::Retry,
            ),
            (
                EffectKind::Write,
                Repetition::Keyed,
                EffectCertainty::Applied,
                ErrorClass::Transient,
                false,
                FailureDecision::Block,
            ),
            (
                EffectKind::Write,
                Repetition::Keyed,
                EffectCertainty::Unknown,
                ErrorClass::Transient,
                true,
                FailureDecision::Block,
            ),
            (
                EffectKind::Write,
                Repetition::Unsafe,
                EffectCertainty::NotApplied,
                ErrorClass::Rejected,
                false,
                FailureDecision::Fail,
            ),
            (
                EffectKind::Write,
                Repetition::Keyed,
                EffectCertainty::Unknown,
                ErrorClass::Internal,
                false,
                FailureDecision::Block,
            ),
            (
                EffectKind::Read,
                Repetition::Safe,
                EffectCertainty::Unknown,
                ErrorClass::Internal,
                false,
                FailureDecision::Fail,
            ),
            (
                EffectKind::Pure,
                Repetition::Safe,
                EffectCertainty::NotApplied,
                ErrorClass::Transient,
                true,
                FailureDecision::Fail,
            ),
        ];
        for (effect, repetition, certainty, class, invalid_output, expected) in cases {
            assert_eq!(
                classify_failure(
                    effect,
                    repetition,
                    AttemptFailure {
                        class,
                        certainty,
                        invalid_output
                    },
                    budget
                ),
                expected,
                "{effect:?}/{repetition:?}/{certainty:?}/{class:?}"
            );
        }
    }

    #[test]
    fn cancellation_and_exhaustion_never_erase_a_possible_write() {
        let failure = AttemptFailure {
            class: ErrorClass::Transient,
            certainty: EffectCertainty::Unknown,
            invalid_output: false,
        };
        for budget in [
            RetryBudget {
                completed_attempts: 1,
                max_attempts: 3,
                now_ms: 100,
                deadline_at_ms: 1000,
                cancel_requested: true,
            },
            RetryBudget {
                completed_attempts: 3,
                max_attempts: 3,
                now_ms: 100,
                deadline_at_ms: 1000,
                cancel_requested: false,
            },
            RetryBudget {
                completed_attempts: 1,
                max_attempts: 3,
                now_ms: 1000,
                deadline_at_ms: 1000,
                cancel_requested: false,
            },
        ] {
            assert_eq!(
                classify_failure(EffectKind::Write, Repetition::Keyed, failure, budget),
                FailureDecision::Block
            );
            let expected = if budget.cancel_requested {
                FailureDecision::Cancel
            } else {
                FailureDecision::Fail
            };
            assert_eq!(
                classify_failure(EffectKind::Read, Repetition::Safe, failure, budget),
                expected
            );
        }
    }
}
