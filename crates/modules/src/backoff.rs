use workflow_forge_protocol::{BackoffPolicy, BackoffRequest};

/// Exponential delay with full jitter. The caller owns retry authorization.
#[derive(Default)]
pub struct ExponentialBackoff;
impl BackoffPolicy for ExponentialBackoff {
    fn delay_ms(&self, request: BackoffRequest<'_>) -> u64 {
        let multiplier = 1_u64
            .checked_shl(request.completed_attempt.saturating_sub(1))
            .unwrap_or(u64::MAX);
        let bound = request
            .policy
            .initial_delay_ms
            .saturating_mul(multiplier)
            .min(request.policy.max_delay_ms);
        if request.policy.jitter {
            ((bound as u128 * request.entropy as u128) / u64::MAX as u128) as u64
        } else {
            bound
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use workflow_forge_protocol::RetryPolicy;

    #[test]
    fn exponential_delay_saturates_and_jitter_stays_within_its_bound() {
        let mut policy = RetryPolicy {
            max_attempts: 5,
            initial_delay_ms: 100,
            max_delay_ms: 700,
            jitter: false,
        };
        let strategy = ExponentialBackoff;
        for (attempt, expected) in [(1, 100), (2, 200), (3, 400), (4, 700), (u32::MAX, 700)] {
            assert_eq!(
                strategy.delay_ms(BackoffRequest {
                    policy: &policy,
                    completed_attempt: attempt,
                    entropy: 0
                }),
                expected
            );
        }
        policy.jitter = true;
        assert_eq!(
            strategy.delay_ms(BackoffRequest {
                policy: &policy,
                completed_attempt: 3,
                entropy: 0
            }),
            0
        );
        assert_eq!(
            strategy.delay_ms(BackoffRequest {
                policy: &policy,
                completed_attempt: 3,
                entropy: u64::MAX
            }),
            400
        );
        assert!(policy.validate(4).is_err());
        assert!(policy.validate(5).is_ok());
    }
}
