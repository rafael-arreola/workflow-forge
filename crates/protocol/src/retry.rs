use crate::ForgeError;
use serde::{Deserialize, Serialize};

/// A total attempt budget, including the first call. Delays do not authorize retry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter: bool,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            initial_delay_ms: 100,
            max_delay_ms: 30_000,
            jitter: true,
        }
    }
}
impl RetryPolicy {
    pub fn validate(&self, host_max_attempts: u32) -> Result<(), ForgeError> {
        if self.max_attempts == 0
            || self.max_attempts > host_max_attempts
            || self.initial_delay_ms > self.max_delay_ms
        {
            Err(ForgeError::new(
                "definition.invalid",
                "Retry policy exceeds host limits or has an invalid delay range",
            ))
        } else {
            Ok(())
        }
    }
}

/// The coordinator supplies entropy and persists the chosen due time before retry.
/// A strategy never changes attempt identity, effect certainty or the retry budget.
pub struct BackoffRequest<'a> {
    pub policy: &'a RetryPolicy,
    pub completed_attempt: u32,
    pub entropy: u64,
}
pub trait BackoffPolicy: Send + Sync {
    fn delay_ms(&self, request: BackoffRequest<'_>) -> u64;
}
