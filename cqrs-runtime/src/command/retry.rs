use cqrs_es::AggregateError;
use std::{error::Error, future::Future, time::Duration};
use thiserror::Error;
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum AggregateConflictRetryPolicyError {
    #[error("conflict retry requires at least one attempt")]
    InvalidAttemptLimit,
    #[error("conflict backoff must be ordered and representable")]
    InvalidBackoff,
    #[error("jitter ratio must be finite and between zero and one")]
    InvalidJitter,
}
/// Controls the bounded, in-process retry of one unchanged aggregate command.
///
/// `max_attempts` counts the initial execution. Only optimistic aggregate
/// conflicts are retried; failures with an unknown commit outcome are returned.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AggregateConflictRetryPolicy {
    pub(super) max_attempts: usize,
    pub(super) initial_backoff: Duration,
    pub(super) max_backoff: Duration,
    pub(super) jitter_ratio: f64,
}

impl AggregateConflictRetryPolicy {
    /// Creates an explicit retry policy. `jitter_ratio` is symmetric, so `0.2`
    /// means ±20%.
    pub fn new(
        max_attempts: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
        jitter_ratio: f64,
    ) -> Result<Self, AggregateConflictRetryPolicyError> {
        if max_attempts == 0 {
            return Err(AggregateConflictRetryPolicyError::InvalidAttemptLimit);
        }
        if initial_backoff > max_backoff || max_backoff.as_millis() > i64::MAX as u128 {
            return Err(AggregateConflictRetryPolicyError::InvalidBackoff);
        }
        if !jitter_ratio.is_finite() || !(0.0..=1.0).contains(&jitter_ratio) {
            return Err(AggregateConflictRetryPolicyError::InvalidJitter);
        }
        Ok(Self {
            max_attempts,
            initial_backoff,
            max_backoff,
            jitter_ratio,
        })
    }

    pub(super) fn backoff_bounds(self, retry: usize) -> (Duration, Duration) {
        debug_assert!(retry > 0);
        let exponent = u32::try_from(retry - 1).unwrap_or(u32::MAX);
        let multiplier = 2_u32.checked_pow(exponent).unwrap_or(u32::MAX);
        let backoff = self.initial_backoff.saturating_mul(multiplier).min(self.max_backoff);
        let lower = backoff.mul_f64(1.0 - self.jitter_ratio).min(backoff);
        let upper = backoff.mul_f64(1.0 + self.jitter_ratio).min(self.max_backoff).max(backoff);
        (lower, upper)
    }

    pub(super) fn backoff(self, retry: usize) -> Duration {
        let (lower, upper) = self.backoff_bounds(retry);
        if lower == upper {
            lower
        } else {
            Duration::from_secs_f64(rand::random_range(lower.as_secs_f64()..=upper.as_secs_f64())).clamp(lower, upper)
        }
    }
}

impl Default for AggregateConflictRetryPolicy {
    fn default() -> Self {
        Self::new(3, Duration::from_millis(5), Duration::from_millis(50), 0.2).expect("valid default conflict retry policy")
    }
}

pub(super) async fn retry_on_aggregate_conflict<T, D, Operation, OperationFuture>(
    policy: AggregateConflictRetryPolicy,
    mut operation: Operation,
) -> Result<T, AggregateError<D>>
where
    D: Error,
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, AggregateError<D>>>,
{
    for attempt in 1..=policy.max_attempts {
        let result = operation().await;
        if matches!(result, Err(AggregateError::AggregateConflict)) && attempt < policy.max_attempts {
            tokio::time::sleep(policy.backoff(attempt)).await;
            continue;
        }
        return result;
    }
    unreachable!("aggregate conflict retry policy always has at least one attempt")
}
