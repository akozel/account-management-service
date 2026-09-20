pub mod email_reservation_gateway;
pub mod user_account_gateway;

use std::{error::Error, future::Future, time::Duration};

use cqrs_es::AggregateError;

use crate::application::CommandGatewayError;

/// Controls the bounded, in-process retry of one unchanged aggregate command.
///
/// `max_attempts` is the total number of command executions, including the
/// initial one. A gateway must capture one immutable command and one immutable
/// set of prebuilt outbox tasks around [`retry_on_aggregate_conflict`], so a
/// retry reloads the aggregate and repeats that same intended write.
///
/// This policy deliberately does not retry connection, deserialization, or
/// unexpected failures: after such an error the result of a commit may be
/// unknown. It also does not make a later, client-initiated HTTP retry
/// idempotent; that remains part of the public API and domain contract.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AggregateConflictRetryPolicy {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
    jitter_ratio: f64,
}

impl AggregateConflictRetryPolicy {
    /// Creates an explicit retry policy.
    ///
    /// `jitter_ratio` is applied symmetrically, so `0.2` means ±20%.
    pub fn new(max_attempts: usize, initial_backoff: Duration, max_backoff: Duration, jitter_ratio: f64) -> Self {
        assert!(
            max_attempts > 0,
            "aggregate conflict retry policy requires at least one attempt"
        );
        assert!(initial_backoff <= max_backoff, "initial backoff must not exceed the cap");
        assert!(
            jitter_ratio.is_finite() && (0.0..=1.0).contains(&jitter_ratio),
            "jitter ratio must be finite and between zero and one"
        );
        Self {
            max_attempts,
            initial_backoff,
            max_backoff,
            jitter_ratio,
        }
    }

    fn backoff_bounds(self, retry: usize) -> (Duration, Duration) {
        debug_assert!(retry > 0);
        let exponent = u32::try_from(retry - 1).unwrap_or(u32::MAX);
        let multiplier = 2_u32.checked_pow(exponent).unwrap_or(u32::MAX);
        let backoff = self.initial_backoff.saturating_mul(multiplier).min(self.max_backoff);
        let lower = backoff.mul_f64(1.0 - self.jitter_ratio);
        let upper = backoff.mul_f64(1.0 + self.jitter_ratio).min(self.max_backoff);
        (lower, upper)
    }

    fn backoff(self, retry: usize) -> Duration {
        let (lower, upper) = self.backoff_bounds(retry);
        if lower == upper {
            lower
        } else {
            Duration::from_secs_f64(rand::random_range(lower.as_secs_f64()..=upper.as_secs_f64()))
        }
    }
}

impl Default for AggregateConflictRetryPolicy {
    fn default() -> Self {
        Self::new(3, Duration::from_millis(5), Duration::from_millis(50), 0.2)
    }
}

/// Repeats one operation only when optimistic aggregate persistence conflicts.
///
/// Callers must make the operation execute the same command with the same
/// prebuilt outbox tasks on every call. The helper returns the operation's
/// original success or error type and never retries domain rejections or
/// technical failures whose commit outcome may be unknown. This fast retry is
/// internal to one command dispatch and does not protect a separate HTTP retry
/// made by a client.
pub async fn retry_on_aggregate_conflict<T, D, Operation, OperationFuture>(
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

pub(super) fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::UserError(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_) => CommandGatewayError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::domain::{EmailReservationError, UserAccountError};

    #[test]
    fn maps_framework_errors_for_both_aggregates() {
        assert_eq!(
            map_aggregate_error::<EmailReservationError>(AggregateError::AggregateConflict),
            CommandGatewayError::Conflict
        );
        assert_eq!(
            map_aggregate_error(AggregateError::UserError(UserAccountError::EmptyFirstName)),
            CommandGatewayError::Domain(UserAccountError::EmptyFirstName)
        );

        for (error, expected_error) in [
            (
                AggregateError::UserError(EmailReservationError::NotReserved),
                CommandGatewayError::Domain(EmailReservationError::NotReserved),
            ),
            (
                AggregateError::UserError(EmailReservationError::AlreadyVerified),
                CommandGatewayError::Domain(EmailReservationError::AlreadyVerified),
            ),
            (
                AggregateError::DatabaseConnectionError(Box::new(io::Error::other("db"))),
                CommandGatewayError::Unavailable,
            ),
            (
                AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
                CommandGatewayError::Unavailable,
            ),
            (
                AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
                CommandGatewayError::Unavailable,
            ),
        ] {
            assert_eq!(map_aggregate_error(error), expected_error);
        }
    }

    fn no_delay_policy(max_attempts: usize) -> AggregateConflictRetryPolicy {
        AggregateConflictRetryPolicy::new(max_attempts, Duration::ZERO, Duration::ZERO, 0.0)
    }

    #[tokio::test]
    async fn retry_helper_returns_immediate_success_without_an_extra_call() {
        let calls = AtomicUsize::new(0);

        let result = retry_on_aggregate_conflict(no_delay_policy(3), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, AggregateError<EmailReservationError>>("accepted")
        })
        .await;

        assert_eq!(result.unwrap(), "accepted");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_helper_succeeds_after_two_conflicts() {
        let calls = AtomicUsize::new(0);

        let result = retry_on_aggregate_conflict(no_delay_policy(3), || async {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(AggregateError::AggregateConflict)
            } else {
                Ok::<_, AggregateError<EmailReservationError>>(42)
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_helper_exhausts_on_exactly_the_third_attempt() {
        let calls = AtomicUsize::new(0);

        let result = retry_on_aggregate_conflict(no_delay_policy(3), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(AggregateError::<EmailReservationError>::AggregateConflict)
        })
        .await;

        assert!(matches!(result, Err(AggregateError::AggregateConflict)));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_helper_does_not_retry_domain_or_technical_errors() {
        let cases = [
            AggregateError::UserError(EmailReservationError::NotReserved),
            AggregateError::DatabaseConnectionError(Box::new(io::Error::other("connection"))),
            AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
        ];

        for error in cases {
            let calls = AtomicUsize::new(0);
            let mut error = Some(error);
            let result = retry_on_aggregate_conflict(no_delay_policy(3), || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(error.take().expect("operation is called once")))
            })
            .await;

            assert!(!matches!(result, Err(AggregateError::AggregateConflict)));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn default_policy_has_bounded_exponential_jitter() {
        let policy = AggregateConflictRetryPolicy::default();

        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.backoff_bounds(1), (Duration::from_millis(4), Duration::from_millis(6)));
        assert_eq!(
            policy.backoff_bounds(2),
            (Duration::from_millis(8), Duration::from_millis(12))
        );
        assert_eq!(
            policy.backoff_bounds(5),
            (Duration::from_millis(40), Duration::from_millis(50))
        );
        for retry in 1..=5 {
            let bounds = policy.backoff_bounds(retry);
            for _ in 0..100 {
                assert!((bounds.0..=bounds.1).contains(&policy.backoff(retry)));
            }
        }
    }

    #[test]
    #[should_panic(expected = "at least one attempt")]
    fn retry_policy_rejects_zero_attempts() {
        AggregateConflictRetryPolicy::new(0, Duration::ZERO, Duration::ZERO, 0.0);
    }
}
