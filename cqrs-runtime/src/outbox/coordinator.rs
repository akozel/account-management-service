//! Task execution within fixed leases and bounded storage operations.
//!
//! Local retries/backoff share one handler invocation budget. Before execution,
//! the task must retain that full budget plus settlement time and the fixed
//! safety margin. A stale budget triggers retry without running the handler,
//! or durable recovery if even safe settlement no longer fits. Absolute local
//! deadlines complement the database's token/expiry fencing; they do not renew
//! ownership or cancel a side effect already accepted by a remote system.

use super::{
    error::OutboxError,
    queue::{ClaimedTask, LEASE_SAFETY_MARGIN, OutboxQueue, RecoveryResult, SettleResult, TaskOutcome},
    task::TaskFormat,
    task_handler::{TaskExecutionError, TaskHandler},
};
use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::time::{Instant, sleep_until, timeout, timeout_at};

#[async_trait]
pub trait OutboxTaskCoordinator: Send + Sync {
    fn formats(&self) -> Vec<TaskFormat>;
    /// Effective storage budget used by worker startup to validate the full lease.
    fn storage_timeout(&self) -> Duration;
    async fn claim(&self, owner: &str, limit: usize, lease: Duration) -> Result<Vec<ClaimedTask>, OutboxError>;
    async fn recover_expired(&self, limit: usize) -> Result<RecoveryResult, OutboxError>;
    async fn execute_and_settle(
        &self,
        handler: &mut dyn TaskHandler,
        task: ClaimedTask,
        timeout: Duration,
    ) -> Result<SettleResult, OutboxError>;
}

#[derive(Clone, Debug)]
pub struct OutboxTaskCoordinatorConfig {
    /// End-to-end budget for claim (all formats), recovery, or settlement.
    pub storage_timeout: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid outbox coordinator configuration: {0}")]
pub struct OutboxTaskCoordinatorConfigError(pub &'static str);

impl Default for OutboxTaskCoordinatorConfig {
    fn default() -> Self {
        Self {
            storage_timeout: Duration::from_secs(5),
        }
    }
}

impl OutboxTaskCoordinatorConfig {
    pub fn from_env() -> Result<Self, OutboxTaskCoordinatorConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, OutboxTaskCoordinatorConfigError> {
        let millis = match lookup("OUTBOX_STORAGE_TIMEOUT_MS") {
            Some(value) => value
                .parse::<u64>()
                .map_err(|_| OutboxTaskCoordinatorConfigError("OUTBOX_STORAGE_TIMEOUT_MS"))?,
            None => 5_000,
        };
        let config = Self {
            storage_timeout: Duration::from_millis(millis),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), OutboxTaskCoordinatorConfigError> {
        if self.storage_timeout.is_zero() || self.storage_timeout.as_millis() > i64::MAX as u128 {
            return Err(OutboxTaskCoordinatorConfigError(
                "positive, representable storage timeout required",
            ));
        }
        Ok(())
    }
}

/// A bounded, jittered exponential delay. Also used for expired leases.
pub fn retry_delay(attempt: i32) -> Duration {
    let exponent = attempt.saturating_sub(1).clamp(0, 6) as u32;
    let base = (5_u64 * 2_u64.pow(exponent)).min(300);
    let jitter = rand::random_range(0.8..=1.2);
    Duration::from_secs_f64((base as f64 * jitter).min(300.0))
}

pub fn outbox_task_coordinator(queue: Arc<dyn OutboxQueue>, formats: Vec<TaskFormat>) -> Arc<dyn OutboxTaskCoordinator> {
    Arc::new(DefaultOutboxTaskCoordinator::new(
        queue,
        formats,
        OutboxTaskCoordinatorConfig::default(),
    ))
}

pub fn outbox_task_coordinator_with_config(
    queue: Arc<dyn OutboxQueue>,
    formats: Vec<TaskFormat>,
    config: OutboxTaskCoordinatorConfig,
) -> Result<Arc<dyn OutboxTaskCoordinator>, OutboxTaskCoordinatorConfigError> {
    config.validate()?;
    Ok(Arc::new(DefaultOutboxTaskCoordinator::new(queue, formats, config)))
}

pub(super) struct DefaultOutboxTaskCoordinator {
    queue: Arc<dyn OutboxQueue>,
    formats: Vec<TaskFormat>,
    next_format: AtomicUsize,
    storage_timeout: Duration,
}

impl DefaultOutboxTaskCoordinator {
    pub(super) fn new(queue: Arc<dyn OutboxQueue>, formats: Vec<TaskFormat>, config: OutboxTaskCoordinatorConfig) -> Self {
        Self {
            queue,
            formats,
            next_format: AtomicUsize::new(0),
            storage_timeout: config.storage_timeout,
        }
    }

    async fn settle_before(
        &self,
        task: &ClaimedTask,
        outcome: TaskOutcome,
        lease_deadline: Instant,
    ) -> Result<SettleResult, OutboxError> {
        let now = Instant::now();
        if now >= lease_deadline {
            return Err(OutboxError::LeaseBudgetExhausted);
        }
        let deadline = (now + self.storage_timeout).min(lease_deadline);
        tokio::select! {
            // Check the deadline before polling storage, including after a pause.
            biased;
            _ = sleep_until(deadline) => Err(OutboxError::TimedOut),
            result = self.queue.settle(task, outcome) => result,
        }
    }
}

#[async_trait]
impl OutboxTaskCoordinator for DefaultOutboxTaskCoordinator {
    fn formats(&self) -> Vec<TaskFormat> {
        self.formats.clone()
    }

    fn storage_timeout(&self) -> Duration {
        self.storage_timeout
    }

    async fn claim(&self, owner: &str, limit: usize, lease: Duration) -> Result<Vec<ClaimedTask>, OutboxError> {
        let deadline = Instant::now() + self.storage_timeout;
        let mut claimed = Vec::new();
        let count = self.formats.len();
        let start = self.next_format.fetch_add(1, Ordering::Relaxed);
        for offset in 0..count {
            if claimed.len() == limit {
                break;
            }
            let format = &self.formats[(start.wrapping_add(offset)) % count];
            let result = timeout_at(deadline, self.queue.claim(format, owner, limit - claimed.len(), lease))
                .await
                .unwrap_or(Err(OutboxError::TimedOut));
            match result {
                Ok(tasks) => claimed.extend(tasks),
                Err(_) if !claimed.is_empty() => break,
                Err(error) => return Err(error),
            }
        }
        Ok(claimed)
    }

    async fn recover_expired(&self, limit: usize) -> Result<RecoveryResult, OutboxError> {
        timeout(self.storage_timeout, self.queue.recover_expired(limit))
            .await
            .unwrap_or(Err(OutboxError::TimedOut))
    }

    async fn execute_and_settle(
        &self,
        handler: &mut dyn TaskHandler,
        task: ClaimedTask,
        timeout: Duration,
    ) -> Result<SettleResult, OutboxError> {
        // Handler retries and backoff share one invocation budget. Start only
        // when that full budget AND settlement fit before this task's lease.
        let now = Instant::now();
        let settlement_deadline = task
            .lease_deadline
            .checked_sub(LEASE_SAFETY_MARGIN)
            .ok_or(OutboxError::LeaseBudgetExhausted)?;
        let required = timeout.checked_add(self.storage_timeout).ok_or(OutboxError::InvalidTask)?;
        if settlement_deadline.saturating_duration_since(now) < required {
            return self
                .settle_before(
                    &task,
                    TaskOutcome::Retry {
                        reason: "insufficient lease budget before handler execution".to_owned(),
                        delay: retry_delay(task.attempt),
                    },
                    settlement_deadline,
                )
                .await;
        }
        let handler_deadline = now.checked_add(timeout).ok_or(OutboxError::InvalidTask)?;
        let outcome = tokio::select! {
            biased;
            _ = sleep_until(handler_deadline) => TaskOutcome::Retry {
                reason: "handler timed out".to_owned(),
                delay: retry_delay(task.attempt),
            },
            result = handler.handle(&task) => match result {
                Ok(()) => TaskOutcome::Completed,
                Err(TaskExecutionError::Permanent { reason }) => TaskOutcome::Failed { reason },
                Err(TaskExecutionError::Retry { reason, retry_after }) => TaskOutcome::Retry {
                    reason,
                    delay: retry_after.unwrap_or_else(|| retry_delay(task.attempt)),
                },
            },
        };
        // Retain a margin before local expiry; queue-side token and database
        // clock checks remain authoritative, including after clock changes.
        self.settle_before(&task, outcome, settlement_deadline).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::test_support::*;
    use std::{
        sync::{Arc, atomic::Ordering},
        time::Duration,
    };

    struct ProbeHandler {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        simulate_pause: bool,
    }

    #[async_trait]
    impl TaskHandler for ProbeHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }

        async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.simulate_pause {
                tokio::time::advance(Duration::from_secs(120)).await;
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_task_retries_without_starting_handler_and_expired_task_waits_for_recovery() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handler = ProbeHandler {
            calls: calls.clone(),
            simulate_pause: false,
        };
        let mut delayed = claimed();
        delayed.lease_deadline = Instant::now() + Duration::from_secs(39);
        assert!(
            service
                .execute_and_settle(&mut handler, delayed, Duration::from_secs(30))
                .await
                .unwrap()
                .is_settled()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            matches!(&queue.outcomes.lock().unwrap()[0].1, TaskOutcome::Retry { reason, .. }
            if reason == "insufficient lease budget before handler execution")
        );
        let mut expired = claimed();
        expired.lease_deadline = Instant::now();
        assert_eq!(
            service
                .execute_and_settle(&mut handler, expired, Duration::from_secs(30))
                .await,
            Err(OutboxError::LeaseBudgetExhausted)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
        assert_eq!(handler.formats(), vec![format()]);
    }

    #[tokio::test(start_paused = true)]
    async fn lease_margin_shortens_settlement_deadline_without_starting_handler() {
        let queue = Arc::new(Queue::default());
        queue.hang_settle.store(true, Ordering::SeqCst);
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handler = ProbeHandler {
            calls: calls.clone(),
            simulate_pause: false,
        };
        let mut task = claimed();
        task.lease_deadline = Instant::now() + LEASE_SAFETY_MARGIN + Duration::from_secs(1);
        let started = Instant::now();
        assert_eq!(
            service.execute_and_settle(&mut handler, task, Duration::from_secs(30)).await,
            Err(OutboxError::TimedOut)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(queue.outcomes.lock().unwrap()[0].1, TaskOutcome::Retry { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn process_pause_exhausting_lease_prevents_a_late_acknowledgement() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handler = ProbeHandler {
            calls: calls.clone(),
            simulate_pause: true,
        };
        assert_eq!(
            service
                .execute_and_settle(&mut handler, claimed(), Duration::from_secs(30))
                .await,
            Err(OutboxError::LeaseBudgetExhausted)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unrepresentable_handler_budget_is_rejected_before_execution() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        assert_eq!(
            service.execute_and_settle(&mut handler(), claimed(), Duration::MAX).await,
            Err(OutboxError::InvalidTask)
        );
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    #[test]
    fn storage_timeout_config_has_defaults_overrides_and_rejects_invalid_budgets() {
        assert_eq!(
            OutboxTaskCoordinatorConfig::from_lookup(|_| None).unwrap().storage_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(
            OutboxTaskCoordinatorConfig::from_lookup(|_| Some("750".into()))
                .unwrap()
                .storage_timeout,
            Duration::from_millis(750)
        );
        for value in ["0", "invalid", "-1", "9223372036854775808", "18446744073709551616"] {
            assert!(OutboxTaskCoordinatorConfig::from_lookup(|_| Some(value.into())).is_err());
        }
        for storage_timeout in [Duration::ZERO, Duration::MAX] {
            assert!(
                outbox_task_coordinator_with_config(
                    Arc::new(Queue::default()),
                    vec![format()],
                    OutboxTaskCoordinatorConfig { storage_timeout }
                )
                .is_err()
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn storage_deadlines_bound_claim_recovery_and_settlement_without_a_terminal_outcome() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator_with_config(
            queue.clone(),
            vec![format()],
            OutboxTaskCoordinatorConfig {
                storage_timeout: Duration::from_secs(3),
            },
        )
        .unwrap();
        queue.hang_claim.store(true, Ordering::SeqCst);
        let started = Instant::now();
        assert_eq!(
            service.claim("test", 1, Duration::from_secs(60)).await.unwrap_err(),
            OutboxError::TimedOut
        );
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        queue.hang_recovery.store(true, Ordering::SeqCst);
        let started = Instant::now();
        assert_eq!(service.recover_expired(1).await.unwrap_err(), OutboxError::TimedOut);
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        queue.hang_settle.store(true, Ordering::SeqCst);
        let started = Instant::now();
        assert_eq!(
            service
                .execute_and_settle(&mut handler(), claimed(), Duration::from_secs(30))
                .await,
            Err(OutboxError::TimedOut)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn one_claim_budget_spans_formats_and_preserves_already_confirmed_tasks() {
        let queue = Arc::new(Queue::default());
        *queue.claim_delay.lock().unwrap() = Duration::from_secs(2);
        let first = claimed();
        let first_id = first.id;
        let mut second = claimed();
        second.format.task_type = "second".into();
        let second_format = second.format.clone();
        queue.incoming.lock().unwrap().extend([first, second]);
        let service = outbox_task_coordinator_with_config(
            queue.clone(),
            vec![format(), second_format],
            OutboxTaskCoordinatorConfig {
                storage_timeout: Duration::from_secs(3),
            },
        )
        .unwrap();
        let started = Instant::now();
        let tasks = service.claim("test", 2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, first_id);
        assert_eq!(tasks[0].lease_deadline, started + Duration::from_secs(60));
        assert_eq!(queue.incoming.lock().unwrap().len(), 1);
        assert_eq!(queue.requested.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_classifies_success_errors_and_timeout_and_preserves_ownership_result() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        for (result, hang) in [
            (Ok(()), false),
            (
                Err(TaskExecutionError::Retry {
                    reason: "throttle".into(),
                    retry_after: Some(Duration::from_secs(300)),
                }),
                false,
            ),
            (
                Err(TaskExecutionError::Retry {
                    reason: "network".into(),
                    retry_after: None,
                }),
                false,
            ),
            (
                Err(TaskExecutionError::Permanent {
                    reason: "invalid".into(),
                }),
                false,
            ),
            (Ok(()), true),
        ] {
            let mut handler = handler();
            handler.result = result;
            handler.hang = hang;
            assert!(
                service
                    .execute_and_settle(&mut handler, claimed(), Duration::from_secs(30))
                    .await
                    .unwrap()
                    .is_settled()
            );
        }
        {
            let outcomes = queue.outcomes.lock().unwrap();
            assert_eq!(outcomes[0].1, TaskOutcome::Completed);
            assert_eq!(
                outcomes[1].1,
                TaskOutcome::Retry {
                    delay: Duration::from_secs(300),
                    reason: "throttle".into()
                }
            );
            assert!(
                matches!(&outcomes[2].1, TaskOutcome::Retry { delay, .. } if *delay >= Duration::from_secs(8) && *delay <= Duration::from_secs(12))
            );
            assert_eq!(
                outcomes[3].1,
                TaskOutcome::Failed {
                    reason: "invalid".into()
                }
            );
            assert!(matches!(&outcomes[4].1, TaskOutcome::Retry { reason, .. } if reason == "handler timed out"));
        }
        let mut handler = handler();
        queue.lost_owner.store(true, Ordering::SeqCst);
        assert!(
            !service
                .execute_and_settle(&mut handler, claimed(), Duration::from_secs(30))
                .await
                .unwrap()
                .is_settled()
        );
        queue.settle_error.store(true, Ordering::SeqCst);
        assert_eq!(
            service
                .execute_and_settle(&mut handler, claimed(), Duration::from_secs(30))
                .await,
            Err(OutboxError::Unavailable)
        );
    }

    #[tokio::test]
    async fn claiming_rotates_formats_and_respects_capacity() {
        let queue = Arc::new(Queue::default());
        let future = TaskFormat {
            task_type: "future".into(),
            task_version: 2,
        };
        let mut second = claimed();
        second.format = future.clone();
        queue.incoming.lock().unwrap().extend([claimed(), second]);
        let service = outbox_task_coordinator(queue.clone(), vec![format(), future.clone()]);
        assert_eq!(service.formats(), vec![format(), future.clone()]);
        assert_eq!(service.claim("test", 1, Duration::from_secs(60)).await.unwrap().len(), 1);
        assert_eq!(
            service.claim("test", 1, Duration::from_secs(60)).await.unwrap()[0].format,
            future
        );
        assert_eq!(*queue.requested.lock().unwrap(), vec![format(), future]);
        assert!(service.claim("test", 0, Duration::from_secs(60)).await.unwrap().is_empty());
        assert!(service.claim("test", 3, Duration::from_secs(60)).await.unwrap().is_empty());
        assert!(
            outbox_task_coordinator(queue.clone(), vec![])
                .claim("test", 1, Duration::from_secs(60))
                .await
                .unwrap()
                .is_empty()
        );
        queue.claim_error.store(true, Ordering::SeqCst);
        assert_eq!(
            service.claim("test", 1, Duration::from_secs(60)).await.unwrap_err(),
            OutboxError::Unavailable
        );
    }

    #[tokio::test]
    async fn a_later_format_failure_does_not_discard_already_claimed_work() {
        let queue = Arc::new(Queue::default());
        let unsupported = TaskFormat {
            task_type: "temporarily_unavailable".into(),
            task_version: 1,
        };
        *queue.failing_format.lock().unwrap() = Some(unsupported.clone());
        let task = claimed();
        let id = task.id;
        queue.incoming.lock().unwrap().push_back(task);
        let service = outbox_task_coordinator(queue, vec![format(), unsupported]);
        let result = service.claim("test", 2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, id);
    }
}
