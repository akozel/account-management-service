//! Task execution within fixed leases and bounded storage operations.
//!
//! Local retries/backoff share one handler invocation budget. Before execution,
//! the task must retain that full budget plus settlement time and the fixed
//! safety margin. A stale budget triggers retry without running the handler,
//! or durable recovery if even safe settlement no longer fits. Absolute local
//! deadlines complement the database's token/expiry fencing; they do not renew
//! ownership or cancel a side effect already accepted by a remote system.

use super::{
    ClaimedTask, LeaseRecoveryReport, OutboxError, OutboxQueue, SettlementRequest, SettlementResult, TaskFormat, TaskHandler,
    TaskHandlerError, redelivery_backoff,
};
use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, sleep_until, timeout, timeout_at};

#[async_trait]
pub trait OutboxTaskCoordinator: Send + Sync {
    fn formats(&self) -> Vec<TaskFormat>;
    /// Effective storage budget used by worker startup to validate the full lease.
    fn storage_timeout(&self) -> Duration;
    async fn claim(&self, owner: &str, limit: usize, lease: Duration) -> Result<Vec<ClaimedTask>, OutboxError>;
    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, OutboxError>;
    /// Apply the invocation and remaining-lease budgets, then settle its outcome.
    /// A settled result can be retry/pending; it is not necessarily completion.
    /// LeaseBudgetExhausted skips unsafe execution/settlement and leaves recovery
    /// to the durable queue. The handler never owns or renews platform leases.
    async fn execute_and_settle(
        &self,
        handler: &mut dyn TaskHandler,
        task: ClaimedTask,
        handler_timeout: Duration,
    ) -> Result<SettlementResult, OutboxError>;
}

mod config;
mod lease_budget;
pub use config::{OutboxTaskCoordinatorConfig, OutboxTaskCoordinatorConfigError};
use lease_budget::{ExecutionPlan, execution_plan};

/// Compose a coordinator with default storage budgets, without reading the environment.
pub fn outbox_task_coordinator(queue: Arc<dyn OutboxQueue>, formats: Vec<TaskFormat>) -> Arc<dyn OutboxTaskCoordinator> {
    Arc::new(DefaultOutboxTaskCoordinator::new(
        queue,
        formats,
        OutboxTaskCoordinatorConfig::default(),
    ))
}

/// Compose a coordinator with validated explicit budgets and registry-derived formats.
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
    next_format_start: AtomicUsize,
    storage_timeout: Duration,
}

impl DefaultOutboxTaskCoordinator {
    pub(super) fn new(queue: Arc<dyn OutboxQueue>, formats: Vec<TaskFormat>, config: OutboxTaskCoordinatorConfig) -> Self {
        Self {
            queue,
            formats,
            next_format_start: AtomicUsize::new(0),
            storage_timeout: config.storage_timeout(),
        }
    }

    async fn settle_before(
        &self,
        task: &ClaimedTask,
        outcome: SettlementRequest,
        settlement_deadline: Instant,
    ) -> Result<SettlementResult, OutboxError> {
        let now = Instant::now();
        if now >= settlement_deadline {
            return Err(OutboxError::LeaseBudgetExhausted);
        }
        let deadline = (now + self.storage_timeout).min(settlement_deadline);
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
        let start = self.next_format_start.fetch_add(1, Ordering::Relaxed);
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

    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, OutboxError> {
        timeout(self.storage_timeout, self.queue.recover_expired(limit))
            .await
            .unwrap_or(Err(OutboxError::TimedOut))
    }

    async fn execute_and_settle(
        &self,
        handler: &mut dyn TaskHandler,
        task: ClaimedTask,
        handler_timeout: Duration,
    ) -> Result<SettlementResult, OutboxError> {
        let (handler_deadline, settlement_deadline) =
            match execution_plan(Instant::now(), task.lease().deadline(), handler_timeout, self.storage_timeout)? {
                ExecutionPlan::Run {
                    handler_deadline,
                    settlement_deadline,
                } => (handler_deadline, settlement_deadline),
                ExecutionPlan::RetryOnly { settlement_deadline } => {
                    return self
                        .settle_before(
                            &task,
                            SettlementRequest::Retry {
                                reason: "insufficient lease budget before handler execution".to_owned(),
                                delay: redelivery_backoff(task.delivery().attempt()),
                            },
                            settlement_deadline,
                        )
                        .await;
                }
                ExecutionPlan::RecoverOnly => return Err(OutboxError::LeaseBudgetExhausted),
            };
        let outcome = tokio::select! {
            biased;
            _ = sleep_until(handler_deadline) => SettlementRequest::Retry {
                reason: "handler timed out".to_owned(),
                delay: redelivery_backoff(task.delivery().attempt()),
            },
            result = handler.handle(task.delivery()) => match result {
                Ok(()) => SettlementRequest::Completed,
                Err(TaskHandlerError::Permanent { reason }) => SettlementRequest::Failed { reason },
                Err(TaskHandlerError::Retry { reason, retry_after }) => SettlementRequest::Retry {
                    reason,
                    delay: retry_after.unwrap_or_else(|| redelivery_backoff(task.delivery().attempt())),
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
    use crate::outbox::{LEASE_SAFETY_MARGIN, TaskDelivery};

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

        async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
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
        let service = outbox_task_coordinator_with_config(
            queue.clone(),
            vec![format()],
            OutboxTaskCoordinatorConfig::new(Duration::from_secs(5)).unwrap(),
        )
        .unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handler = ProbeHandler {
            calls: calls.clone(),
            simulate_pause: false,
        };
        let mut delayed = claimed();
        delayed = with_deadline(delayed, Instant::now() + Duration::from_secs(39));
        assert!(
            service
                .execute_and_settle(&mut handler, delayed, Duration::from_secs(30))
                .await
                .unwrap()
                .is_applied()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            matches!(&queue.outcomes.lock().unwrap()[0].1, SettlementRequest::Retry { reason, .. }
                if reason == "insufficient lease budget before handler execution")
        );
        let mut expired = claimed();
        expired = with_deadline(expired, Instant::now());
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
        task = with_deadline(task, Instant::now() + LEASE_SAFETY_MARGIN + Duration::from_secs(1));
        let started = Instant::now();
        assert_eq!(
            service.execute_and_settle(&mut handler, task, Duration::from_secs(30)).await,
            Err(OutboxError::TimedOut)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(queue.outcomes.lock().unwrap()[0].1, SettlementRequest::Retry { .. }));
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
            Err(OutboxError::from(crate::outbox::QueueRequestError::InvalidExecutionBudget))
        );
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn storage_deadlines_bound_claim_recovery_and_settlement_without_a_terminal_outcome() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator_with_config(
            queue.clone(),
            vec![format()],
            OutboxTaskCoordinatorConfig::new(Duration::from_secs(3)).unwrap(),
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
        let first_id = first.delivery().id();
        let mut second = claimed();
        second = with_format(second, TaskFormat::new("second", 1).unwrap());
        let second_format = second.delivery().format().clone();
        queue.incoming.lock().unwrap().extend([first, second]);
        let service = outbox_task_coordinator_with_config(
            queue.clone(),
            vec![format(), second_format],
            OutboxTaskCoordinatorConfig::new(Duration::from_secs(3)).unwrap(),
        )
        .unwrap();
        let started = Instant::now();
        let tasks = service.claim("test", 2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].delivery().id(), first_id);
        assert_eq!(tasks[0].lease().deadline(), started + Duration::from_secs(60));
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
                Err(TaskHandlerError::Retry {
                    reason: "throttle".into(),
                    retry_after: Some(Duration::from_secs(300)),
                }),
                false,
            ),
            (
                Err(TaskHandlerError::Retry {
                    reason: "network".into(),
                    retry_after: None,
                }),
                false,
            ),
            (
                Err(TaskHandlerError::Permanent {
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
                    .is_applied()
            );
        }
        {
            let outcomes = queue.outcomes.lock().unwrap();
            assert_eq!(outcomes[0].1, SettlementRequest::Completed);
            assert_eq!(
                outcomes[1].1,
                SettlementRequest::Retry {
                    delay: Duration::from_secs(300),
                    reason: "throttle".into()
                }
            );
            assert!(
                matches!(&outcomes[2].1, SettlementRequest::Retry { delay, .. } if *delay >= Duration::from_secs(8) && *delay <= Duration::from_secs(12))
            );
            assert_eq!(
                outcomes[3].1,
                SettlementRequest::Failed {
                    reason: "invalid".into()
                }
            );
            assert!(matches!(&outcomes[4].1, SettlementRequest::Retry { reason, .. } if reason == "handler timed out"));
        }
        let mut handler = handler();
        queue.lost_owner.store(true, Ordering::SeqCst);
        assert!(
            !service
                .execute_and_settle(&mut handler, claimed(), Duration::from_secs(30))
                .await
                .unwrap()
                .is_applied()
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
        let future = TaskFormat::new("future", 2).unwrap();
        let mut second = claimed();
        second = with_format(second, future.clone());
        queue.incoming.lock().unwrap().extend([claimed(), second]);
        let service = outbox_task_coordinator(queue.clone(), vec![format(), future.clone()]);
        assert_eq!(service.formats(), vec![format(), future.clone()]);
        assert_eq!(service.claim("test", 1, Duration::from_secs(60)).await.unwrap().len(), 1);
        assert_eq!(
            service.claim("test", 1, Duration::from_secs(60)).await.unwrap()[0]
                .delivery()
                .format(),
            &future
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
        let unsupported = TaskFormat::new("temporarily_unavailable", 1).unwrap();
        *queue.failing_format.lock().unwrap() = Some(unsupported.clone());
        let task = claimed();
        let id = task.delivery().id();
        queue.incoming.lock().unwrap().push_back(task);
        let service = outbox_task_coordinator(queue, vec![format(), unsupported]);
        let result = service.claim("test", 2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].delivery().id(), id);
    }
}
