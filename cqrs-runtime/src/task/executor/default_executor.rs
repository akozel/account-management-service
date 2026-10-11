//! Queue-backed invocation and settlement within original lease deadlines.
use super::lease_budget::{ExecutionPlan, execution_plan};
use crate::task::{
    executor::port::TaskExecutor,
    handling::{dispatcher::TaskDispatcher, handler::TaskHandlerError},
    queue::{
        error::TaskProcessingError,
        lease::LeasedTask,
        port::TaskQueue,
        retry::redelivery_backoff,
        settlement::{SettlementRequest, SettlementResult},
        storage_config::TaskStorageConfig,
    },
};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, sleep_until};
/// Compose a task executor with default storage budgets, without reading the environment.
pub fn task_executor(queue: Arc<dyn TaskQueue>) -> Arc<dyn TaskExecutor> {
    Arc::new(DefaultTaskExecutor::new(queue, TaskStorageConfig::default()))
}

/// Compose a task executor with checked end-to-end storage budgets.
pub fn task_executor_with_config(queue: Arc<dyn TaskQueue>, config: TaskStorageConfig) -> Arc<dyn TaskExecutor> {
    Arc::new(DefaultTaskExecutor::new(queue, config))
}

struct DefaultTaskExecutor {
    queue: Arc<dyn TaskQueue>,
    storage_timeout: Duration,
}

impl DefaultTaskExecutor {
    fn new(queue: Arc<dyn TaskQueue>, config: TaskStorageConfig) -> Self {
        Self {
            queue,
            storage_timeout: config.storage_timeout(),
        }
    }

    async fn settle_before(
        &self,
        task: &LeasedTask,
        outcome: SettlementRequest,
        settlement_deadline: Instant,
    ) -> Result<SettlementResult, TaskProcessingError> {
        let now = Instant::now();
        if now >= settlement_deadline {
            return Err(TaskProcessingError::LeaseBudgetExhausted);
        }
        let deadline = (now + self.storage_timeout).min(settlement_deadline);
        tokio::select! {
            // Check the deadline before polling storage, including after a pause.
            biased;
            _ = sleep_until(deadline) => Err(TaskProcessingError::TimedOut),
            result = self.queue.settle(task, outcome) => result,
        }
    }
}

#[async_trait]
impl TaskExecutor for DefaultTaskExecutor {
    fn storage_timeout(&self) -> Duration {
        self.storage_timeout
    }

    async fn execute_and_settle(
        &self,
        dispatcher: &mut TaskDispatcher,
        task: LeasedTask,
        handler_timeout: Duration,
    ) -> Result<SettlementResult, TaskProcessingError> {
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
                                delay: redelivery_backoff(task.attempt().number()),
                            },
                            settlement_deadline,
                        )
                        .await;
                }
                ExecutionPlan::RecoverOnly => return Err(TaskProcessingError::LeaseBudgetExhausted),
            };
        let result = tokio::select! {
            biased;
            _ = sleep_until(handler_deadline) => Err(TaskHandlerError::Retry {
                reason: "handler timed out".to_owned(),
                retry_after: None,
            }),
            result = dispatcher.dispatch(task.attempt()) => result,
        };
        let fallback_delay = match &result {
            Err(TaskHandlerError::Retry { retry_after: None, .. }) => redelivery_backoff(task.attempt().number()),
            _ => Duration::ZERO,
        };
        let outcome = settlement_request(result, fallback_delay);
        // Retain a margin before local expiry; queue-side token and database
        // clock checks remain authoritative, including after clock changes.
        self.settle_before(&task, outcome, settlement_deadline).await
    }
}

fn settlement_request(result: Result<(), TaskHandlerError>, fallback_delay: Duration) -> SettlementRequest {
    match result {
        Ok(()) => SettlementRequest::Completed,
        Err(TaskHandlerError::Permanent { reason }) => SettlementRequest::Failed { reason },
        Err(TaskHandlerError::Retry { reason, retry_after }) => SettlementRequest::Retry {
            reason,
            delay: retry_after.unwrap_or(fallback_delay),
        },
    }
}
#[cfg(test)]
mod tests {
    use crate::task::{
        data::{attempt::TaskAttempt, format::TaskFormat},
        handling::adapter::TaskAdapter,
        queue::lease::LEASE_SAFETY_MARGIN,
    };

    use super::*;
    use crate::task::test_support::*;
    use std::{
        sync::{Arc, atomic::Ordering},
        time::Duration,
    };

    struct ProbeAdapter {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        simulate_pause: bool,
    }

    #[async_trait]
    impl TaskAdapter for ProbeAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }

        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
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
        let service = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_secs(5)).unwrap());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handler = ProbeAdapter {
            calls: calls.clone(),
            simulate_pause: false,
        };
        let mut handler = dispatcher(handler);
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
            Err(TaskProcessingError::LeaseBudgetExhausted)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
        assert_eq!(handler.formats(), vec![format()]);
    }

    #[tokio::test(start_paused = true)]
    async fn lease_margin_shortens_settlement_deadline_without_starting_handler() {
        let queue = Arc::new(Queue::default());
        queue.hang_settle.store(true, Ordering::SeqCst);
        let service = task_executor(queue.clone());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handler = ProbeAdapter {
            calls: calls.clone(),
            simulate_pause: false,
        };
        let mut handler = dispatcher(handler);
        let mut task = claimed();
        task = with_deadline(task, Instant::now() + LEASE_SAFETY_MARGIN + Duration::from_secs(1));
        let started = Instant::now();
        assert_eq!(
            service.execute_and_settle(&mut handler, task, Duration::from_secs(30)).await,
            Err(TaskProcessingError::TimedOut)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(queue.outcomes.lock().unwrap()[0].1, SettlementRequest::Retry { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn process_pause_exhausting_lease_prevents_a_late_acknowledgement() {
        let queue = Arc::new(Queue::default());
        let service = task_executor(queue.clone());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handler = ProbeAdapter {
            calls: calls.clone(),
            simulate_pause: true,
        };
        let mut handler = dispatcher(handler);
        assert_eq!(
            service
                .execute_and_settle(&mut handler, claimed(), Duration::from_secs(30))
                .await,
            Err(TaskProcessingError::LeaseBudgetExhausted)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unrepresentable_handler_budget_is_rejected_before_execution() {
        let queue = Arc::new(Queue::default());
        let service = task_executor(queue.clone());
        assert_eq!(
            service
                .execute_and_settle(&mut dispatcher(adapter()), claimed(), Duration::MAX)
                .await,
            Err(TaskProcessingError::InvalidExecutionBudget)
        );
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn storage_deadline_bounds_settlement_without_a_terminal_outcome() {
        let queue = Arc::new(Queue::default());
        let service = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_secs(3)).unwrap());
        queue.hang_settle.store(true, Ordering::SeqCst);
        let started = Instant::now();
        assert_eq!(
            service
                .execute_and_settle(&mut dispatcher(adapter()), claimed(), Duration::from_secs(30))
                .await,
            Err(TaskProcessingError::TimedOut)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_classifies_success_errors_and_timeout_and_preserves_ownership_result() {
        let queue = Arc::new(Queue::default());
        let service = task_executor(queue.clone());
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
            let mut handler = adapter();
            handler.result = result;
            handler.hang = hang;
            let mut handler = dispatcher(handler);
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
        let mut handler = dispatcher(adapter());
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
            Err(TaskProcessingError::Unavailable)
        );
    }
}
