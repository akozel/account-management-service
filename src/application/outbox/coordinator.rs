use super::{
    error::OutboxError,
    queue::{ClaimedTask, OutboxQueue, RecoveryResult, SettleResult, TaskOutcome},
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

#[async_trait]
pub trait OutboxTaskCoordinator: Send + Sync {
    fn formats(&self) -> Vec<TaskFormat>;
    async fn claim(&self, owner: &str, limit: usize, lease: Duration) -> Result<Vec<ClaimedTask>, OutboxError>;
    async fn recover_expired(&self, limit: usize) -> Result<RecoveryResult, OutboxError>;
    async fn execute_and_settle(
        &self,
        handler: &mut dyn TaskHandler,
        task: ClaimedTask,
        timeout: Duration,
    ) -> Result<SettleResult, OutboxError>;
}

/// A bounded, jittered exponential delay. Also used for expired leases.
pub fn retry_delay(attempt: i32) -> Duration {
    let exponent = attempt.saturating_sub(1).clamp(0, 6) as u32;
    let base = (5_u64 * 2_u64.pow(exponent)).min(300);
    let jitter = rand::random_range(0.8..=1.2);
    Duration::from_secs_f64((base as f64 * jitter).min(300.0))
}

pub fn outbox_task_coordinator(queue: Arc<dyn OutboxQueue>, formats: Vec<TaskFormat>) -> Arc<dyn OutboxTaskCoordinator> {
    Arc::new(DefaultOutboxTaskCoordinator::new(queue, formats))
}

pub(super) struct DefaultOutboxTaskCoordinator {
    queue: Arc<dyn OutboxQueue>,
    formats: Vec<TaskFormat>,
    next_format: AtomicUsize,
}

impl DefaultOutboxTaskCoordinator {
    pub(super) fn new(queue: Arc<dyn OutboxQueue>, formats: Vec<TaskFormat>) -> Self {
        Self {
            queue,
            formats,
            next_format: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl OutboxTaskCoordinator for DefaultOutboxTaskCoordinator {
    fn formats(&self) -> Vec<TaskFormat> {
        self.formats.clone()
    }

    async fn claim(&self, owner: &str, limit: usize, lease: Duration) -> Result<Vec<ClaimedTask>, OutboxError> {
        let mut claimed = Vec::new();
        let count = self.formats.len();
        let start = self.next_format.fetch_add(1, Ordering::Relaxed);
        for offset in 0..count {
            if claimed.len() == limit {
                break;
            }
            let format = &self.formats[(start.wrapping_add(offset)) % count];
            match self.queue.claim(format, owner, limit - claimed.len(), lease).await {
                Ok(tasks) => claimed.extend(tasks),
                Err(_) if !claimed.is_empty() => break,
                Err(error) => return Err(error),
            }
        }
        Ok(claimed)
    }

    async fn recover_expired(&self, limit: usize) -> Result<RecoveryResult, OutboxError> {
        self.queue.recover_expired(limit).await
    }

    async fn execute_and_settle(
        &self,
        handler: &mut dyn TaskHandler,
        task: ClaimedTask,
        timeout: Duration,
    ) -> Result<SettleResult, OutboxError> {
        let outcome = match tokio::time::timeout(timeout, handler.handle(&task)).await {
            Ok(Ok(())) => TaskOutcome::Completed,
            Ok(Err(TaskExecutionError::Permanent { reason })) => TaskOutcome::Failed { reason },
            Ok(Err(TaskExecutionError::Retry { reason, retry_after })) => TaskOutcome::Retry {
                reason,
                delay: retry_after.unwrap_or_else(|| retry_delay(task.attempt)),
            },
            Err(_) => TaskOutcome::Retry {
                reason: "handler timed out".to_owned(),
                delay: retry_delay(task.attempt),
            },
        };
        self.queue.settle(&task, outcome).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::email_reservation::tasks::send_verification_code::send_verification_code_task_handler;
    use crate::application::outbox::test_support::*;
    use std::{
        sync::{Arc, atomic::Ordering},
        time::Duration,
    };

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
            let mut sender = sender();
            sender.result = result;
            sender.hang = hang;
            let mut handler = send_verification_code_task_handler(Box::new(sender));
            assert!(
                service
                    .execute_and_settle(handler.as_mut(), claimed(), Duration::from_secs(30))
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
        let mut handler = send_verification_code_task_handler(Box::new(sender()));
        queue.lost_owner.store(true, Ordering::SeqCst);
        assert!(
            !service
                .execute_and_settle(handler.as_mut(), claimed(), Duration::from_secs(30))
                .await
                .unwrap()
                .is_settled()
        );
        queue.settle_error.store(true, Ordering::SeqCst);
        assert_eq!(
            service
                .execute_and_settle(handler.as_mut(), claimed(), Duration::from_secs(30))
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
