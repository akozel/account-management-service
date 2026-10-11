//! One producer's bounded, rotating acquisition and expired-lease recovery.
use crate::task::{
    data::format::TaskFormat,
    queue::{error::TaskProcessingError, lease::LeasedTask, port::TaskQueue, settlement::LeaseRecoveryReport},
};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, timeout, timeout_at};

pub(super) struct Poller {
    queue: Arc<dyn TaskQueue>,
    formats: Vec<TaskFormat>,
    next_format_start: usize,
    storage_timeout: Duration,
}

impl Poller {
    pub(super) fn new(queue: Arc<dyn TaskQueue>, formats: Vec<TaskFormat>, storage_timeout: Duration) -> Self {
        Self {
            queue,
            formats,
            next_format_start: 0,
            storage_timeout,
        }
    }

    pub(super) async fn claim(
        &mut self,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        let deadline = Instant::now() + self.storage_timeout;
        let mut claimed = Vec::new();
        let count = self.formats.len();
        let start = self.next_format_start;
        self.next_format_start = start.wrapping_add(1);
        for offset in 0..count {
            if claimed.len() == limit {
                break;
            }
            let format = &self.formats[start.wrapping_add(offset) % count];
            let result = timeout_at(deadline, self.queue.claim(format, owner, limit - claimed.len(), lease))
                .await
                .unwrap_or(Err(TaskProcessingError::TimedOut));
            match result {
                Ok(tasks) => claimed.extend(tasks),
                Err(_) if !claimed.is_empty() => break,
                Err(error) => return Err(error),
            }
        }
        Ok(claimed)
    }

    pub(super) async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        timeout(self.storage_timeout, self.queue.recover_expired(limit))
            .await
            .unwrap_or(Err(TaskProcessingError::TimedOut))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::test_support::*;
    use std::sync::atomic::Ordering;

    #[tokio::test(start_paused = true)]
    async fn storage_deadlines_bound_claim_and_recovery() {
        let queue = Arc::new(Queue::default());
        let mut poller = Poller::new(queue.clone(), vec![format()], Duration::from_secs(3));
        queue.hang_claim.store(true, Ordering::SeqCst);
        let started = Instant::now();
        assert_eq!(
            poller.claim("test", 1, Duration::from_secs(60)).await.unwrap_err(),
            TaskProcessingError::TimedOut
        );
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        queue.hang_recovery.store(true, Ordering::SeqCst);
        let started = Instant::now();
        assert_eq!(poller.recover_expired(1).await.unwrap_err(), TaskProcessingError::TimedOut);
        assert_eq!(started.elapsed(), Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn one_claim_budget_spans_formats_and_preserves_already_confirmed_tasks() {
        let queue = Arc::new(Queue::default());
        *queue.claim_delay.lock().unwrap() = Duration::from_secs(2);
        let first = claimed();
        let first_id = first.attempt().id();
        let mut second = claimed();
        second = with_format(second, TaskFormat::new("second", 1).unwrap());
        let second_format = second.attempt().format().clone();
        queue.incoming.lock().unwrap().extend([first, second]);
        let mut service = Poller::new(queue.clone(), vec![format(), second_format], Duration::from_secs(3));
        let started = Instant::now();
        let tasks = service.claim("test", 2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].attempt().id(), first_id);
        assert_eq!(tasks[0].lease().deadline(), started + Duration::from_secs(60));
        assert_eq!(queue.incoming.lock().unwrap().len(), 1);
        assert_eq!(queue.requested.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn claiming_rotates_formats_and_respects_capacity() {
        let queue = Arc::new(Queue::default());
        let future = TaskFormat::new("future", 2).unwrap();
        let mut second = claimed();
        second = with_format(second, future.clone());
        queue.incoming.lock().unwrap().extend([claimed(), second]);
        let mut service = Poller::new(queue.clone(), vec![format(), future.clone()], Duration::from_secs(6));
        assert_eq!(service.formats, vec![format(), future.clone()]);
        assert_eq!(service.claim("test", 1, Duration::from_secs(60)).await.unwrap().len(), 1);
        assert_eq!(
            service.claim("test", 1, Duration::from_secs(60)).await.unwrap()[0]
                .attempt()
                .format(),
            &future
        );
        assert_eq!(*queue.requested.lock().unwrap(), vec![format(), future]);
        assert!(service.claim("test", 0, Duration::from_secs(60)).await.unwrap().is_empty());
        assert!(service.claim("test", 3, Duration::from_secs(60)).await.unwrap().is_empty());
        assert!(
            Poller::new(queue.clone(), vec![], Duration::from_secs(6))
                .claim("test", 1, Duration::from_secs(60))
                .await
                .unwrap()
                .is_empty()
        );
        queue.claim_error.store(true, Ordering::SeqCst);
        assert_eq!(
            service.claim("test", 1, Duration::from_secs(60)).await.unwrap_err(),
            TaskProcessingError::Unavailable
        );
    }

    #[tokio::test]
    async fn a_later_format_failure_does_not_discard_already_claimed_work() {
        let queue = Arc::new(Queue::default());
        let unsupported = TaskFormat::new("temporarily_unavailable", 1).unwrap();
        *queue.failing_format.lock().unwrap() = Some(unsupported.clone());
        let task = claimed();
        let id = task.attempt().id();
        queue.incoming.lock().unwrap().push_back(task);
        let mut service = Poller::new(queue, vec![format(), unsupported], Duration::from_secs(6));
        let result = service.claim("test", 2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].attempt().id(), id);
    }
}
