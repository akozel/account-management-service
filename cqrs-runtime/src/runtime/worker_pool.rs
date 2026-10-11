//! Ownership, observation and bounded drain of permanent Tokio workers.
use super::worker::run_task_worker;
use crate::task::{executor::port::TaskExecutor, handling::dispatcher::TaskDispatcher, queue::lease::LeasedTask};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::Notify,
    task::{Id, JoinSet},
};
use tokio_util::sync::CancellationToken;

pub(super) struct WorkerExit {
    pub(super) index: usize,
    pub(super) tokio_task_id: Id,
    pub(super) panicked: bool,
    pub(super) cancelled: bool,
}
pub(super) struct WorkerPool {
    workers: JoinSet<usize>,
    worker_indices: HashMap<Id, usize>,
    executor: Arc<dyn TaskExecutor>,
    receiver: async_channel::Receiver<LeasedTask>,
    capacity_available: Arc<Notify>,
    handler_timeout: Duration,
    stop: CancellationToken,
}
impl WorkerPool {
    pub(super) fn new(
        executor: Arc<dyn TaskExecutor>,
        receiver: async_channel::Receiver<LeasedTask>,
        capacity_available: Arc<Notify>,
        handler_timeout: Duration,
        stop: CancellationToken,
    ) -> Self {
        Self {
            workers: JoinSet::new(),
            worker_indices: HashMap::new(),
            executor,
            receiver,
            capacity_available,
            handler_timeout,
            stop,
        }
    }

    pub(super) async fn next_exit(&mut self) -> Option<WorkerExit> {
        let exit = self.workers.join_next_with_id().await?;
        let (tokio_task_id, panicked, cancelled) = match &exit {
            Ok((id, _)) => (*id, false, false),
            Err(error) => (error.id(), error.is_panic(), error.is_cancelled()),
        };
        let index = self
            .worker_indices
            .remove(&tokio_task_id)
            .expect("every worker has a registered index");
        Some(WorkerExit {
            index,
            tokio_task_id,
            panicked,
            cancelled,
        })
    }

    pub(super) fn spawn(&mut self, index: usize, dispatcher: TaskDispatcher) {
        let handle = self.workers.spawn(run_task_worker(
            index,
            dispatcher,
            self.executor.clone(),
            self.receiver.clone(),
            self.capacity_available.clone(),
            self.handler_timeout,
            self.stop.clone(),
        ));
        self.worker_indices.insert(handle.id(), index);
    }

    pub(super) async fn drain(&mut self) {
        self.stop.cancel();
        // Closing a channel still permits receiving buffered tasks. Stop must win
        // the worker's biased select before closure wakes pending receives.
        self.receiver.close();
        let _ = tokio::time::timeout(self.handler_timeout + self.executor.storage_timeout(), async {
            while self.workers.join_next().await.is_some() {}
        })
        .await;
        // Abort remaining workers and observe cancellation before returning.
        self.workers.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::task::{
        queue::{error::TaskProcessingError, settlement::SettlementResult},
        test_support::{adapter, claimed},
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct ActiveCall<'a>(&'a AtomicUsize);
    impl Drop for ActiveCall<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    async fn wait_until(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn drain_aborts_and_joins_an_executor_that_ignores_execution_budgets() {
        // An executor implementation can violate cooperative budget enforcement;
        // the supervisor still owns and bounds the lifetime of its worker tasks.
        struct StalledExecutor(Arc<AtomicUsize>);
        #[async_trait]
        impl TaskExecutor for StalledExecutor {
            fn storage_timeout(&self) -> Duration {
                Duration::from_millis(10)
            }
            async fn execute_and_settle(
                &self,
                _: &mut TaskDispatcher,
                _: LeasedTask,
                _: Duration,
            ) -> Result<SettlementResult, TaskProcessingError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveCall(&self.0);
                std::future::pending().await
            }
        }
        let probe = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = async_channel::bounded(1);
        let stop = CancellationToken::new();
        let mut pool = WorkerPool {
            workers: JoinSet::new(),
            worker_indices: HashMap::new(),
            executor: Arc::new(StalledExecutor(probe.clone())),
            receiver,
            capacity_available: Arc::new(Notify::new()),
            handler_timeout: Duration::from_millis(20),
            stop,
        };
        pool.spawn(0, TaskDispatcher::try_new(vec![Box::new(adapter())]).unwrap());
        sender.send(claimed()).await.unwrap();
        wait_until(|| probe.load(Ordering::SeqCst) == 1).await;
        let started = tokio::time::Instant::now();
        pool.drain().await;
        assert_eq!(started.elapsed(), Duration::from_millis(30));
        assert!(pool.workers.is_empty());
        assert_eq!(probe.load(Ordering::SeqCst), 0, "abort must be observed before drain returns");
    }
}
