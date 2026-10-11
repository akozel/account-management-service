//! Bounded admission and coordination of polling, worker exits and shutdown.
use crate::runtime::worker_pool_config::TaskWorkerPoolConfigError;
use crate::task::{executor::port::TaskExecutor, handling::dispatcher::TaskDispatcher, queue::port::TaskQueue};
use std::sync::Arc;
use thiserror::Error;
use tokio::{sync::Notify, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;
use {
    super::{dispatcher_factory::build_checked_dispatcher, poller::Poller, worker::log_failed, worker_pool::WorkerPool},
    crate::runtime::{
        dispatcher_factory::{TaskDispatcherFactory, WorkerSetupError},
        worker_pool_config::TaskWorkerPoolConfig,
    },
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskRuntimeError {
    #[error(transparent)]
    Configuration(#[from] TaskWorkerPoolConfigError),
    #[error("{0}")]
    WorkerSetup(#[from] WorkerSetupError),
}

// PostgreSQL recovery currently performs one UPDATE per row; do not scale this with C.
const RECOVERY_BATCH_LIMIT: usize = 32;
/// One producer claims under free bounded-channel capacity; at most C deliveries run and Q wait.
/// Each worker owns its dispatcher for its entire Tokio task lifetime. Token cancellation
/// stops admission/dequeue and drains only started invocations for a common H+S budget.
/// Startup/restart errors cancel the supplied token before returning/draining. Pass a
/// child token to isolate a pool; clones share cancellation with the host. Dropping a
/// token handle does not request shutdown: lifecycle owners should use a drop guard.
/// Dropping/aborting this supervisor cancels the token and aborts its owned workers;
/// asynchronous graceful draining requires cancelling the token and awaiting `run`.
/// Original lease deadlines and database fencing remain authoritative.
pub async fn run(
    queue: Arc<dyn TaskQueue>,
    executor: Arc<dyn TaskExecutor>,
    factory: TaskDispatcherFactory,
    config: TaskWorkerPoolConfig,
    shutdown: CancellationToken,
) -> Result<(), TaskRuntimeError> {
    let _cancel_on_exit = shutdown.clone().drop_guard();
    config.validate_for_storage_timeout(executor.storage_timeout())?;
    if shutdown.is_cancelled() {
        return Ok(());
    }
    let first = factory().map_err(|source| WorkerSetupError::Build { worker_index: 0, source })?;
    let formats = first.formats().to_vec();
    let dispatchers = std::iter::once(Ok(first))
        .chain((1..config.concurrency()).map(|index| build_checked_dispatcher(&factory, &formats, index)))
        .collect::<Result<Vec<_>, _>>()?;
    run_prepared(queue, executor, factory, config, dispatchers, shutdown).await
}

/// Shared execution engine for ready dispatchers and the low-level entry point.
pub(super) async fn run_prepared(
    queue: Arc<dyn TaskQueue>,
    executor: Arc<dyn TaskExecutor>,
    factory: TaskDispatcherFactory,
    config: TaskWorkerPoolConfig,
    dispatchers: Vec<TaskDispatcher>,
    shutdown: CancellationToken,
) -> Result<(), TaskRuntimeError> {
    let _cancel_on_exit = shutdown.clone().drop_guard();
    if shutdown.is_cancelled() {
        return Ok(());
    }
    let formats = dispatchers[0].formats().to_vec();
    let mut poller = Poller::new(queue, formats.clone(), executor.storage_timeout());
    let owner = Uuid::new_v4().to_string();
    let (sender, receiver) = async_channel::bounded(config.queue_capacity());
    let capacity_available = Arc::new(Notify::new());
    let mut pool = WorkerPool::new(
        executor.clone(),
        receiver,
        capacity_available.clone(),
        config.handler_timeout(),
        shutdown.clone(),
    );
    for (index, dispatcher) in dispatchers.into_iter().enumerate() {
        pool.spawn(index, dispatcher);
    }
    info!(concurrency = config.concurrency(), queue_capacity = config.queue_capacity(),
        lease_seconds = config.lease_duration().as_secs(), owner = %owner, "task runtime started");
    let mut interval = tokio::time::interval(config.poll_interval());
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // All fallible runtime exits converge here rather than aborting healthy workers early.
    let result = async {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                Some(exit) = pool.next_exit() => {
                    let index = exit.index;
                    error!(worker = index, tokio_task_id = %exit.tokio_task_id,
                        panicked = exit.panicked, cancelled = exit.cancelled,
                        owner = %owner, "task worker stopped; restarting");
                    let dispatcher = build_checked_dispatcher(&factory, &formats, index)?;
                    if shutdown.is_cancelled() {
                        break;
                    }
                    pool.spawn(index, dispatcher);
                }
                _ = interval.tick() => {
                    let recovery = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        result = poller.recover_expired(config.concurrency().min(RECOVERY_BATCH_LIMIT)) => result,
                    };
                    match recovery {
                        Ok(report) => for failure in report.failures() { log_failed(failure); },
                        Err(error) => error!(error = %error, "task lease recovery failed"),
                    }
                }
                _ = capacity_available.notified() => {},
            }
            let limit = config.queue_capacity() - sender.len();
            if limit == 0 {
                continue;
            }
            // This is the only producer. While claim awaits, consumers can only
            // reduce occupancy, so the returned batch fits without reservations.
            let claimed = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                result = poller.claim(&owner, limit, config.lease_duration()) => result,
            };
            match claimed {
                Ok(tasks) => {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    assert!(tasks.len() <= limit, "queue must honor the claim limit");
                    for task in tasks {
                        // TrySendError contains LeasedTask; never format it into
                        // a panic or log because payload and lease token are private.
                        assert!(
                            sender.try_send(task).is_ok(),
                            "single producer's claimed batch must fit an open queue"
                        );
                    }
                }
                Err(error) => error!(error = %error, "task claim failed"),
            }
        }
        Ok(())
    }
    .await;
    // Notify the host before waiting for healthy workers, retaining the original error.
    shutdown.cancel();
    pool.drain().await;
    drop(sender);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::task::{executor::default_executor::task_executor, test_support::*};
    use std::{sync::atomic::Ordering, time::Duration};
    async fn wait_until(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }
    fn worker_config(c: usize, q: usize, poll: Duration) -> TaskWorkerPoolConfig {
        TaskWorkerPoolConfig::new(c, q, poll, Duration::from_secs(30), Duration::from_secs(120)).unwrap()
    }

    #[tokio::test]
    async fn startup_diagnostics_report_effective_worker_limits_and_owner() {
        let queue = Arc::new(Queue::default());
        let stop = CancellationToken::new();
        let config = worker_config(2, 3, Duration::from_secs(3600));
        let (_, log) = crate::diagnostics::test_support::capture_at(tracing::Level::INFO, async {
            let stopping = async {
                wait_until(|| queue.recovery_calls.load(Ordering::SeqCst) > 0).await;
                stop.cancel();
            };
            let factory: TaskDispatcherFactory = Arc::new(|| TaskDispatcher::try_new(vec![Box::new(adapter())]));
            let (result, ()) = tokio::join!(
                super::run(
                    queue.clone(),
                    task_executor(queue.clone()),
                    factory,
                    config.clone(),
                    stop.clone()
                ),
                stopping
            );
            result.unwrap();
        })
        .await;
        assert_eq!(log.matches("task runtime started").count(), 1);
        assert!(log.contains("concurrency=2 queue_capacity=3"));
        assert!(log.contains(&format!("lease_seconds={}", config.lease_duration().as_secs())));
        let owner = log.split("owner=").nth(1).unwrap().split_whitespace().next().unwrap();
        assert!(Uuid::parse_str(owner).is_ok());
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }
}
