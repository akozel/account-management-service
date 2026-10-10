//! Bounded delivery admission and JoinSet supervision of permanent registry-owning workers.
use super::{
    OutboxWorkerPoolConfig, OutboxWorkerPoolConfigError,
    worker_task::{log_failed, run_worker},
};
use crate::outbox::{ClaimedTask, OutboxTaskCoordinator, TaskFormat, TaskHandlerRegistry, TaskHandlerRegistryError};
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::Notify,
    task::{Id, JoinSet},
    time::MissedTickBehavior,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

// PostgreSQL recovery currently performs one UPDATE per row; do not scale this with C.
const RECOVERY_BATCH_LIMIT: usize = 32;

/// Fresh handler registry per worker/restart; every instance must expose the
/// same ordered format list as the coordinator. Sender state belongs to a worker.
pub type HandlerRegistryFactory = Arc<dyn Fn() -> Result<TaskHandlerRegistry, TaskHandlerRegistryError> + Send + Sync>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkerPoolError {
    #[error(transparent)]
    Configuration(#[from] OutboxWorkerPoolConfigError),
    #[error("worker {worker_index} registry construction failed: {source}")]
    RegistryBuild {
        worker_index: usize,
        #[source]
        source: TaskHandlerRegistryError,
    },
    #[error("worker {worker_index} registry and coordinator formats differ")]
    HandlerFormatsMismatch { worker_index: usize },
}

fn checked_handler(
    factory: &HandlerRegistryFactory,
    formats: &[TaskFormat],
    worker_index: usize,
) -> Result<TaskHandlerRegistry, WorkerPoolError> {
    let handler = factory().map_err(|source| WorkerPoolError::RegistryBuild { worker_index, source })?;
    if handler.formats() == formats {
        Ok(handler)
    } else {
        Err(WorkerPoolError::HandlerFormatsMismatch { worker_index })
    }
}

struct WorkerPool {
    workers: JoinSet<usize>,
    indices: HashMap<Id, usize>,
    coordinator: Arc<dyn OutboxTaskCoordinator>,
    receiver: async_channel::Receiver<ClaimedTask>,
    available: Arc<Notify>,
    handler_timeout: Duration,
    stop: CancellationToken,
}
impl WorkerPool {
    fn spawn(&mut self, index: usize, handler: TaskHandlerRegistry) {
        let handle = self.workers.spawn(run_worker(
            index,
            handler,
            self.coordinator.clone(),
            self.receiver.clone(),
            self.available.clone(),
            self.handler_timeout,
            self.stop.clone(),
        ));
        self.indices.insert(handle.id(), index);
    }

    async fn drain(&mut self) {
        self.stop.cancel();
        // Closing a channel still permits receiving buffered tasks. Stop must win
        // the worker's biased select before closure wakes pending receives.
        self.receiver.close();
        let _ = tokio::time::timeout(self.handler_timeout + self.coordinator.storage_timeout(), async {
            while self.workers.join_next().await.is_some() {}
        })
        .await;
        // Abort remaining workers and observe cancellation before returning.
        self.workers.shutdown().await;
    }
}

/// One producer claims under free bounded-channel capacity; at most C deliveries run and Q wait.
/// Each worker owns its registry for its entire Tokio task lifetime. Token cancellation
/// stops admission/dequeue and drains only started invocations for a common H+S budget.
/// Startup/restart errors cancel the supplied token before returning/draining. Pass a
/// child token to isolate a pool; clones share cancellation with the host. Dropping a
/// token handle does not request shutdown: lifecycle owners should use a drop guard.
/// Dropping/aborting this supervisor cancels the token and aborts its owned workers;
/// asynchronous graceful draining requires cancelling the token and awaiting `run`.
/// Original lease deadlines and database fencing remain authoritative.
pub async fn run(
    coordinator: Arc<dyn OutboxTaskCoordinator>,
    factory: HandlerRegistryFactory,
    config: OutboxWorkerPoolConfig,
    shutdown: CancellationToken,
) -> Result<(), WorkerPoolError> {
    let _cancel_on_exit = shutdown.clone().drop_guard();
    config.validate_for_storage_timeout(coordinator.storage_timeout())?;
    if shutdown.is_cancelled() {
        return Ok(());
    }
    let formats = coordinator.formats();
    let handlers = (0..config.concurrency())
        .map(|index| checked_handler(&factory, &formats, index))
        .collect::<Result<Vec<_>, _>>()?;
    let owner = Uuid::new_v4().to_string();
    let (sender, receiver) = async_channel::bounded(config.queue_capacity());
    let available = Arc::new(Notify::new());
    let mut pool = WorkerPool {
        workers: JoinSet::new(),
        indices: HashMap::new(),
        coordinator: coordinator.clone(),
        receiver,
        available: available.clone(),
        handler_timeout: config.handler_timeout(),
        stop: shutdown.clone(),
    };
    for (index, handler) in handlers.into_iter().enumerate() {
        pool.spawn(index, handler);
    }
    info!(concurrency = config.concurrency(), queue_capacity = config.queue_capacity(),
        lease_seconds = config.lease_duration().as_secs(), owner = %owner, "outbox worker pool started");
    let mut interval = tokio::time::interval(config.poll_interval());
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // All fallible runtime exits converge here rather than aborting healthy workers early.
    let result = async {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                Some(exit) = pool.workers.join_next_with_id() => {
                    let (id, panicked, cancelled) = match &exit {
                        Ok((id, _)) => (*id, false, false),
                        Err(error) => (error.id(), error.is_panic(), error.is_cancelled()),
                    };
                    let index = pool.indices.remove(&id).expect("every worker has a registered index");
                    error!(worker = index, tokio_task_id = %id, panicked, cancelled,
                        owner = %owner, "outbox worker stopped; restarting");
                    let handler = checked_handler(&factory, &formats, index)?;
                    if shutdown.is_cancelled() {
                        break;
                    }
                    pool.spawn(index, handler);
                }
                _ = interval.tick() => {
                    let recovery = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        result = coordinator.recover_expired(config.concurrency().min(RECOVERY_BATCH_LIMIT)) => result,
                    };
                    match recovery {
                        Ok(report) => for failure in report.failures() { log_failed(failure); },
                        Err(error) => error!(error = %error, "outbox lease recovery failed"),
                    }
                }
                _ = available.notified() => {},
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
                result = coordinator.claim(&owner, limit, config.lease_duration()) => result,
            };
            match claimed {
                Ok(tasks) => {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    assert!(tasks.len() <= limit, "queue must honor the claim limit");
                    for task in tasks {
                        // TrySendError contains ClaimedTask; never format it into
                        // a panic or log because payload and lease token are private.
                        assert!(
                            sender.try_send(task).is_ok(),
                            "single producer's claimed batch must fit an open queue"
                        );
                    }
                }
                Err(error) => error!(error = %error, "outbox claim failed"),
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
    use crate::outbox::{test_support::*, *};
    use async_trait::async_trait;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    #[tokio::test]
    async fn invalid_full_lease_budget_is_rejected_before_polling() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator_with_config(
            queue.clone(),
            vec![format()],
            OutboxTaskCoordinatorConfig::new(Duration::from_secs(10)).unwrap(),
        )
        .unwrap();
        let factory: HandlerRegistryFactory = Arc::new(|| TaskHandlerRegistry::try_new(vec![Box::new(handler())]));
        let shutdown = CancellationToken::new();
        assert!(
            super::run(
                service,
                factory,
                OutboxWorkerPoolConfig::new(
                    OutboxWorkerPoolConfig::default().concurrency(),
                    OutboxWorkerPoolConfig::default().concurrency(),
                    OutboxWorkerPoolConfig::default().poll_interval(),
                    OutboxWorkerPoolConfig::default().handler_timeout(),
                    Duration::from_secs(50)
                )
                .unwrap(),
                shutdown
            )
            .await
            .is_err()
        );
        assert!(queue.requested.lock().unwrap().is_empty());
        assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn supervisor_recovers_workers_and_survives_storage_errors_and_lost_acknowledgements() {
        let queue = Arc::new(Queue::default());
        queue.claim_error.store(true, Ordering::SeqCst);
        queue.recovery_error.store(true, Ordering::SeqCst);
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        let calls = Arc::new(AtomicUsize::new(0));
        struct PanicOnce {
            calls: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl TaskHandler for PanicOnce {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
                assert_ne!(self.calls.fetch_add(1, Ordering::SeqCst), 0, "injected worker failure");
                Ok(())
            }
        }
        let observed = calls.clone();
        let factory: HandlerRegistryFactory =
            Arc::new(move || TaskHandlerRegistry::try_new(vec![Box::new(PanicOnce { calls: observed.clone() })]));
        let config = OutboxWorkerPoolConfig::new(
            1,
            1,
            Duration::from_millis(2),
            OutboxWorkerPoolConfig::default().handler_timeout(),
            OutboxWorkerPoolConfig::default().lease_duration(),
        )
        .unwrap();
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let worker = tokio::spawn(super::run(service, factory, config, shutdown));
        wait_until(|| queue.recovery_calls.load(Ordering::SeqCst) >= 2).await;
        queue.claim_error.store(false, Ordering::SeqCst);
        queue.recovery_error.store(false, Ordering::SeqCst);
        queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
        wait_until(|| calls.load(Ordering::SeqCst) >= 2 && !queue.outcomes.lock().unwrap().is_empty()).await;
        // An acknowledgement failure leaves durable recovery to the queue's lease mechanism.
        queue.settle_error.store(true, Ordering::SeqCst);
        queue.incoming.lock().unwrap().push_back(claimed());
        wait_until(|| queue.outcomes.lock().unwrap().len() >= 2).await;
        queue.settle_error.store(false, Ordering::SeqCst);
        queue.lost_owner.store(true, Ordering::SeqCst);
        queue.incoming.lock().unwrap().push_back(claimed());
        wait_until(|| queue.outcomes.lock().unwrap().len() >= 3).await;
        stop.cancel();
        worker.await.unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn shutdown_drains_in_flight_work_and_invalid_worker_wiring_is_rejected() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        let seen = Arc::new(AtomicUsize::new(0));
        let factory: HandlerRegistryFactory = Arc::new({
            let seen = seen.clone();
            move || TaskHandlerRegistry::try_new(vec![Box::new(CountingHandler(seen.clone()))])
        });
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let invalid =
            OutboxWorkerPoolConfig::new(1, 1, Duration::from_secs(1), Duration::from_secs(30), Duration::from_secs(45)).unwrap();
        assert!(
            super::run(service.clone(), factory.clone(), invalid, shutdown.child_token())
                .await
                .is_err()
        );
        assert!(
            super::run(
                outbox_task_coordinator(queue.clone(), vec![]),
                factory.clone(),
                OutboxWorkerPoolConfig::default(),
                shutdown.child_token()
            )
            .await
            .is_err()
        );
        queue.incoming.lock().unwrap().push_back(claimed());
        let worker = tokio::spawn(super::run(service, factory, OutboxWorkerPoolConfig::default(), shutdown));
        wait_until(|| seen.load(Ordering::SeqCst) > 0).await;
        stop.cancel();
        worker.await.unwrap().unwrap();
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
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

    #[tokio::test]
    async fn storage_timeouts_allow_polling_and_free_worker_capacity_without_shutdown() {
        for stage in ["claim", "recovery", "settle"] {
            let queue = Arc::new(Queue::default());
            queue.hang_claim.store(stage == "claim", Ordering::SeqCst);
            queue.hang_recovery.store(stage == "recovery", Ordering::SeqCst);
            queue.hang_settle.store(stage == "settle", Ordering::SeqCst);
            queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
            let service = outbox_task_coordinator_with_config(
                queue.clone(),
                vec![format()],
                OutboxTaskCoordinatorConfig::new(Duration::from_millis(5)).unwrap(),
            )
            .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let factory: HandlerRegistryFactory = Arc::new({
                let calls = calls.clone();
                move || TaskHandlerRegistry::try_new(vec![Box::new(CountingHandler(calls.clone()))])
            });
            let shutdown = CancellationToken::new();
            let stop = shutdown.clone();
            let worker = tokio::spawn(super::run(
                service,
                factory,
                OutboxWorkerPoolConfig::new(
                    1,
                    1,
                    Duration::from_millis(2),
                    OutboxWorkerPoolConfig::default().handler_timeout(),
                    OutboxWorkerPoolConfig::default().lease_duration(),
                )
                .unwrap(),
                shutdown,
            ));
            match stage {
                "claim" => {
                    wait_until(|| queue.requested.lock().unwrap().len() >= 2).await;
                    assert_eq!(calls.load(Ordering::SeqCst), 0);
                    queue.hang_claim.store(false, Ordering::SeqCst);
                }
                "recovery" => {
                    wait_until(|| queue.recovery_calls.load(Ordering::SeqCst) >= 2).await;
                    queue.hang_recovery.store(false, Ordering::SeqCst);
                }
                _ => {}
            }
            // Both tasks are executed even when the first acknowledgement never returns.
            wait_until(|| calls.load(Ordering::SeqCst) == 2 && queue.outcomes.lock().unwrap().len() == 2).await;
            assert!(!worker.is_finished());
            stop.cancel();
            worker.await.unwrap().unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_stalled_polling_and_bounds_stalled_acknowledgements() {
        for stage in ["claim", "recovery", "settle"] {
            let queue = Arc::new(Queue::default());
            queue.hang_claim.store(stage == "claim", Ordering::SeqCst);
            queue.hang_recovery.store(stage == "recovery", Ordering::SeqCst);
            queue.hang_settle.store(stage == "settle", Ordering::SeqCst);
            queue.incoming.lock().unwrap().push_back(claimed());
            let service = outbox_task_coordinator(queue.clone(), vec![format()]);
            let factory: HandlerRegistryFactory = Arc::new(|| TaskHandlerRegistry::try_new(vec![Box::new(handler())]));
            let shutdown = CancellationToken::new();
            let stop = shutdown.clone();
            let worker = tokio::spawn(super::run(service, factory, OutboxWorkerPoolConfig::default(), shutdown));
            wait_until(|| match stage {
                "claim" => !queue.requested.lock().unwrap().is_empty(),
                "recovery" => queue.recovery_calls.load(Ordering::SeqCst) > 0,
                "settle" => !queue.outcomes.lock().unwrap().is_empty(),
                _ => unreachable!(),
            })
            .await;
            let start = tokio::time::Instant::now();
            stop.cancel();
            tokio::time::timeout(Duration::from_secs(22), worker)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(start.elapsed() <= Duration::from_secs(21));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drain_uses_the_configured_settlement_budget() {
        let queue = Arc::new(Queue::default());
        queue.hang_settle.store(true, Ordering::SeqCst);
        queue.incoming.lock().unwrap().push_back(claimed());
        let service = outbox_task_coordinator_with_config(
            queue.clone(),
            vec![format()],
            OutboxTaskCoordinatorConfig::new(Duration::from_secs(10)).unwrap(),
        )
        .unwrap();
        let factory: HandlerRegistryFactory = Arc::new(|| {
            let mut handler = handler();
            handler.hang = true;
            TaskHandlerRegistry::try_new(vec![Box::new(handler)])
        });
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let started = tokio::time::Instant::now();
        let worker = tokio::spawn(super::run(
            service,
            factory,
            worker_config(128, 128, Duration::from_secs(1)),
            shutdown,
        ));
        wait_until(|| queue.incoming.lock().unwrap().is_empty()).await;
        stop.cancel();
        worker.await.unwrap().unwrap();
        assert!(started.elapsed() >= Duration::from_secs(40));
        assert!(matches!(queue.outcomes.lock().unwrap()[0].1, SettlementRequest::Retry { .. }));
    }

    #[tokio::test]
    async fn validates_every_workers_registry_before_polling() {
        struct OtherHandler;

        #[async_trait]
        impl TaskHandler for OtherHandler {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![TaskFormat::new("other", 1).unwrap()]
            }

            async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
                Ok(())
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let factory: HandlerRegistryFactory = Arc::new({
            let calls = calls.clone();
            move || {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    TaskHandlerRegistry::try_new(vec![Box::new(handler())])
                } else {
                    TaskHandlerRegistry::try_new(vec![Box::new(OtherHandler)])
                }
            }
        });
        let service = outbox_task_coordinator(Arc::new(Queue::default()), vec![format()]);
        let shutdown = CancellationToken::new();
        let result = super::run(
            service,
            factory,
            OutboxWorkerPoolConfig::new(
                2,
                2,
                OutboxWorkerPoolConfig::default().poll_interval(),
                OutboxWorkerPoolConfig::default().handler_timeout(),
                OutboxWorkerPoolConfig::default().lease_duration(),
            )
            .unwrap(),
            shutdown,
        )
        .await;
        assert_eq!(result, Err(WorkerPoolError::HandlerFormatsMismatch { worker_index: 1 }));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn registry_build_failures_are_typed_on_startup_and_restart() {
        struct PanicHandler;
        #[async_trait]
        impl TaskHandler for PanicHandler {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
                panic!("injected worker failure");
            }
        }
        for fail_on_restart in [false, true] {
            let queue = Arc::new(Queue::default());
            queue.incoming.lock().unwrap().push_back(claimed());
            let calls = Arc::new(AtomicUsize::new(0));
            let factory: HandlerRegistryFactory = Arc::new(move || {
                if fail_on_restart && calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    TaskHandlerRegistry::try_new(vec![Box::new(PanicHandler)])
                } else {
                    Err(TaskHandlerRegistryError::Empty)
                }
            });
            let coordinator = outbox_task_coordinator(queue.clone(), vec![format()]);
            let shutdown = CancellationToken::new();
            let config = OutboxWorkerPoolConfig::new(
                1,
                1,
                Duration::from_millis(2),
                Duration::from_secs(30),
                Duration::from_secs(120),
            )
            .unwrap();
            let error = tokio::time::timeout(Duration::from_secs(2), super::run(coordinator, factory, config, shutdown))
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(
                error,
                WorkerPoolError::RegistryBuild {
                    worker_index: 0,
                    source: TaskHandlerRegistryError::Empty
                }
            );
            assert_eq!(queue.requested.lock().unwrap().len(), usize::from(fail_on_restart));
            assert!(queue.outcomes.lock().unwrap().is_empty());
        }
    }

    struct IoProbe {
        active: AtomicUsize,
        peak: AtomicUsize,
        built: AtomicUsize,
        calls: std::sync::Mutex<Vec<(usize, Id, usize)>>,
        gate: tokio::sync::Semaphore,
    }
    impl Default for IoProbe {
        fn default() -> Self {
            Self {
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                built: AtomicUsize::new(0),
                calls: std::sync::Mutex::new(Vec::new()),
                gate: tokio::sync::Semaphore::new(0),
            }
        }
    }
    struct ActiveCall<'a>(&'a AtomicUsize);
    impl Drop for ActiveCall<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    struct IoHandler {
        probe: Arc<IoProbe>,
        registry: usize,
        calls: usize,
    }
    #[async_trait]
    impl TaskHandler for IoHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
            let active = self.probe.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _active = ActiveCall(&self.probe.active);
            self.probe.peak.fetch_max(active, Ordering::SeqCst);
            self.calls += 1;
            self.probe
                .calls
                .lock()
                .unwrap()
                .push((self.registry, tokio::task::id(), self.calls));
            self.probe.gate.acquire().await.unwrap().forget();
            Ok(())
        }
    }
    fn io_factory(probe: Arc<IoProbe>) -> HandlerRegistryFactory {
        Arc::new(move || {
            let registry = probe.built.fetch_add(1, Ordering::SeqCst);
            TaskHandlerRegistry::try_new(vec![Box::new(IoHandler {
                probe: probe.clone(),
                registry,
                calls: 0,
            })])
        })
    }
    fn worker_config(c: usize, q: usize, poll: Duration) -> OutboxWorkerPoolConfig {
        OutboxWorkerPoolConfig::new(c, q, poll, Duration::from_secs(30), Duration::from_secs(120)).unwrap()
    }

    #[tokio::test]
    async fn bounded_admission_is_independent_of_concurrency_and_shutdown_leaves_queue_for_recovery() {
        for (c, q) in [(2, 3), (2, 1), (3, 1)] {
            let queue = Arc::new(Queue::default());
            queue.incoming.lock().unwrap().extend((0..20).map(|_| claimed()));
            let probe = Arc::new(IoProbe::default());
            let coordinator = outbox_task_coordinator(queue.clone(), vec![format()]);
            let shutdown = CancellationToken::new();
            let stop = shutdown.clone();
            let pool = tokio::spawn(super::run(
                coordinator,
                io_factory(probe.clone()),
                worker_config(c, q, Duration::from_millis(2)),
                shutdown,
            ));
            wait_until(|| probe.active.load(Ordering::SeqCst) == c && queue.incoming.lock().unwrap().len() == 20 - c - q).await;
            assert_eq!(probe.built.load(Ordering::SeqCst), c);
            assert_eq!(probe.peak.load(Ordering::SeqCst), c);
            assert!(queue.claim_limits.lock().unwrap().iter().all(|&limit| limit <= q));
            let calls = queue.claim_limits.lock().unwrap().len();
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(
                queue.claim_limits.lock().unwrap().len(),
                calls,
                "full queue cannot claim more work"
            );
            stop.cancel();
            tokio::time::sleep(Duration::from_millis(2)).await;
            assert!(!pool.is_finished(), "started operations must drain");
            probe.gate.add_permits(c);
            pool.await.unwrap().unwrap();
            assert_eq!(
                probe.calls.lock().unwrap().len(),
                c,
                "queued operations do not start on shutdown"
            );
            assert_eq!(queue.outcomes.lock().unwrap().len(), c);
            assert_eq!(queue.incoming.lock().unwrap().len(), 20 - c - q);
            assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn consumers_free_capacity_during_claim_without_parallel_claims_or_overflow() {
        struct PausedClaimCoordinator {
            inner: Arc<dyn OutboxTaskCoordinator>,
            calls: AtomicUsize,
            gate: tokio::sync::Semaphore,
        }
        #[async_trait]
        impl OutboxTaskCoordinator for PausedClaimCoordinator {
            fn formats(&self) -> Vec<TaskFormat> {
                self.inner.formats()
            }
            fn storage_timeout(&self) -> Duration {
                self.inner.storage_timeout()
            }
            async fn claim(&self, owner: &str, limit: usize, lease: Duration) -> Result<Vec<ClaimedTask>, OutboxError> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 2 {
                    self.gate.acquire().await.unwrap().forget();
                }
                self.inner.claim(owner, limit, lease).await
            }
            async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, OutboxError> {
                self.inner.recover_expired(limit).await
            }
            async fn execute_and_settle(
                &self,
                handler: &mut dyn TaskHandler,
                task: ClaimedTask,
                timeout: Duration,
            ) -> Result<SettlementResult, OutboxError> {
                self.inner.execute_and_settle(handler, task, timeout).await
            }
        }
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend((0..7).map(|_| claimed()));
        let coordinator = Arc::new(PausedClaimCoordinator {
            inner: outbox_task_coordinator(queue.clone(), vec![format()]),
            calls: AtomicUsize::new(0),
            gate: tokio::sync::Semaphore::new(0),
        });
        let probe = Arc::new(IoProbe::default());
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let pool = tokio::spawn(super::run(
            coordinator.clone(),
            io_factory(probe.clone()),
            worker_config(1, 2, Duration::from_secs(3600)),
            shutdown,
        ));
        // One active delivery and two waiting; the next claim starts on dequeue.
        wait_until(|| probe.active.load(Ordering::SeqCst) == 1 && queue.incoming.lock().unwrap().len() == 4).await;
        probe.gate.add_permits(1);
        wait_until(|| coordinator.calls.load(Ordering::SeqCst) == 3).await;
        // Finish the second delivery and dequeue the third while claim is pending.
        probe.gate.add_permits(1);
        wait_until(|| probe.calls.lock().unwrap().len() == 3).await;
        assert_eq!(queue.outcomes.lock().unwrap().len(), 2);
        assert_eq!(queue.incoming.lock().unwrap().len(), 4);
        assert_eq!(
            coordinator.calls.load(Ordering::SeqCst),
            3,
            "dequeue notifications cannot start a parallel claim"
        );
        coordinator.gate.add_permits(1);
        // The captured limit still fits; a subsequent claim refills the remaining slot.
        wait_until(|| queue.incoming.lock().unwrap().len() == 2).await;
        assert_eq!(*queue.claim_limits.lock().unwrap(), vec![2, 1, 1, 1]);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
        assert_eq!(probe.calls.lock().unwrap().len(), 3);
        stop.cancel();
        // Observe internal stop before releasing the active invocation.
        tokio::task::yield_now().await;
        probe.gate.add_permits(1);
        pool.await.unwrap().unwrap();
        assert_eq!(queue.outcomes.lock().unwrap().len(), 3);
        assert_eq!(
            probe.calls.lock().unwrap().len(),
            3,
            "the two buffered deliveries stay for recovery"
        );
    }

    #[tokio::test]
    async fn thousand_tasks_are_each_executed_and_settled_successfully() {
        struct RecordingHandler(Arc<std::sync::Mutex<Vec<Uuid>>>);
        #[async_trait]
        impl TaskHandler for RecordingHandler {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            async fn handle(&mut self, task: &TaskDelivery) -> Result<(), TaskHandlerError> {
                self.0.lock().unwrap().push(task.id());
                tokio::task::yield_now().await;
                Ok(())
            }
        }
        let tasks: Vec<_> = (0..1000).map(|_| claimed()).collect();
        let mut expected: Vec<_> = tasks.iter().map(|task| task.delivery().id()).collect();
        expected.sort_unstable();
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend(tasks);
        let executed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let factory: HandlerRegistryFactory = Arc::new({
            let executed = executed.clone();
            move || TaskHandlerRegistry::try_new(vec![Box::new(RecordingHandler(executed.clone()))])
        });
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let pool = tokio::spawn(super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            factory,
            // More deliveries than C+Q exercises batch admission and Notify refill.
            worker_config(16, 32, Duration::from_secs(3600)),
            shutdown,
        ));
        wait_until(|| queue.outcomes.lock().unwrap().len() >= expected.len()).await;
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), pool)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let mut actual = executed.lock().unwrap().clone();
        actual.sort_unstable();
        assert_eq!(
            actual, expected,
            "each delivery must execute exactly once in this successful run"
        );
        let outcomes = queue.outcomes.lock().unwrap();
        assert!(outcomes.iter().all(|(_, outcome)| *outcome == SettlementRequest::Completed));
        let mut settled: Vec<_> = outcomes.iter().map(|(id, _)| *id).collect();
        settled.sort_unstable();
        assert_eq!(settled, expected, "each delivered ID must have one successful settlement");
        assert!(queue.incoming.lock().unwrap().is_empty());
        assert!(queue.claim_limits.lock().unwrap().iter().all(|&limit| limit <= 32));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn thousand_io_workers_wait_concurrently_on_one_runtime_thread() {
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend((0..2100).map(|_| claimed()));
        let probe = Arc::new(IoProbe::default());
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let pool = tokio::spawn(super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            io_factory(probe.clone()),
            worker_config(1000, 1000, Duration::from_millis(2)),
            shutdown,
        ));
        wait_until(|| probe.active.load(Ordering::SeqCst) == 1000 && queue.incoming.lock().unwrap().len() == 100).await;
        assert_eq!(probe.peak.load(Ordering::SeqCst), 1000);
        let task_ids: std::collections::HashSet<_> = probe.calls.lock().unwrap().iter().map(|call| call.1).collect();
        assert_eq!(task_ids.len(), 1000);
        assert!(queue.recovery_limits.lock().unwrap().iter().all(|&limit| limit == 32));
        stop.cancel();
        tokio::time::sleep(Duration::from_millis(2)).await;
        probe.gate.add_permits(1000);
        pool.await.unwrap().unwrap();
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1000);
    }

    #[tokio::test]
    async fn sequential_deliveries_preserve_registry_state_and_tokio_task_identity() {
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
        let probe = Arc::new(IoProbe::default());
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let pool = tokio::spawn(super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            io_factory(probe.clone()),
            worker_config(1, 1, Duration::from_millis(2)),
            shutdown,
        ));
        wait_until(|| probe.calls.lock().unwrap().len() == 1).await;
        probe.gate.add_permits(1);
        wait_until(|| probe.calls.lock().unwrap().len() == 2).await;
        let calls = probe.calls.lock().unwrap().clone();
        assert_eq!(calls[0].0, calls[1].0);
        assert_eq!(calls[0].1, calls[1].1);
        assert_eq!((calls[0].2, calls[1].2), (1, 2));
        assert_eq!(probe.built.load(Ordering::SeqCst), 1);
        stop.cancel();
        tokio::time::sleep(Duration::from_millis(2)).await;
        probe.gate.add_permits(1);
        pool.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn panic_restarts_without_poll_tick_with_a_new_registry_and_task() {
        struct PanicFirstRegistry {
            registry: usize,
            records: Arc<std::sync::Mutex<Vec<(usize, Id)>>>,
        }
        #[async_trait]
        impl TaskHandler for PanicFirstRegistry {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
                self.records.lock().unwrap().push((self.registry, tokio::task::id()));
                assert_ne!(self.registry, 0, "injected failure");
                Ok(())
            }
        }
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        let built = Arc::new(AtomicUsize::new(0));
        let factory: HandlerRegistryFactory = Arc::new({
            let records = records.clone();
            let built = built.clone();
            move || {
                TaskHandlerRegistry::try_new(vec![Box::new(PanicFirstRegistry {
                    registry: built.fetch_add(1, Ordering::SeqCst),
                    records: records.clone(),
                })])
            }
        });
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let pool = tokio::spawn(super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            factory,
            worker_config(1, 1, Duration::from_secs(3600)),
            shutdown,
        ));
        wait_until(|| queue.outcomes.lock().unwrap().len() == 1).await;
        let records = records.lock().unwrap().clone();
        assert_eq!(built.load(Ordering::SeqCst), 2);
        assert_eq!((records[0].0, records[1].0), (0, 1));
        assert_ne!(records[0].1, records[1].1);
        assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 1);
        assert!(!stop.is_cancelled(), "a worker panic must not cancel the host");
        stop.cancel();
        pool.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn restart_factory_error_drains_healthy_operations_instead_of_aborting_them() {
        for mismatch in [false, true] {
            assert_restart_failure_drains(mismatch).await;
        }
    }

    async fn assert_restart_failure_drains(mismatch: bool) {
        struct OtherHandler;
        #[async_trait]
        impl TaskHandler for OtherHandler {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![TaskFormat::new("other", 1).unwrap()]
            }
            async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
                panic!("mismatched registry must never execute");
            }
        }
        struct PanicOnDemand(Arc<tokio::sync::Semaphore>);
        #[async_trait]
        impl TaskHandler for PanicOnDemand {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
                self.0.acquire().await.unwrap().forget();
                panic!("injected failure");
            }
        }
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend((0..5).map(|_| claimed()));
        let probe = Arc::new(IoProbe::default());
        let crash = Arc::new(tokio::sync::Semaphore::new(0));
        let built = Arc::new(AtomicUsize::new(0));
        let factory: HandlerRegistryFactory = Arc::new({
            let probe = probe.clone();
            let crash = crash.clone();
            let built = built.clone();
            move || match built.fetch_add(1, Ordering::SeqCst) {
                0 => TaskHandlerRegistry::try_new(vec![Box::new(PanicOnDemand(crash.clone()))]),
                1 => TaskHandlerRegistry::try_new(vec![Box::new(IoHandler {
                    probe: probe.clone(),
                    registry: 1,
                    calls: 0,
                })]),
                _ if mismatch => TaskHandlerRegistry::try_new(vec![Box::new(OtherHandler)]),
                _ => Err(TaskHandlerRegistryError::Empty),
            }
        });
        let parent = CancellationToken::new();
        let shutdown = parent.child_token();
        let pool = tokio::spawn(super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            factory,
            worker_config(2, 2, Duration::from_secs(3600)),
            shutdown.clone(),
        ));
        wait_until(|| probe.active.load(Ordering::SeqCst) == 1 && queue.incoming.lock().unwrap().len() == 1).await;
        crash.add_permits(1);
        wait_until(|| built.load(Ordering::SeqCst) == 3).await;
        tokio::time::timeout(Duration::from_secs(1), shutdown.cancelled())
            .await
            .unwrap();
        assert!(!parent.is_cancelled(), "the caller can isolate a pool with a child token");
        assert!(!pool.is_finished(), "host notification must precede healthy worker drain");
        assert_eq!(probe.active.load(Ordering::SeqCst), 1);
        assert!(queue.outcomes.lock().unwrap().is_empty());
        probe.gate.add_permits(1);
        let expected = if mismatch {
            WorkerPoolError::HandlerFormatsMismatch { worker_index: 0 }
        } else {
            WorkerPoolError::RegistryBuild {
                worker_index: 0,
                source: TaskHandlerRegistryError::Empty,
            }
        };
        assert_eq!(pool.await.unwrap(), Err(expected));
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
        assert_eq!(probe.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn initially_cancelled_shutdown_does_not_build_workers_or_claim() {
        let queue = Arc::new(Queue::default());
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            Arc::new(|| panic!("cancelled startup must not build a registry")),
            worker_config(2, 1, Duration::from_secs(1)),
            shutdown,
        )
        .await
        .unwrap();
        assert!(queue.requested.lock().unwrap().is_empty());
        assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn drain_aborts_and_joins_a_coordinator_that_ignores_execution_budgets() {
        // A coordinator implementation can violate cooperative budget enforcement;
        // the supervisor still owns and bounds the lifetime of its worker tasks.
        struct StalledCoordinator(Arc<IoProbe>);
        #[async_trait]
        impl OutboxTaskCoordinator for StalledCoordinator {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            fn storage_timeout(&self) -> Duration {
                Duration::from_millis(10)
            }
            async fn claim(&self, _: &str, _: usize, _: Duration) -> Result<Vec<ClaimedTask>, OutboxError> {
                Ok(vec![])
            }
            async fn recover_expired(&self, _: usize) -> Result<LeaseRecoveryReport, OutboxError> {
                Ok(Default::default())
            }
            async fn execute_and_settle(
                &self,
                _: &mut dyn TaskHandler,
                _: ClaimedTask,
                _: Duration,
            ) -> Result<SettlementResult, OutboxError> {
                self.0.active.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveCall(&self.0.active);
                std::future::pending().await
            }
        }
        let probe = Arc::new(IoProbe::default());
        let (sender, receiver) = async_channel::bounded(1);
        let stop = CancellationToken::new();
        let mut pool = WorkerPool {
            workers: JoinSet::new(),
            indices: HashMap::new(),
            coordinator: Arc::new(StalledCoordinator(probe.clone())),
            receiver,
            available: Arc::new(Notify::new()),
            handler_timeout: Duration::from_millis(20),
            stop,
        };
        pool.spawn(0, TaskHandlerRegistry::try_new(vec![Box::new(handler())]).unwrap());
        sender.send(claimed()).await.unwrap();
        wait_until(|| probe.active.load(Ordering::SeqCst) == 1).await;
        let started = tokio::time::Instant::now();
        pool.drain().await;
        assert_eq!(started.elapsed(), Duration::from_millis(30));
        assert!(pool.workers.is_empty());
        assert_eq!(
            probe.active.load(Ordering::SeqCst),
            0,
            "abort must be observed before drain returns"
        );
    }

    #[tokio::test]
    async fn cancellation_of_supervisor_aborts_owned_workers_without_detaching_them() {
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
        let probe = Arc::new(IoProbe::default());
        let shutdown = CancellationToken::new();
        let pool = tokio::spawn(super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            io_factory(probe.clone()),
            worker_config(1, 1, Duration::from_secs(3600)),
            shutdown.clone(),
        ));
        wait_until(|| probe.active.load(Ordering::SeqCst) == 1 && queue.incoming.lock().unwrap().is_empty()).await;
        pool.abort();
        assert!(pool.await.unwrap_err().is_cancelled());
        assert!(shutdown.is_cancelled(), "supervisor abort must notify the host");
        wait_until(|| probe.active.load(Ordering::SeqCst) == 0).await;
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn shared_cancellation_stops_dequeue_while_supervisor_is_claiming() {
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend((0..5).map(|_| claimed()));
        let probe = Arc::new(IoProbe::default());
        let shutdown = CancellationToken::new();
        let pool = tokio::spawn(super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            io_factory(probe.clone()),
            worker_config(1, 2, Duration::from_secs(3600)),
            shutdown.clone(),
        ));
        wait_until(|| probe.active.load(Ordering::SeqCst) == 1 && queue.incoming.lock().unwrap().len() == 2).await;
        queue.hang_claim.store(true, Ordering::SeqCst);
        probe.gate.add_permits(1);
        wait_until(|| probe.calls.lock().unwrap().len() == 2 && queue.requested.lock().unwrap().len() == 3).await;
        shutdown.cancel();
        // Finish the active operation immediately, even before supervisor resumes.
        probe.gate.add_permits(1);
        pool.await.unwrap().unwrap();
        assert_eq!(probe.calls.lock().unwrap().len(), 2);
        assert_eq!(queue.outcomes.lock().unwrap().len(), 2);
        assert_eq!(queue.incoming.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn cancellation_in_a_ready_claim_discards_the_batch_without_execution() {
        struct CancelAfterClaim {
            inner: Arc<Queue>,
            shutdown: CancellationToken,
        }
        #[async_trait]
        impl OutboxQueue for CancelAfterClaim {
            async fn claim(
                &self,
                format: &TaskFormat,
                owner: &str,
                limit: usize,
                lease: Duration,
            ) -> Result<Vec<ClaimedTask>, OutboxError> {
                let tasks = self.inner.claim(format, owner, limit, lease).await?;
                self.shutdown.cancel();
                Ok(tasks)
            }
            async fn settle(&self, task: &ClaimedTask, request: SettlementRequest) -> Result<SettlementResult, OutboxError> {
                self.inner.settle(task, request).await
            }
            async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, OutboxError> {
                self.inner.recover_expired(limit).await
            }
        }
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().push_back(claimed());
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let factory: HandlerRegistryFactory = Arc::new({
            let calls = calls.clone();
            move || TaskHandlerRegistry::try_new(vec![Box::new(CountingHandler(calls.clone()))])
        });
        super::run(
            outbox_task_coordinator(
                Arc::new(CancelAfterClaim {
                    inner: queue.clone(),
                    shutdown: shutdown.clone(),
                }),
                vec![format()],
            ),
            factory,
            worker_config(1, 1, Duration::from_secs(3600)),
            shutdown,
        )
        .await
        .unwrap();
        assert!(queue.incoming.lock().unwrap().is_empty(), "claim did complete");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn startup_factory_panic_cancels_the_host_without_polling_storage() {
        let queue = Arc::new(Queue::default());
        let shutdown = CancellationToken::new();
        let supervisor = tokio::spawn(super::run(
            outbox_task_coordinator(queue.clone(), vec![format()]),
            Arc::new(|| panic!("injected startup factory panic")),
            worker_config(1, 1, Duration::from_secs(1)),
            shutdown.clone(),
        ));
        assert!(supervisor.await.unwrap_err().is_panic());
        assert!(shutdown.is_cancelled());
        assert!(queue.requested.lock().unwrap().is_empty());
    }
}
