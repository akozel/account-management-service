use super::{
    HandlerRegistryFactory, OutboxRuntimeConfig, OutboxWorkerPoolConfig, OutboxWorkerPoolError, WorkerRegistryError,
    supervisor::{build_checked_registry, run_prepared},
};
use crate::outbox::{OutboxQueue, OutboxTaskCoordinator, TaskHandlerRegistry, outbox_task_coordinator_with_config};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Owns exactly C checked startup registries and their restart factory.
/// Preparation neither starts tasks nor owns a cancellation token. The run
/// future binds a token; its DropGuard starts at first poll.
/// Dropping this object simply releases its registries. It cannot be cloned.
pub struct PreparedOutboxRuntime {
    coordinator: Arc<dyn OutboxTaskCoordinator>,
    factory: HandlerRegistryFactory,
    config: OutboxWorkerPoolConfig,
    registries: Vec<TaskHandlerRegistry>,
}

impl PreparedOutboxRuntime {
    /// Consume the prepared registries without rebuilding startup registries.
    /// At first poll a DropGuard owns cancellation; a pre-cancelled token skips
    /// spawn/claims/recovery. Active work retains the existing H+S drain contract.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), OutboxWorkerPoolError> {
        run_prepared(self.coordinator, self.factory, self.config, self.registries, shutdown).await
    }
}

/// Assemble all startup registries and the coordinator synchronously, before
/// opening inbound listeners. Does not perform storage I/O or spawn Tokio tasks.
/// Factories must return promptly and only construct registries, not begin effects.
/// The first registry supplies ordered formats and becomes worker zero's registry.
pub fn prepare(
    queue: Arc<dyn OutboxQueue>,
    factory: HandlerRegistryFactory,
    config: OutboxRuntimeConfig,
) -> Result<PreparedOutboxRuntime, WorkerRegistryError> {
    let (coordinator_config, worker_config) = config.into_parts();
    let first = factory().map_err(|source| WorkerRegistryError::Build { worker_index: 0, source })?;
    let formats = first.formats().to_vec();
    let registries = std::iter::once(Ok(first))
        .chain((1..worker_config.concurrency()).map(|index| build_checked_registry(&factory, &formats, index)))
        .collect::<Result<Vec<_>, _>>()?;
    let coordinator = outbox_task_coordinator_with_config(queue, formats, coordinator_config);
    Ok(PreparedOutboxRuntime {
        coordinator,
        factory,
        config: worker_config,
        registries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::{
        OutboxTaskCoordinatorConfig, TaskDelivery, TaskFormat, TaskHandler, TaskHandlerError, TaskHandlerRegistryError,
        test_support::{Queue, format},
    };
    use async_trait::async_trait;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    struct TrackedHandler {
        formats: Vec<TaskFormat>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for TrackedHandler {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[async_trait]
    impl TaskHandler for TrackedHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            self.formats.clone()
        }
        async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
            Ok(())
        }
    }
    fn registry(formats: Vec<TaskFormat>, dropped: Arc<AtomicUsize>) -> Result<TaskHandlerRegistry, TaskHandlerRegistryError> {
        TaskHandlerRegistry::try_new(vec![Box::new(TrackedHandler { formats, dropped })])
    }
    fn config(c: usize) -> OutboxRuntimeConfig {
        OutboxRuntimeConfig::new(
            OutboxTaskCoordinatorConfig::default(),
            OutboxWorkerPoolConfig::new(c, c, Duration::from_secs(1), Duration::from_secs(15), Duration::from_secs(60)).unwrap(),
        )
        .unwrap()
    }
    fn assert_no_storage(queue: &Queue) {
        assert!(queue.requested.lock().unwrap().is_empty());
        assert!(queue.outcomes.lock().unwrap().is_empty());
        assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn synchronous_preparation_builds_exactly_c_registries_without_a_tokio_runtime() {
        let queue = Arc::new(Queue::default());
        let built = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let factory: HandlerRegistryFactory = Arc::new({
            let built = built.clone();
            let dropped = dropped.clone();
            move || {
                built.fetch_add(1, Ordering::SeqCst);
                registry(vec![format()], dropped.clone())
            }
        });
        let prepared = prepare(queue.clone(), factory, config(3)).unwrap();
        assert_eq!(built.load(Ordering::SeqCst), 3);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_no_storage(&queue);
        drop(prepared);
        assert_eq!(dropped.load(Ordering::SeqCst), 3);
        assert_no_storage(&queue);
    }

    #[test]
    fn format_name_version_and_order_mismatches_drop_partial_startup_registries() {
        let first = vec![format(), TaskFormat::new("audit", 1).unwrap()];
        let mut reversed = first.clone();
        reversed.reverse();
        for second in [
            vec![TaskFormat::new("other", 1).unwrap(), first[1].clone()],
            vec![TaskFormat::new(first[0].task_type(), 2).unwrap(), first[1].clone()],
            reversed,
        ] {
            let queue = Arc::new(Queue::default());
            let built = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let factory: HandlerRegistryFactory = Arc::new({
                let built = built.clone();
                let dropped = dropped.clone();
                let first = first.clone();
                move || {
                    registry(
                        if built.fetch_add(1, Ordering::SeqCst) == 0 {
                            first.clone()
                        } else {
                            second.clone()
                        },
                        dropped.clone(),
                    )
                }
            });
            let error = prepare(queue.clone(), factory, config(3)).err().unwrap();
            assert_eq!(error, WorkerRegistryError::FormatsMismatch { worker_index: 1 });
            assert!(std::error::Error::source(&error).is_none());
            assert_eq!(built.load(Ordering::SeqCst), 2);
            assert_eq!(dropped.load(Ordering::SeqCst), 2);
            assert_no_storage(&queue);
        }
    }

    #[test]
    fn empty_duplicate_and_later_factory_failures_are_indexed_before_storage_work() {
        for (fail_at, formats, source, dropped_count) in [
            (0, vec![], TaskHandlerRegistryError::Empty, 1),
            (
                0,
                vec![format(), format()],
                TaskHandlerRegistryError::DuplicateFormat(format()),
                1,
            ),
            (2, vec![], TaskHandlerRegistryError::Empty, 3),
        ] {
            let queue = Arc::new(Queue::default());
            let built = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let factory: HandlerRegistryFactory = Arc::new({
                let built = built.clone();
                let dropped = dropped.clone();
                move || {
                    registry(
                        if built.fetch_add(1, Ordering::SeqCst) == fail_at {
                            formats.clone()
                        } else {
                            vec![format()]
                        },
                        dropped.clone(),
                    )
                }
            });
            let error = prepare(queue.clone(), factory, config(4)).err().unwrap();
            assert_eq!(
                error,
                WorkerRegistryError::Build {
                    worker_index: fail_at,
                    source
                }
            );
            assert_eq!(built.load(Ordering::SeqCst), fail_at + 1);
            assert_eq!(dropped.load(Ordering::SeqCst), dropped_count);
            assert_no_storage(&queue);
        }
    }

    #[tokio::test]
    async fn cancelled_prepared_run_releases_handlers_without_spawn_or_storage() {
        let queue = Arc::new(Queue::default());
        let built = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let factory: HandlerRegistryFactory = Arc::new({
            let built = built.clone();
            let dropped = dropped.clone();
            move || {
                built.fetch_add(1, Ordering::SeqCst);
                registry(vec![format()], dropped.clone())
            }
        });
        let prepared = prepare(queue.clone(), factory, config(2)).unwrap();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        tokio::spawn(prepared.run(shutdown)).await.unwrap().unwrap();
        assert_eq!(built.load(Ordering::SeqCst), 2);
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        assert_no_storage(&queue);
    }
}
