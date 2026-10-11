use crate::task::{
    executor::{default_executor::task_executor_with_config, port::TaskExecutor},
    handling::dispatcher::TaskDispatcher,
    queue::port::TaskQueue,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use {
    super::{dispatcher_factory::build_checked_dispatcher, supervisor::run_prepared},
    crate::runtime::{
        config::TaskRuntimeConfig,
        dispatcher_factory::{TaskDispatcherFactory, WorkerSetupError},
        supervisor::TaskRuntimeError,
        worker_pool_config::TaskWorkerPoolConfig,
    },
};

/// Owns exactly C checked startup dispatchers and their restart factory.
/// Preparation neither starts tasks nor owns a cancellation token. The run
/// future binds a token; its DropGuard starts at first poll.
/// Dropping this object simply releases its dispatchers. It cannot be cloned.
pub struct TaskRuntime {
    queue: Arc<dyn TaskQueue>,
    executor: Arc<dyn TaskExecutor>,
    factory: TaskDispatcherFactory,
    config: TaskWorkerPoolConfig,
    dispatchers: Vec<TaskDispatcher>,
}

impl TaskRuntime {
    /// Consume the prepared dispatchers without rebuilding startup dispatchers.
    /// At first poll a DropGuard owns cancellation; a pre-cancelled token skips
    /// spawn/claims/recovery. Active work retains the existing H+S drain contract.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), TaskRuntimeError> {
        run_prepared(
            self.queue,
            self.executor,
            self.factory,
            self.config,
            self.dispatchers,
            shutdown,
        )
        .await
    }
}

/// Assemble all startup dispatchers and the executor synchronously, before
/// opening inbound listeners. Does not perform storage I/O or spawn Tokio tasks.
/// Factories must return promptly and only construct dispatchers, not begin effects.
/// The first dispatcher supplies ordered formats and becomes worker zero's dispatcher.
pub fn prepare(
    queue: Arc<dyn TaskQueue>,
    factory: TaskDispatcherFactory,
    config: TaskRuntimeConfig,
) -> Result<TaskRuntime, WorkerSetupError> {
    let (storage_config, worker_config) = config.into_parts();
    let first = factory().map_err(|source| WorkerSetupError::Build { worker_index: 0, source })?;
    let formats = first.formats().to_vec();
    let dispatchers = std::iter::once(Ok(first))
        .chain((1..worker_config.concurrency()).map(|index| build_checked_dispatcher(&factory, &formats, index)))
        .collect::<Result<Vec<_>, _>>()?;
    let executor = task_executor_with_config(queue.clone(), storage_config);
    Ok(TaskRuntime {
        queue,
        executor,
        factory,
        config: worker_config,
        dispatchers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{
        data::{attempt::TaskAttempt, format::TaskFormat},
        handling::{adapter::TaskAdapter, dispatcher::TaskDispatcherError, handler::TaskHandlerError},
        queue::storage_config::TaskStorageConfig,
        test_support::{Queue, format},
    };
    use async_trait::async_trait;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    struct TrackedAdapter {
        formats: Vec<TaskFormat>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for TrackedAdapter {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[async_trait]
    impl TaskAdapter for TrackedAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            self.formats.clone()
        }
        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            Ok(())
        }
    }
    fn dispatcher(formats: Vec<TaskFormat>, dropped: Arc<AtomicUsize>) -> Result<TaskDispatcher, TaskDispatcherError> {
        TaskDispatcher::try_new(vec![Box::new(TrackedAdapter { formats, dropped })])
    }
    fn config(c: usize) -> TaskRuntimeConfig {
        TaskRuntimeConfig::new(
            TaskStorageConfig::default(),
            TaskWorkerPoolConfig::new(c, c, Duration::from_secs(1), Duration::from_secs(15), Duration::from_secs(60)).unwrap(),
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
        let factory: TaskDispatcherFactory = Arc::new({
            let built = built.clone();
            let dropped = dropped.clone();
            move || {
                built.fetch_add(1, Ordering::SeqCst);
                dispatcher(vec![format()], dropped.clone())
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
            let factory: TaskDispatcherFactory = Arc::new({
                let built = built.clone();
                let dropped = dropped.clone();
                let first = first.clone();
                move || {
                    dispatcher(
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
            assert_eq!(error, WorkerSetupError::FormatsMismatch { worker_index: 1 });
            assert!(std::error::Error::source(&error).is_none());
            assert_eq!(built.load(Ordering::SeqCst), 2);
            assert_eq!(dropped.load(Ordering::SeqCst), 2);
            assert_no_storage(&queue);
        }
    }

    #[test]
    fn empty_duplicate_and_later_factory_failures_are_indexed_before_storage_work() {
        for (fail_at, formats, source, dropped_count) in [
            (0, vec![], TaskDispatcherError::Empty, 1),
            (0, vec![format(), format()], TaskDispatcherError::DuplicateFormat(format()), 1),
            (2, vec![], TaskDispatcherError::Empty, 3),
        ] {
            let queue = Arc::new(Queue::default());
            let built = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let factory: TaskDispatcherFactory = Arc::new({
                let built = built.clone();
                let dropped = dropped.clone();
                move || {
                    dispatcher(
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
                WorkerSetupError::Build {
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
        let factory: TaskDispatcherFactory = Arc::new({
            let built = built.clone();
            let dropped = dropped.clone();
            move || {
                built.fetch_add(1, Ordering::SeqCst);
                dispatcher(vec![format()], dropped.clone())
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
