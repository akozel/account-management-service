use super::*;

#[tokio::test]
async fn cancellation_during_restart_factory_does_not_start_a_replacement_worker() {
    struct PanickingAdapter;
    #[async_trait]
    impl TaskAdapter for PanickingAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            panic!("injected worker panic");
        }
    }
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let stop = CancellationToken::new();
    let builds = Arc::new(AtomicUsize::new(0));
    let replacement_calls = Arc::new(AtomicUsize::new(0));
    let factory: TaskDispatcherFactory = Arc::new({
        let builds = builds.clone();
        let stop = stop.clone();
        let replacement_calls = replacement_calls.clone();
        move || {
            if builds.fetch_add(1, Ordering::SeqCst) == 0 {
                TaskDispatcher::try_new(vec![Box::new(PanickingAdapter)])
            } else {
                stop.cancel();
                TaskDispatcher::try_new(vec![Box::new(CountingAdapter(replacement_calls.clone()))])
            }
        }
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        run(
            queue.clone(),
            task_executor(queue.clone()),
            factory,
            worker_config(1, 1, Duration::from_secs(3600)),
            stop.clone(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(stop.is_cancelled());
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    assert_eq!(replacement_calls.load(Ordering::SeqCst), 0);
    assert!(queue.outcomes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn supervisor_recovers_workers_and_survives_storage_errors_and_lost_acknowledgements() {
    let queue = Arc::new(Queue::default());
    queue.claim_error.store(true, Ordering::SeqCst);
    queue.recovery_error.store(true, Ordering::SeqCst);
    let service = task_executor(queue.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    struct PanicOnce {
        calls: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl TaskAdapter for PanicOnce {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            assert_ne!(self.calls.fetch_add(1, Ordering::SeqCst), 0, "injected worker failure");
            Ok(())
        }
    }
    let observed = calls.clone();
    let factory: TaskDispatcherFactory =
        Arc::new(move || TaskDispatcher::try_new(vec![Box::new(PanicOnce { calls: observed.clone() })]));
    let config = TaskWorkerPoolConfig::new(
        1,
        1,
        Duration::from_millis(2),
        TaskWorkerPoolConfig::default().handler_timeout(),
        TaskWorkerPoolConfig::default().lease_duration(),
    )
    .unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let worker = tokio::spawn(run(queue.clone(), service, factory, config, shutdown));
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
async fn storage_timeouts_allow_polling_and_free_worker_capacity_without_shutdown() {
    for stage in ["claim", "recovery", "settle"] {
        let queue = Arc::new(Queue::default());
        queue.hang_claim.store(stage == "claim", Ordering::SeqCst);
        queue.hang_recovery.store(stage == "recovery", Ordering::SeqCst);
        queue.hang_settle.store(stage == "settle", Ordering::SeqCst);
        queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
        let service = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_millis(5)).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let factory: TaskDispatcherFactory = Arc::new({
            let calls = calls.clone();
            move || TaskDispatcher::try_new(vec![Box::new(CountingAdapter(calls.clone()))])
        });
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let worker = tokio::spawn(run(
            queue.clone(),
            service,
            factory,
            TaskWorkerPoolConfig::new(
                1,
                1,
                Duration::from_millis(2),
                TaskWorkerPoolConfig::default().handler_timeout(),
                TaskWorkerPoolConfig::default().lease_duration(),
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

#[tokio::test]
async fn registry_build_failures_are_typed_on_startup_and_restart() {
    struct PanicAdapter;
    #[async_trait]
    impl TaskAdapter for PanicAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            panic!("injected worker failure");
        }
    }
    for fail_on_restart in [false, true] {
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().push_back(claimed());
        let calls = Arc::new(AtomicUsize::new(0));
        let factory: TaskDispatcherFactory = Arc::new(move || {
            if fail_on_restart && calls.fetch_add(1, Ordering::SeqCst) == 0 {
                TaskDispatcher::try_new(vec![Box::new(PanicAdapter)])
            } else {
                Err(TaskDispatcherError::Empty)
            }
        });
        let executor = task_executor(queue.clone());
        let shutdown = CancellationToken::new();
        let config = TaskWorkerPoolConfig::new(
            1,
            1,
            Duration::from_millis(2),
            Duration::from_secs(30),
            Duration::from_secs(120),
        )
        .unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            run(queue.clone(), executor, factory, config, shutdown),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(
            error,
            TaskRuntimeError::WorkerSetup(WorkerSetupError::Build {
                worker_index: 0,
                source: TaskDispatcherError::Empty
            })
        );
        assert_eq!(queue.requested.lock().unwrap().len(), usize::from(fail_on_restart));
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn sequential_deliveries_preserve_registry_state_and_tokio_task_identity() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let pool = tokio::spawn(
        prepared_worker(
            queue.clone(),
            io_factory(probe.clone()),
            worker_config(1, 1, Duration::from_millis(2)),
        )
        .run(shutdown),
    );
    wait_until(|| probe.calls.lock().unwrap().len() == 1).await;
    probe.gate.add_permits(1);
    wait_until(|| probe.calls.lock().unwrap().len() == 2).await;
    let calls = probe.calls.lock().unwrap().clone();
    assert_eq!(calls[0].0, 0, "the first prepared dispatcher must execute");
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
        dispatcher: usize,
        records: Arc<std::sync::Mutex<Vec<(usize, Id)>>>,
    }
    #[async_trait]
    impl TaskAdapter for PanicFirstRegistry {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            self.records.lock().unwrap().push((self.dispatcher, tokio::task::id()));
            assert_ne!(self.dispatcher, 0, "injected failure");
            Ok(())
        }
    }
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let built = Arc::new(AtomicUsize::new(0));
    let factory: TaskDispatcherFactory = Arc::new({
        let records = records.clone();
        let built = built.clone();
        move || {
            TaskDispatcher::try_new(vec![Box::new(PanicFirstRegistry {
                dispatcher: built.fetch_add(1, Ordering::SeqCst),
                records: records.clone(),
            })])
        }
    });
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let pool =
        tokio::spawn(prepared_worker(queue.clone(), factory, worker_config(1, 1, Duration::from_secs(3600))).run(shutdown));
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
    struct OtherAdapter;
    #[async_trait]
    impl TaskAdapter for OtherAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat::new("other", 1).unwrap()]
        }
        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            panic!("mismatched dispatcher must never execute");
        }
    }
    struct PanicOnDemand(Arc<tokio::sync::Semaphore>);
    #[async_trait]
    impl TaskAdapter for PanicOnDemand {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            self.0.acquire().await.unwrap().forget();
            panic!("injected failure");
        }
    }
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend((0..5).map(|_| claimed()));
    let probe = Arc::new(IoProbe::default());
    let crash = Arc::new(tokio::sync::Semaphore::new(0));
    let built = Arc::new(AtomicUsize::new(0));
    let factory: TaskDispatcherFactory = Arc::new({
        let probe = probe.clone();
        let crash = crash.clone();
        let built = built.clone();
        move || match built.fetch_add(1, Ordering::SeqCst) {
            0 => TaskDispatcher::try_new(vec![Box::new(PanicOnDemand(crash.clone()))]),
            1 => TaskDispatcher::try_new(vec![Box::new(IoAdapter {
                probe: probe.clone(),
                dispatcher: 1,
                calls: 0,
            })]),
            _ if mismatch => TaskDispatcher::try_new(vec![Box::new(OtherAdapter)]),
            _ => Err(TaskDispatcherError::Empty),
        }
    });
    let parent = CancellationToken::new();
    let shutdown = parent.child_token();
    let pool = tokio::spawn(
        prepared_worker(queue.clone(), factory, worker_config(2, 2, Duration::from_secs(3600))).run(shutdown.clone()),
    );
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
        TaskRuntimeError::WorkerSetup(WorkerSetupError::FormatsMismatch { worker_index: 0 })
    } else {
        TaskRuntimeError::WorkerSetup(WorkerSetupError::Build {
            worker_index: 0,
            source: TaskDispatcherError::Empty,
        })
    };
    assert_eq!(pool.await.unwrap(), Err(expected));
    assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}
