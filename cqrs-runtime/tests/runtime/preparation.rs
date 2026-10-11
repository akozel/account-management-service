use super::*;

#[tokio::test]
async fn invalid_full_lease_budget_is_rejected_before_polling() {
    let queue = Arc::new(Queue::default());
    let service = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_secs(10)).unwrap());
    let factory: TaskDispatcherFactory = Arc::new(|| TaskDispatcher::try_new(vec![Box::new(adapter())]));
    let shutdown = CancellationToken::new();
    assert!(
        run(
            queue.clone(),
            service,
            factory,
            TaskWorkerPoolConfig::new(
                TaskWorkerPoolConfig::default().concurrency(),
                TaskWorkerPoolConfig::default().concurrency(),
                TaskWorkerPoolConfig::default().poll_interval(),
                TaskWorkerPoolConfig::default().handler_timeout(),
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
async fn validates_every_workers_registry_before_polling() {
    struct OtherAdapter;

    #[async_trait]
    impl TaskAdapter for OtherAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat::new("other", 1).unwrap()]
        }

        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            Ok(())
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let factory: TaskDispatcherFactory = Arc::new({
        let calls = calls.clone();
        move || {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                TaskDispatcher::try_new(vec![Box::new(adapter())])
            } else {
                TaskDispatcher::try_new(vec![Box::new(OtherAdapter)])
            }
        }
    });
    let queue = Arc::new(Queue::default());
    let service = task_executor(queue.clone());
    let shutdown = CancellationToken::new();
    let result = run(
        queue.clone(),
        service,
        factory,
        TaskWorkerPoolConfig::new(
            2,
            2,
            TaskWorkerPoolConfig::default().poll_interval(),
            TaskWorkerPoolConfig::default().handler_timeout(),
            TaskWorkerPoolConfig::default().lease_duration(),
        )
        .unwrap(),
        shutdown,
    )
    .await;
    assert_eq!(
        result,
        Err(TaskRuntimeError::WorkerSetup(WorkerSetupError::FormatsMismatch {
            worker_index: 1
        }))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn initially_cancelled_shutdown_does_not_build_workers_or_claim() {
    let queue = Arc::new(Queue::default());
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    run(
        queue.clone(),
        task_executor(queue.clone()),
        Arc::new(|| panic!("cancelled startup must not build a dispatcher")),
        worker_config(2, 1, Duration::from_secs(1)),
        shutdown,
    )
    .await
    .unwrap();
    assert!(queue.requested.lock().unwrap().is_empty());
    assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn startup_factory_panic_cancels_the_host_without_polling_storage() {
    let queue = Arc::new(Queue::default());
    let shutdown = CancellationToken::new();
    let supervisor = tokio::spawn(run(
        queue.clone(),
        task_executor(queue.clone()),
        Arc::new(|| panic!("injected startup factory panic")),
        worker_config(1, 1, Duration::from_secs(1)),
        shutdown.clone(),
    ));
    assert!(supervisor.await.unwrap_err().is_panic());
    assert!(shutdown.is_cancelled());
    assert!(queue.requested.lock().unwrap().is_empty());
}
