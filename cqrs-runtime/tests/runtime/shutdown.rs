use super::*;

#[tokio::test]
async fn shutdown_drains_in_flight_work_and_invalid_worker_wiring_is_rejected() {
    let queue = Arc::new(Queue::default());
    let service = task_executor(queue.clone());
    let seen = Arc::new(AtomicUsize::new(0));
    let factory: TaskDispatcherFactory = Arc::new({
        let seen = seen.clone();
        move || TaskDispatcher::try_new(vec![Box::new(CountingAdapter(seen.clone()))])
    });
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let invalid =
        TaskWorkerPoolConfig::new(1, 1, Duration::from_secs(1), Duration::from_secs(30), Duration::from_secs(45)).unwrap();
    assert!(
        run(
            queue.clone(),
            service.clone(),
            factory.clone(),
            invalid,
            shutdown.child_token()
        )
        .await
        .is_err()
    );
    assert!(
        run(
            queue.clone(),
            task_executor(queue.clone()),
            Arc::new(|| TaskDispatcher::try_new(vec![])),
            TaskWorkerPoolConfig::default(),
            shutdown.child_token()
        )
        .await
        .is_err()
    );
    queue.incoming.lock().unwrap().push_back(claimed());
    let worker = tokio::spawn(run(
        queue.clone(),
        service,
        factory,
        TaskWorkerPoolConfig::default(),
        shutdown,
    ));
    wait_until(|| seen.load(Ordering::SeqCst) > 0).await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_stalled_polling_and_bounds_stalled_acknowledgements() {
    for stage in ["claim", "recovery", "settle"] {
        let queue = Arc::new(Queue::default());
        queue.hang_claim.store(stage == "claim", Ordering::SeqCst);
        queue.hang_recovery.store(stage == "recovery", Ordering::SeqCst);
        queue.hang_settle.store(stage == "settle", Ordering::SeqCst);
        queue.incoming.lock().unwrap().push_back(claimed());
        let service = task_executor(queue.clone());
        let factory: TaskDispatcherFactory = Arc::new(|| TaskDispatcher::try_new(vec![Box::new(adapter())]));
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let worker = tokio::spawn(run(
            queue.clone(),
            service,
            factory,
            TaskWorkerPoolConfig::default(),
            shutdown,
        ));
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
    let service = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_secs(10)).unwrap());
    let factory: TaskDispatcherFactory = Arc::new(|| {
        let mut handler = adapter();
        handler.hang = true;
        TaskDispatcher::try_new(vec![Box::new(handler)])
    });
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let started = tokio::time::Instant::now();
    let worker = tokio::spawn(run(
        queue.clone(),
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
async fn cancellation_of_supervisor_aborts_owned_workers_without_detaching_them() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let pool = tokio::spawn(
        prepared_worker(
            queue.clone(),
            io_factory(probe.clone()),
            worker_config(1, 1, Duration::from_secs(3600)),
        )
        .run(shutdown.clone()),
    );
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
    let pool = tokio::spawn(run(
        queue.clone(),
        task_executor(queue.clone()),
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
    impl TaskQueue for CancelAfterClaim {
        async fn claim(
            &self,
            format: &TaskFormat,
            owner: &str,
            limit: usize,
            lease: Duration,
        ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
            let tasks = self.inner.claim(format, owner, limit, lease).await?;
            self.shutdown.cancel();
            Ok(tasks)
        }
        async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
            self.inner.settle(task, request).await
        }
        async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
            self.inner.recover_expired(limit).await
        }
    }
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().push_back(claimed());
    let calls = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let factory: TaskDispatcherFactory = Arc::new({
        let calls = calls.clone();
        move || TaskDispatcher::try_new(vec![Box::new(CountingAdapter(calls.clone()))])
    });
    let cancelling_queue = Arc::new(CancelAfterClaim {
        inner: queue.clone(),
        shutdown: shutdown.clone(),
    });
    tokio::time::timeout(
        Duration::from_secs(1),
        run(
            cancelling_queue.clone(),
            task_executor(cancelling_queue),
            factory,
            worker_config(1, 1, Duration::from_secs(3600)),
            shutdown,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(queue.incoming.lock().unwrap().is_empty(), "claim did complete");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(queue.outcomes.lock().unwrap().is_empty());
}
