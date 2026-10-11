use super::*;

#[tokio::test]
async fn bounded_admission_is_independent_of_concurrency_and_shutdown_leaves_queue_for_recovery() {
    for (c, q) in [(2, 3), (2, 1), (3, 1)] {
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend((0..20).map(|_| claimed()));
        let probe = Arc::new(IoProbe::default());
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let pool = tokio::spawn(
            prepared_worker(
                queue.clone(),
                io_factory(probe.clone()),
                worker_config(c, q, Duration::from_millis(2)),
            )
            .run(shutdown),
        );
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
    struct PausedClaimQueue {
        inner: Arc<dyn TaskQueue>,
        calls: AtomicUsize,
        gate: tokio::sync::Semaphore,
    }
    #[async_trait]
    impl TaskQueue for PausedClaimQueue {
        async fn claim(
            &self,
            format: &TaskFormat,
            owner: &str,
            limit: usize,
            lease: Duration,
        ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 2 {
                self.gate.acquire().await.unwrap().forget();
            }
            self.inner.claim(format, owner, limit, lease).await
        }
        async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
            self.inner.settle(task, request).await
        }
        async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
            self.inner.recover_expired(limit).await
        }
    }
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend((0..7).map(|_| claimed()));
    let paused_queue = Arc::new(PausedClaimQueue {
        inner: queue.clone(),
        calls: AtomicUsize::new(0),
        gate: tokio::sync::Semaphore::new(0),
    });
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let pool = tokio::spawn(run(
        paused_queue.clone(),
        task_executor(paused_queue.clone()),
        io_factory(probe.clone()),
        worker_config(1, 2, Duration::from_secs(3600)),
        shutdown,
    ));
    // One active delivery and two waiting; the next claim starts on dequeue.
    wait_until(|| probe.active.load(Ordering::SeqCst) == 1 && queue.incoming.lock().unwrap().len() == 4).await;
    probe.gate.add_permits(1);
    wait_until(|| paused_queue.calls.load(Ordering::SeqCst) == 3).await;
    // Finish the second delivery and dequeue the third while claim is pending.
    probe.gate.add_permits(1);
    wait_until(|| probe.calls.lock().unwrap().len() == 3).await;
    assert_eq!(queue.outcomes.lock().unwrap().len(), 2);
    assert_eq!(queue.incoming.lock().unwrap().len(), 4);
    assert_eq!(
        paused_queue.calls.load(Ordering::SeqCst),
        3,
        "dequeue notifications cannot start a parallel claim"
    );
    paused_queue.gate.add_permits(1);
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
    struct RecordingAdapter(Arc<std::sync::Mutex<Vec<Uuid>>>);
    #[async_trait]
    impl TaskAdapter for RecordingAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&mut self, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
            self.0.lock().unwrap().push(delivery.id());
            tokio::task::yield_now().await;
            Ok(())
        }
    }
    let tasks: Vec<_> = (0..1000).map(|_| claimed()).collect();
    let mut expected: Vec<_> = tasks.iter().map(|task| task.attempt().id()).collect();
    expected.sort_unstable();
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend(tasks);
    let executed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let factory: TaskDispatcherFactory = Arc::new({
        let executed = executed.clone();
        move || TaskDispatcher::try_new(vec![Box::new(RecordingAdapter(executed.clone()))])
    });
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let pool = tokio::spawn(
        prepared_worker(
            queue.clone(),
            factory,
            // More deliveries than C+Q exercises batch admission and Notify refill.
            worker_config(16, 32, Duration::from_secs(3600)),
        )
        .run(shutdown),
    );
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
    let pool = tokio::spawn(
        prepared_worker(
            queue.clone(),
            io_factory(probe.clone()),
            worker_config(1000, 1000, Duration::from_millis(2)),
        )
        .run(shutdown),
    );
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
