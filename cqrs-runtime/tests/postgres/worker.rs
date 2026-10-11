use cqrs_runtime::outbox::TaskDelivery;
use std::sync::Mutex;

use super::*;
use cqrs_runtime::{
    outbox::{OutboxTaskCoordinatorConfig, TaskHandlerRegistryError},
    postgres::{PostgresOutboxPreparationError, PostgresOutboxQueueConfig, prepare_outbox_worker},
    worker::{OutboxPreparationError, OutboxRuntimeConfig, WorkerPoolError},
};

struct DummyHandler(Arc<Mutex<Vec<(Uuid, String)>>>);

#[async_trait]
impl TaskHandler for DummyHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn handle(&mut self, task: &TaskDelivery) -> Result<(), TaskHandlerError> {
        let payload = serde_json::from_value::<DummyTaskV1>(task.payload().clone()).map_err(|_| TaskHandlerError::Permanent {
            reason: "invalid dummy task payload".to_owned(),
        })?;
        self.0.lock().unwrap().push((task.id(), payload.value));
        Ok(())
    }
}

#[tokio::test]
async fn two_live_workers_complete_every_supported_task_and_quarantine_bad_payloads() {
    let db = Database::new().await;
    let first = NewOutboxTask::new(DummyTaskV1 { value: "first".into() }).unwrap();
    let second = NewOutboxTask::new(DummyTaskV1 { value: "second".into() }).unwrap();
    let bad = NewOutboxTask::from_payload(format(), json!({})).unwrap();
    enqueue(&db.pool, vec![first.clone(), second.clone(), bad.clone()]).await;
    let handled = Arc::new(Mutex::new(Vec::new()));
    let factory: HandlerRegistryFactory = Arc::new({
        let handled = handled.clone();
        move || TaskHandlerRegistry::try_new(vec![Box::new(DummyHandler(handled.clone()))])
    });
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let config = OutboxWorkerPoolConfig::new(
        2,
        2,
        Duration::from_millis(10),
        OutboxWorkerPoolConfig::default().handler_timeout(),
        OutboxWorkerPoolConfig::default().lease_duration(),
    )
    .unwrap();
    let config = OutboxRuntimeConfig::new(OutboxTaskCoordinatorConfig::default(), config).unwrap();
    // A startup failure must leave existing durable work unclaimed.
    let failed = prepare_outbox_worker(
        db.pool.clone(),
        Arc::new(|| Err(TaskHandlerRegistryError::Empty)),
        PostgresOutboxQueueConfig::default(),
        config.clone(),
    );
    assert!(matches!(
        failed,
        Err(PostgresOutboxPreparationError::Preparation(OutboxPreparationError::Worker(
            WorkerPoolError::RegistryBuild {
                worker_index: 0,
                source: TaskHandlerRegistryError::Empty,
            }
        )))
    ));
    let left = prepare_outbox_worker(
        db.pool.clone(),
        factory.clone(),
        PostgresOutboxQueueConfig::default(),
        config.clone(),
    )
    .unwrap();
    let right = prepare_outbox_worker(db.pool.clone(), factory, PostgresOutboxQueueConfig::default(), config).unwrap();
    for task in [&first, &second, &bad] {
        assert_worker_state(&db.pool, task.id(), "pending", 0, None).await;
    }
    assert!(handled.lock().unwrap().is_empty());
    let left = tokio::spawn(left.run(shutdown.clone()));
    let right = tokio::spawn(right.run(shutdown));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE finished_at IS NOT NULL")
                .fetch_one(&db.pool)
                .await
                .unwrap();
            if count == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    left.await.unwrap().unwrap();
    right.await.unwrap().unwrap();

    let handled = handled.lock().unwrap().clone();
    assert_eq!(handled.len(), 2);
    for (task, expected) in [(first, "first"), (second, "second")] {
        assert!(handled.contains(&(task.id(), expected.to_owned())));
        assert_eq!(row(&db.pool, task.id()).await.get::<String, _>("status"), "completed");
    }
    assert_eq!(row(&db.pool, bad.id()).await.get::<String, _>("status"), "failed");
    db.close().await;
}

#[tokio::test]
async fn one_worker_pool_routes_two_registered_task_types() {
    struct AuditHandler(Arc<AtomicUsize>);

    #[async_trait]
    impl TaskHandler for AuditHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat::new("audit", 1).unwrap()]
        }

        async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let db = Database::new().await;
    let dummy = new_task();
    let audit = NewOutboxTask::from_payload(TaskFormat::new("audit", 1).unwrap(), json!({"kind":"runtime"})).unwrap();
    let unknown = NewOutboxTask::from_payload(TaskFormat::new("future", 1).unwrap(), json!({"value":"runtime"})).unwrap();
    enqueue(&db.pool, vec![dummy.clone(), audit.clone(), unknown.clone()]).await;

    let handled = Arc::new(Mutex::new(Vec::new()));
    let audited = Arc::new(AtomicUsize::new(0));
    let factory: HandlerRegistryFactory = Arc::new({
        let handled = handled.clone();
        let audited = audited.clone();
        move || {
            TaskHandlerRegistry::try_new(vec![
                Box::new(DummyHandler(handled.clone())),
                Box::new(AuditHandler(audited.clone())),
            ])
        }
    });
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let prepared = prepare_outbox_worker(
        db.pool.clone(),
        factory,
        PostgresOutboxQueueConfig::default(),
        OutboxRuntimeConfig::new(
            OutboxTaskCoordinatorConfig::default(),
            OutboxWorkerPoolConfig::new(
                2,
                2,
                Duration::from_millis(10),
                OutboxWorkerPoolConfig::default().handler_timeout(),
                OutboxWorkerPoolConfig::default().lease_duration(),
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    for task in [&dummy, &audit, &unknown] {
        assert_worker_state(&db.pool, task.id(), "pending", 0, None).await;
    }
    assert!(handled.lock().unwrap().is_empty());
    assert_eq!(audited.load(Ordering::SeqCst), 0);
    let worker = tokio::spawn(prepared.run(shutdown));
    tokio::time::timeout(Duration::from_secs(10), async {
        while row(&db.pool, dummy.id()).await.get::<String, _>("status") != "completed"
            || row(&db.pool, audit.id()).await.get::<String, _>("status") != "completed"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    worker.await.unwrap().unwrap();
    assert_eq!(handled.lock().unwrap().len(), 1);
    assert_eq!(audited.load(Ordering::SeqCst), 1);
    assert_eq!(row(&db.pool, unknown.id()).await.get::<String, _>("status"), "pending");
    assert_eq!(row(&db.pool, unknown.id()).await.get::<i32, _>("attempts"), 0);
    db.close().await;
}
