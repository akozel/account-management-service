use cqrs_runtime::outbox::TaskDelivery;
use std::sync::Mutex;

use super::*;

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
    let service = outbox_task_coordinator(Arc::new(PostgresOutboxQueue::new(db.pool.clone())), vec![format()]);
    let (stop, shutdown) = watch::channel(false);
    let config = OutboxWorkerPoolConfig::new(
        2,
        2,
        Duration::from_millis(10),
        OutboxWorkerPoolConfig::default().handler_timeout(),
        OutboxWorkerPoolConfig::default().lease_duration(),
    )
    .unwrap();
    let left = tokio::spawn(outbox_worker_pool::run(
        service.clone(),
        factory.clone(),
        config.clone(),
        shutdown.clone(),
    ));
    let right = tokio::spawn(outbox_worker_pool::run(service, factory, config, shutdown));
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
    stop.send(true).unwrap();
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
    let service = outbox_task_coordinator(
        Arc::new(PostgresOutboxQueue::new(db.pool.clone())),
        factory().unwrap().formats(),
    );
    let (stop, shutdown) = watch::channel(false);
    let worker = tokio::spawn(outbox_worker_pool::run(
        service,
        factory,
        OutboxWorkerPoolConfig::new(
            2,
            2,
            Duration::from_millis(10),
            OutboxWorkerPoolConfig::default().handler_timeout(),
            OutboxWorkerPoolConfig::default().lease_duration(),
        )
        .unwrap(),
        shutdown,
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while row(&db.pool, dummy.id()).await.get::<String, _>("status") != "completed"
            || row(&db.pool, audit.id()).await.get::<String, _>("status") != "completed"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    assert_eq!(handled.lock().unwrap().len(), 1);
    assert_eq!(audited.load(Ordering::SeqCst), 1);
    assert_eq!(row(&db.pool, unknown.id()).await.get::<String, _>("status"), "pending");
    assert_eq!(row(&db.pool, unknown.id()).await.get::<i32, _>("attempts"), 0);
    db.close().await;
}
