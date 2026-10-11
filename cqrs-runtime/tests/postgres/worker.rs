use cqrs_runtime::runtime::dispatcher_factory::WorkerSetupError;
use cqrs_runtime::task::data::attempt::TaskAttempt;
use std::sync::Mutex;

use super::*;
use cqrs_runtime::{
    postgres::{task_queue::config::PostgresTaskQueueConfig, task_runtime_factory::prepare_task_runtime},
    runtime::config::TaskRuntimeConfig,
    task::{handling::dispatcher::TaskDispatcherError, queue::storage_config::TaskStorageConfig},
};

struct DummyHandler(Arc<Mutex<Vec<(Uuid, String)>>>);

#[async_trait]
impl TaskAdapter for DummyHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&mut self, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
        let payload =
            serde_json::from_value::<DummyTaskV1>(delivery.payload().clone()).map_err(|_| TaskHandlerError::Permanent {
                reason: "invalid dummy task payload".to_owned(),
            })?;
        self.0.lock().unwrap().push((delivery.id(), payload.value));
        Ok(())
    }
}

#[tokio::test]
async fn two_live_workers_complete_every_supported_task_and_quarantine_bad_payloads() {
    let db = Database::new().await;
    let first = NewTask::new(DummyTaskV1 { value: "first".into() }).unwrap();
    let second = NewTask::new(DummyTaskV1 { value: "second".into() }).unwrap();
    let bad = NewTask::from_payload(format(), json!({})).unwrap();
    enqueue(&db.pool, vec![first.clone(), second.clone(), bad.clone()]).await;
    let handled = Arc::new(Mutex::new(Vec::new()));
    let factory: TaskDispatcherFactory = Arc::new({
        let handled = handled.clone();
        move || TaskDispatcher::try_new(vec![Box::new(DummyHandler(handled.clone()))])
    });
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let config = TaskWorkerPoolConfig::new(
        2,
        2,
        Duration::from_millis(10),
        TaskWorkerPoolConfig::default().handler_timeout(),
        TaskWorkerPoolConfig::default().lease_duration(),
    )
    .unwrap();
    let config = TaskRuntimeConfig::new(TaskStorageConfig::default(), config).unwrap();
    // A startup failure must leave existing durable work unclaimed.
    let failed = prepare_task_runtime(
        db.pool.clone(),
        Arc::new(|| Err(TaskDispatcherError::Empty)),
        PostgresTaskQueueConfig::default(),
        config.clone(),
    );
    assert!(matches!(
        failed,
        Err(WorkerSetupError::Build {
            worker_index: 0,
            source: TaskDispatcherError::Empty,
        })
    ));
    let left = prepare_task_runtime(
        db.pool.clone(),
        factory.clone(),
        PostgresTaskQueueConfig::default(),
        config.clone(),
    )
    .unwrap();
    let right = prepare_task_runtime(db.pool.clone(), factory, PostgresTaskQueueConfig::default(), config).unwrap();
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
    impl TaskAdapter for AuditHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat::new("audit", 1).unwrap()]
        }

        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let db = Database::new().await;
    let dummy = new_task();
    let audit = NewTask::from_payload(TaskFormat::new("audit", 1).unwrap(), json!({"kind":"runtime"})).unwrap();
    let unknown = NewTask::from_payload(TaskFormat::new("future", 1).unwrap(), json!({"value":"runtime"})).unwrap();
    enqueue(&db.pool, vec![dummy.clone(), audit.clone(), unknown.clone()]).await;

    let handled = Arc::new(Mutex::new(Vec::new()));
    let audited = Arc::new(AtomicUsize::new(0));
    let factory: TaskDispatcherFactory = Arc::new({
        let handled = handled.clone();
        let audited = audited.clone();
        move || {
            TaskDispatcher::try_new(vec![
                Box::new(DummyHandler(handled.clone())),
                Box::new(AuditHandler(audited.clone())),
            ])
        }
    });
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let prepared = prepare_task_runtime(
        db.pool.clone(),
        factory,
        PostgresTaskQueueConfig::default(),
        TaskRuntimeConfig::new(
            TaskStorageConfig::default(),
            TaskWorkerPoolConfig::new(
                2,
                2,
                Duration::from_millis(10),
                TaskWorkerPoolConfig::default().handler_timeout(),
                TaskWorkerPoolConfig::default().lease_duration(),
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
