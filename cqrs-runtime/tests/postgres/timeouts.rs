use super::*;
use cqrs_runtime::task::queue::lease::TaskLease;
use cqrs_runtime::{
    postgres::task_queue::config::PostgresTaskQueueConfig,
    task::{executor::default_executor::task_executor_with_config, queue::storage_config::TaskStorageConfig},
};

struct LostReplyQueue {
    inner: PostgresTaskQueue,
    lose_claim_reply: std::sync::atomic::AtomicBool,
    lose_settle_reply: std::sync::atomic::AtomicBool,
    delayed_format: Option<TaskFormat>,
}

#[async_trait]
impl TaskQueue for LostReplyQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        if self.delayed_format.as_ref() == Some(format) {
            std::future::pending::<()>().await;
        }
        let result = self.inner.claim(format, owner, limit, lease).await?;
        if self.lose_claim_reply.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok(result)
    }

    async fn settle(
        &self,
        task: &LeasedTask,
        outcome: SettlementRequest,
    ) -> Result<cqrs_runtime::task::queue::settlement::SettlementResult, TaskProcessingError> {
        let result = self.inner.settle(task, outcome).await?;
        if self.lose_settle_reply.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok(result)
    }

    async fn recover_expired(
        &self,
        limit: usize,
    ) -> Result<cqrs_runtime::task::queue::settlement::LeaseRecoveryReport, TaskProcessingError> {
        self.inner.recover_expired(limit).await
    }
}

#[tokio::test]
async fn deadlines_after_committed_claim_or_settlement_preserve_durable_outcomes() {
    let db = Database::new().await;
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let queue = Arc::new(LostReplyQueue {
        inner: PostgresTaskQueue::new(db.pool.clone()),
        lose_claim_reply: std::sync::atomic::AtomicBool::new(true),
        lose_settle_reply: std::sync::atomic::AtomicBool::new(true),
        delayed_format: None,
    });
    let executor = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_millis(200)).unwrap());
    assert_eq!(
        claim_with_timeout(queue.as_ref(), executor.storage_timeout(), "test", 1, Duration::from_secs(60))
            .await
            .unwrap_err(),
        TaskProcessingError::TimedOut
    );
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    expire(&db.pool, task.id()).await;
    assert_eq!(
        tokio::time::timeout(executor.storage_timeout(), queue.recover_expired(1))
            .await
            .unwrap_or(Err(TaskProcessingError::TimedOut))
            .unwrap()
            .processed_count(),
        1
    );
    make_ready(&db.pool, task.id()).await;
    queue.lose_claim_reply.store(false, Ordering::SeqCst);
    let owned = claim_with_timeout(queue.as_ref(), executor.storage_timeout(), "test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(owned.attempt().id(), task.id());
    assert_eq!(
        executor
            .execute_and_settle(&mut dispatcher(adapter(Ok(()))), owned, Duration::from_secs(1))
            .await,
        Err(TaskProcessingError::TimedOut)
    );
    assert_worker_state(&db.pool, task.id(), "completed", 2, None).await;
    assert_eq!(
        tokio::time::timeout(executor.storage_timeout(), queue.recover_expired(1))
            .await
            .unwrap_or(Err(TaskProcessingError::TimedOut))
            .unwrap()
            .processed_count(),
        0
    );
    assert!(
        claim_with_timeout(queue.as_ref(), executor.storage_timeout(), "test", 1, Duration::from_secs(60))
            .await
            .unwrap()
            .is_empty()
    );
    db.close().await;
}

#[tokio::test]
async fn delayed_batch_keeps_the_original_task_deadline_and_skips_expired_execution() {
    let db = Database::new().await;
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let delayed_format = TaskFormat::new("delayed", 1).unwrap();
    let queue = Arc::new(LostReplyQueue {
        inner: PostgresTaskQueue::new(db.pool.clone()),
        lose_claim_reply: std::sync::atomic::AtomicBool::new(false),
        lose_settle_reply: std::sync::atomic::AtomicBool::new(false),
        delayed_format: Some(delayed_format.clone()),
    });
    let executor = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_millis(300)).unwrap());
    let deadline = tokio::time::Instant::now() + executor.storage_timeout();
    let mut batch = tokio::time::timeout_at(deadline, queue.claim(&format(), "test", 2, Duration::from_millis(100)))
        .await
        .unwrap()
        .unwrap();
    // The next format consumes the same batch deadline after the first claim
    // committed. Its wait must not refresh the first task's lease clock.
    assert!(
        tokio::time::timeout_at(deadline, queue.claim(&delayed_format, "test", 1, Duration::from_millis(100)))
            .await
            .is_err()
    );
    let old = batch.pop().unwrap();
    assert!(old.lease().deadline() <= tokio::time::Instant::now());
    let calls = Arc::new(AtomicUsize::new(0));
    assert_eq!(
        executor
            .execute_and_settle(
                &mut dispatcher(CountingAdapter(calls.clone())),
                old.clone(),
                Duration::from_secs(1)
            )
            .await,
        Err(TaskProcessingError::LeaseBudgetExhausted)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    // Force server expiry as well: the conservative local deadline may precede it.
    expire(&db.pool, task.id()).await;
    assert_eq!(
        tokio::time::timeout(executor.storage_timeout(), queue.recover_expired(1))
            .await
            .unwrap_or(Err(TaskProcessingError::TimedOut))
            .unwrap()
            .processed_count(),
        1
    );
    make_ready(&db.pool, task.id()).await;
    let fresh = claim(&queue.inner).await.pop().unwrap();
    assert_ne!(old.lease().lock_token(), fresh.lease().lock_token());
    assert!(
        !queue
            .inner
            .settle(&old, SettlementRequest::Completed)
            .await
            .unwrap()
            .is_applied()
    );
    assert!(
        executor
            .execute_and_settle(&mut dispatcher(CountingAdapter(calls.clone())), fresh, Duration::from_secs(1))
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_worker_state(&db.pool, task.id(), "completed", 2, None).await;
    db.close().await;
}

#[tokio::test]
async fn insufficient_remaining_lease_returns_task_to_retry_without_performing_its_effect() {
    let db = Database::new().await;
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let executor = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_millis(300)).unwrap());
    let mut old = claim_with_timeout(queue.as_ref(), executor.storage_timeout(), "test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    // Model dispatch delay using a shorter conservative local budget; the DB
    // ownership remains live, so the original attempt can safely request retry.
    old = LeasedTask::new(
        old.attempt().clone(),
        TaskLease::new(
            old.lease().lock_token(),
            tokio::time::Instant::now() + Duration::from_secs(10),
        ),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        executor
            .execute_and_settle(
                &mut dispatcher(CountingAdapter(calls.clone())),
                old.clone(),
                Duration::from_secs(30)
            )
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_worker_state(
        &db.pool,
        task.id(),
        "pending",
        1,
        Some("insufficient lease budget before handler execution"),
    )
    .await;
    make_ready(&db.pool, task.id()).await;
    let fresh = claim_with_timeout(queue.as_ref(), executor.storage_timeout(), "test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(fresh.attempt().id(), old.attempt().id());
    assert_eq!(fresh.attempt().payload(), old.attempt().payload());
    assert_ne!(fresh.lease().lock_token(), old.lease().lock_token());
    assert!(
        executor
            .execute_and_settle(
                &mut dispatcher(CountingAdapter(calls.clone())),
                fresh,
                Duration::from_secs(30)
            )
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_worker_state(&db.pool, task.id(), "completed", 2, None).await;
    db.close().await;
}

async fn assert_session_timeout(pool: &PgPool) {
    let setting: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(pool).await.unwrap();
    assert_eq!(setting, "10s");
}

#[tokio::test]
async fn server_timeout_bounds_queue_lock_waits_rolls_back_and_restores_pool_settings() {
    let db = Database::new().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(db.pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    sqlx::query("SET statement_timeout = '10s'").execute(&pool).await.unwrap();
    let queue = PostgresTaskQueue::with_config(
        pool.clone(),
        PostgresTaskQueueConfig::new(Duration::from_millis(100)).unwrap(),
    );

    let processing = new_task();
    enqueue(&db.pool, vec![processing.clone()]).await;
    let owned = claim(&queue).await.pop().unwrap();
    let pending = new_task();
    enqueue(&db.pool, vec![pending.clone()]).await;
    assert_session_timeout(&pool).await;

    for operation in ["claim", "settle", "recovery"] {
        if operation == "recovery" {
            expire(&db.pool, processing.id()).await;
        }
        let mut blocker = db.pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE transactional_outbox IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            match operation {
                "claim" => queue.claim(&format(), "test", 1, Duration::from_secs(60)).await.map(|_| ()),
                "settle" => queue.settle(&owned, SettlementRequest::Completed).await.map(|_| ()),
                _ => queue.recover_expired(1).await.map(|_| ()),
            }
        })
        .await
        .expect("PostgreSQL must abort the statement before the outer test deadline");
        assert_eq!(result, Err(TaskProcessingError::Unavailable));
        blocker.rollback().await.unwrap();
        assert_session_timeout(&pool).await;
        assert_worker_state(&db.pool, pending.id(), "pending", 0, None).await;
        assert_worker_state(&db.pool, processing.id(), "processing", 1, None).await;
    }

    assert_eq!(queue.recover_expired(1).await.unwrap().processed_count(), 1);
    assert_session_timeout(&pool).await;
    make_ready(&db.pool, processing.id()).await;
    let tasks = queue.claim(&format(), "test", 2, Duration::from_secs(60)).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert!(!queue.settle(&owned, SettlementRequest::Completed).await.unwrap().is_applied());
    for task in tasks {
        assert!(queue.settle(&task, SettlementRequest::Completed).await.unwrap().is_applied());
    }
    assert_session_timeout(&pool).await;
    pool.close().await;
    db.close().await;
}

#[tokio::test]
async fn server_timeout_cancels_slow_execution_and_claim_changes_are_rolled_back() {
    let db = Database::new().await;
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION slow_outbox_update() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_sleep(1); RETURN NEW; END;
        $$;
        CREATE TRIGGER slow_outbox BEFORE UPDATE ON transactional_outbox
        FOR EACH ROW EXECUTE FUNCTION slow_outbox_update();
    "#,
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let queue = PostgresTaskQueue::with_config(
        db.pool.clone(),
        PostgresTaskQueueConfig::new(Duration::from_millis(100)).unwrap(),
    );
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        queue.claim(&format(), "test", 1, Duration::from_secs(60)),
    )
    .await
    .expect("the server must cancel slow execution before the test deadline");
    assert_eq!(result.unwrap_err(), TaskProcessingError::Unavailable);
    assert_worker_state(&db.pool, task.id(), "pending", 0, None).await;
    sqlx::raw_sql("DROP TRIGGER slow_outbox ON transactional_outbox; DROP FUNCTION slow_outbox_update();")
        .execute(&db.pool)
        .await
        .unwrap();
    let owned = claim(&queue).await.pop().unwrap();
    assert!(queue.settle(&owned, SettlementRequest::Completed).await.unwrap().is_applied());
    assert_worker_state(&db.pool, task.id(), "completed", 1, None).await;
    db.close().await;
}

#[tokio::test]
async fn runtime_deadline_includes_connection_acquisition() {
    let db = Database::new().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(db.pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let held_connection = pool.acquire().await.unwrap();
    let queue = Arc::new(PostgresTaskQueue::new(pool.clone()));
    let executor = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_millis(200)).unwrap());
    assert_eq!(
        claim_with_timeout(queue.as_ref(), executor.storage_timeout(), "test", 1, Duration::from_secs(60))
            .await
            .unwrap_err(),
        TaskProcessingError::TimedOut
    );
    assert_eq!(
        tokio::time::timeout(executor.storage_timeout(), queue.recover_expired(1))
            .await
            .unwrap_or(Err(TaskProcessingError::TimedOut))
            .unwrap_err(),
        TaskProcessingError::TimedOut
    );
    drop(held_connection);
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let owned = claim_with_timeout(queue.as_ref(), executor.storage_timeout(), "test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let held_connection = pool.acquire().await.unwrap();
    assert_eq!(
        executor
            .execute_and_settle(&mut dispatcher(adapter(Ok(()))), owned.clone(), Duration::from_secs(1))
            .await,
        Err(TaskProcessingError::TimedOut)
    );
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    drop(held_connection);
    assert!(
        executor
            .execute_and_settle(&mut dispatcher(adapter(Ok(()))), owned, Duration::from_secs(1))
            .await
            .unwrap()
            .is_applied()
    );
    assert_worker_state(&db.pool, task.id(), "completed", 1, None).await;
    pool.close().await;
    db.close().await;
}

#[cfg(feature = "worker")]
#[tokio::test]
async fn shutdown_after_committed_claim_or_settlement_preserves_durable_outcomes() {
    for stage in ["claim", "settle"] {
        let db = Database::new().await;
        let task = new_task();
        enqueue(&db.pool, vec![task.clone()]).await;
        let queue = Arc::new(LostReplyQueue {
            inner: PostgresTaskQueue::new(db.pool.clone()),
            lose_claim_reply: std::sync::atomic::AtomicBool::new(stage == "claim"),
            lose_settle_reply: std::sync::atomic::AtomicBool::new(stage == "settle"),
            delayed_format: None,
        });
        let executor = task_executor_with_config(queue.clone(), TaskStorageConfig::new(Duration::from_millis(300)).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let factory: TaskDispatcherFactory = Arc::new({
            let calls = calls.clone();
            move || TaskDispatcher::try_new(vec![Box::new(CountingAdapter(calls.clone()))])
        });
        let shutdown = CancellationToken::new();
        let worker = tokio::spawn(runtime::supervisor::run(
            queue.clone(),
            executor.clone(),
            factory,
            TaskWorkerPoolConfig::new(
                1,
                1,
                Duration::from_secs(3600),
                Duration::from_secs(1),
                Duration::from_secs(120),
            )
            .unwrap(),
            shutdown.clone(),
        ));
        let expected = if stage == "claim" { "processing" } else { "completed" };
        tokio::time::timeout(Duration::from_secs(10), async {
            while row(&db.pool, task.id()).await.get::<String, _>("status") != expected {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_worker_state(&db.pool, task.id(), expected, 1, None).await;
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(stage == "settle"));
        if stage == "claim" {
            expire(&db.pool, task.id()).await;
            assert_eq!(
                tokio::time::timeout(executor.storage_timeout(), queue.recover_expired(1))
                    .await
                    .unwrap_or(Err(TaskProcessingError::TimedOut))
                    .unwrap()
                    .processed_count(),
                1
            );
            make_ready(&db.pool, task.id()).await;
            queue.lose_claim_reply.store(false, Ordering::SeqCst);
            let owned = claim_with_timeout(
                queue.as_ref(),
                executor.storage_timeout(),
                "after_shutdown",
                1,
                Duration::from_secs(120),
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
            assert_eq!(owned.attempt().id(), task.id());
            assert!(
                executor
                    .execute_and_settle(&mut dispatcher(adapter(Ok(()))), owned, Duration::from_secs(1))
                    .await
                    .unwrap()
                    .is_applied()
            );
            assert_worker_state(&db.pool, task.id(), "completed", 2, None).await;
        } else {
            assert_eq!(
                tokio::time::timeout(executor.storage_timeout(), queue.recover_expired(1))
                    .await
                    .unwrap_or(Err(TaskProcessingError::TimedOut))
                    .unwrap()
                    .processed_count(),
                0
            );
            assert!(
                claim_with_timeout(
                    queue.as_ref(),
                    executor.storage_timeout(),
                    "after_shutdown",
                    1,
                    Duration::from_secs(120)
                )
                .await
                .unwrap()
                .is_empty()
            );
        }
        db.close().await;
    }
}

// Model caller cancellation of the durable queue's await; runtime Poller deadline
// enforcement is covered by its private unit tests and supervisor tests.
async fn claim_with_timeout(
    queue: &dyn TaskQueue,
    timeout: Duration,
    owner: &str,
    limit: usize,
    lease: Duration,
) -> Result<Vec<LeasedTask>, TaskProcessingError> {
    tokio::time::timeout(timeout, queue.claim(&format(), owner, limit, lease))
        .await
        .unwrap_or(Err(TaskProcessingError::TimedOut))
}
