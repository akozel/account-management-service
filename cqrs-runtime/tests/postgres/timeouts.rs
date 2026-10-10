use super::*;
use cqrs_runtime::outbox::ClaimLease;
use cqrs_runtime::{
    outbox::{OutboxTaskCoordinatorConfig, outbox_task_coordinator_with_config},
    postgres::PostgresOutboxQueueConfig,
};

struct LostReplyQueue {
    inner: PostgresOutboxQueue,
    lose_claim_reply: std::sync::atomic::AtomicBool,
    lose_settle_reply: std::sync::atomic::AtomicBool,
    delayed_format: Option<TaskFormat>,
}

#[async_trait]
impl OutboxQueue for LostReplyQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<ClaimedTask>, OutboxError> {
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
        task: &ClaimedTask,
        outcome: SettlementRequest,
    ) -> Result<cqrs_runtime::outbox::SettlementResult, OutboxError> {
        let result = self.inner.settle(task, outcome).await?;
        if self.lose_settle_reply.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok(result)
    }

    async fn recover_expired(&self, limit: usize) -> Result<cqrs_runtime::outbox::LeaseRecoveryReport, OutboxError> {
        self.inner.recover_expired(limit).await
    }
}

#[tokio::test]
async fn deadlines_after_committed_claim_or_settlement_preserve_durable_outcomes() {
    let db = Database::new().await;
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let queue = Arc::new(LostReplyQueue {
        inner: PostgresOutboxQueue::new(db.pool.clone()),
        lose_claim_reply: std::sync::atomic::AtomicBool::new(true),
        lose_settle_reply: std::sync::atomic::AtomicBool::new(true),
        delayed_format: None,
    });
    let coordinator = outbox_task_coordinator_with_config(
        queue.clone(),
        vec![format()],
        OutboxTaskCoordinatorConfig::new(Duration::from_millis(200)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        coordinator.claim("test", 1, Duration::from_secs(60)).await.unwrap_err(),
        OutboxError::TimedOut
    );
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    expire(&db.pool, task.id()).await;
    assert_eq!(coordinator.recover_expired(1).await.unwrap().processed_count(), 1);
    make_ready(&db.pool, task.id()).await;
    queue.lose_claim_reply.store(false, Ordering::SeqCst);
    let owned = coordinator
        .claim("test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(owned.delivery().id(), task.id());
    assert_eq!(
        coordinator
            .execute_and_settle(&mut handler(Ok(())), owned, Duration::from_secs(1))
            .await,
        Err(OutboxError::TimedOut)
    );
    assert_worker_state(&db.pool, task.id(), "completed", 2, None).await;
    assert_eq!(coordinator.recover_expired(1).await.unwrap().processed_count(), 0);
    assert!(
        coordinator
            .claim("test", 1, Duration::from_secs(60))
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
        inner: PostgresOutboxQueue::new(db.pool.clone()),
        lose_claim_reply: std::sync::atomic::AtomicBool::new(false),
        lose_settle_reply: std::sync::atomic::AtomicBool::new(false),
        delayed_format: Some(delayed_format.clone()),
    });
    let coordinator = outbox_task_coordinator_with_config(
        queue.clone(),
        vec![format(), delayed_format],
        OutboxTaskCoordinatorConfig::new(Duration::from_millis(300)).unwrap(),
    )
    .unwrap();
    let old = coordinator
        .claim("test", 2, Duration::from_millis(100))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(old.lease().deadline() <= tokio::time::Instant::now());
    let calls = Arc::new(AtomicUsize::new(0));
    assert_eq!(
        coordinator
            .execute_and_settle(&mut CountingHandler(calls.clone()), old.clone(), Duration::from_secs(1))
            .await,
        Err(OutboxError::LeaseBudgetExhausted)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    // Force server expiry as well: the conservative local deadline may precede it.
    expire(&db.pool, task.id()).await;
    assert_eq!(coordinator.recover_expired(1).await.unwrap().processed_count(), 1);
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
        coordinator
            .execute_and_settle(&mut CountingHandler(calls.clone()), fresh, Duration::from_secs(1))
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
    let queue = Arc::new(PostgresOutboxQueue::new(db.pool.clone()));
    let coordinator = outbox_task_coordinator_with_config(
        queue.clone(),
        vec![format()],
        OutboxTaskCoordinatorConfig::new(Duration::from_millis(300)).unwrap(),
    )
    .unwrap();
    let mut old = coordinator
        .claim("test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    // Model dispatch delay using a shorter conservative local budget; the DB
    // ownership remains live, so the original attempt can safely request retry.
    old = ClaimedTask::new(
        old.delivery().clone(),
        ClaimLease::new(
            old.lease().lock_token(),
            tokio::time::Instant::now() + Duration::from_secs(10),
        ),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        coordinator
            .execute_and_settle(&mut CountingHandler(calls.clone()), old.clone(), Duration::from_secs(30))
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
    let fresh = coordinator
        .claim("test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(fresh.delivery().id(), old.delivery().id());
    assert_eq!(fresh.delivery().payload(), old.delivery().payload());
    assert_ne!(fresh.lease().lock_token(), old.lease().lock_token());
    assert!(
        coordinator
            .execute_and_settle(&mut CountingHandler(calls.clone()), fresh, Duration::from_secs(30))
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
    let queue = PostgresOutboxQueue::with_config(
        pool.clone(),
        PostgresOutboxQueueConfig::new(Duration::from_millis(100)).unwrap(),
    )
    .unwrap();

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
        assert_eq!(result, Err(OutboxError::Unavailable));
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
    let queue = PostgresOutboxQueue::with_config(
        db.pool.clone(),
        PostgresOutboxQueueConfig::new(Duration::from_millis(100)).unwrap(),
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        queue.claim(&format(), "test", 1, Duration::from_secs(60)),
    )
    .await
    .expect("the server must cancel slow execution before the test deadline");
    assert_eq!(result.unwrap_err(), OutboxError::Unavailable);
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
    let queue = Arc::new(PostgresOutboxQueue::new(pool.clone()));
    let coordinator = outbox_task_coordinator_with_config(
        queue,
        vec![format()],
        OutboxTaskCoordinatorConfig::new(Duration::from_millis(200)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        coordinator.claim("test", 1, Duration::from_secs(60)).await.unwrap_err(),
        OutboxError::TimedOut
    );
    assert_eq!(coordinator.recover_expired(1).await.unwrap_err(), OutboxError::TimedOut);
    drop(held_connection);
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let owned = coordinator
        .claim("test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let held_connection = pool.acquire().await.unwrap();
    assert_eq!(
        coordinator
            .execute_and_settle(&mut handler(Ok(())), owned.clone(), Duration::from_secs(1))
            .await,
        Err(OutboxError::TimedOut)
    );
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    drop(held_connection);
    assert!(
        coordinator
            .execute_and_settle(&mut handler(Ok(())), owned, Duration::from_secs(1))
            .await
            .unwrap()
            .is_applied()
    );
    assert_worker_state(&db.pool, task.id(), "completed", 1, None).await;
    pool.close().await;
    db.close().await;
}

#[tokio::test]
async fn shutdown_after_committed_claim_or_settlement_preserves_durable_outcomes() {
    for stage in ["claim", "settle"] {
        let db = Database::new().await;
        let task = new_task();
        enqueue(&db.pool, vec![task.clone()]).await;
        let queue = Arc::new(LostReplyQueue {
            inner: PostgresOutboxQueue::new(db.pool.clone()),
            lose_claim_reply: std::sync::atomic::AtomicBool::new(stage == "claim"),
            lose_settle_reply: std::sync::atomic::AtomicBool::new(stage == "settle"),
            delayed_format: None,
        });
        let coordinator = outbox_task_coordinator_with_config(
            queue.clone(),
            vec![format()],
            OutboxTaskCoordinatorConfig::new(Duration::from_millis(300)).unwrap(),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let factory: HandlerRegistryFactory = Arc::new({
            let calls = calls.clone();
            move || TaskHandlerRegistry::try_new(vec![Box::new(CountingHandler(calls.clone()))])
        });
        let shutdown = CancellationToken::new();
        let worker = tokio::spawn(outbox_worker_pool::run(
            coordinator.clone(),
            factory,
            OutboxWorkerPoolConfig::new(
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
            assert_eq!(coordinator.recover_expired(1).await.unwrap().processed_count(), 1);
            make_ready(&db.pool, task.id()).await;
            queue.lose_claim_reply.store(false, Ordering::SeqCst);
            let owned = coordinator
                .claim("after_shutdown", 1, Duration::from_secs(120))
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(owned.delivery().id(), task.id());
            assert!(
                coordinator
                    .execute_and_settle(&mut handler(Ok(())), owned, Duration::from_secs(1))
                    .await
                    .unwrap()
                    .is_applied()
            );
            assert_worker_state(&db.pool, task.id(), "completed", 2, None).await;
        } else {
            assert_eq!(coordinator.recover_expired(1).await.unwrap().processed_count(), 0);
            assert!(
                coordinator
                    .claim("after_shutdown", 1, Duration::from_secs(120))
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        db.close().await;
    }
}
