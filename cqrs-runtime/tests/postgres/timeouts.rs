use super::*;
use cqrs_runtime::{
    outbox::{OutboxTaskCoordinatorConfig, outbox_task_coordinator_with_config},
    postgres::PostgresOutboxQueueConfig,
};

struct LostReplyQueue {
    inner: PostgresOutboxQueue,
    lose_claim_reply: std::sync::atomic::AtomicBool,
    lose_settle_reply: std::sync::atomic::AtomicBool,
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
        let result = self.inner.claim(format, owner, limit, lease).await?;
        if self.lose_claim_reply.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok(result)
    }

    async fn settle(&self, task: &ClaimedTask, outcome: TaskOutcome) -> Result<cqrs_runtime::outbox::SettleResult, OutboxError> {
        let result = self.inner.settle(task, outcome).await?;
        if self.lose_settle_reply.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok(result)
    }

    async fn recover_expired(&self, limit: usize) -> Result<cqrs_runtime::outbox::RecoveryResult, OutboxError> {
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
    });
    let coordinator = outbox_task_coordinator_with_config(
        queue.clone(),
        vec![format()],
        OutboxTaskCoordinatorConfig {
            storage_timeout: Duration::from_millis(200),
        },
    )
    .unwrap();
    assert_eq!(
        coordinator.claim("test", 1, Duration::from_secs(60)).await.unwrap_err(),
        OutboxError::TimedOut
    );
    assert_worker_state(&db.pool, task.id, "processing", 1, None).await;
    expire(&db.pool, task.id).await;
    assert_eq!(coordinator.recover_expired(1).await.unwrap().recovered, 1);
    make_ready(&db.pool, task.id).await;
    queue.lose_claim_reply.store(false, Ordering::SeqCst);
    let owned = coordinator
        .claim("test", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(owned.id, task.id);
    assert_eq!(
        coordinator
            .execute_and_settle(&mut handler(Ok(())), owned, Duration::from_secs(1))
            .await,
        Err(OutboxError::TimedOut)
    );
    assert_worker_state(&db.pool, task.id, "completed", 2, None).await;
    assert_eq!(coordinator.recover_expired(1).await.unwrap().recovered, 0);
    assert!(
        coordinator
            .claim("test", 1, Duration::from_secs(60))
            .await
            .unwrap()
            .is_empty()
    );
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
        PostgresOutboxQueueConfig {
            statement_timeout: Duration::from_millis(100),
        },
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
            expire(&db.pool, processing.id).await;
        }
        let mut blocker = db.pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE transactional_outbox IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            match operation {
                "claim" => queue.claim(&format(), "test", 1, Duration::from_secs(60)).await.map(|_| ()),
                "settle" => queue.settle(&owned, TaskOutcome::Completed).await.map(|_| ()),
                _ => queue.recover_expired(1).await.map(|_| ()),
            }
        })
        .await
        .expect("PostgreSQL must abort the statement before the outer test deadline");
        assert_eq!(result, Err(OutboxError::Unavailable));
        blocker.rollback().await.unwrap();
        assert_session_timeout(&pool).await;
        assert_worker_state(&db.pool, pending.id, "pending", 0, None).await;
        assert_worker_state(&db.pool, processing.id, "processing", 1, None).await;
    }

    assert_eq!(queue.recover_expired(1).await.unwrap().recovered, 1);
    assert_session_timeout(&pool).await;
    make_ready(&db.pool, processing.id).await;
    let tasks = queue.claim(&format(), "test", 2, Duration::from_secs(60)).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert!(!queue.settle(&owned, TaskOutcome::Completed).await.unwrap().is_settled());
    for task in tasks {
        assert!(queue.settle(&task, TaskOutcome::Completed).await.unwrap().is_settled());
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
        PostgresOutboxQueueConfig {
            statement_timeout: Duration::from_millis(100),
        },
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        queue.claim(&format(), "test", 1, Duration::from_secs(60)),
    )
    .await
    .expect("the server must cancel slow execution before the test deadline");
    assert_eq!(result.unwrap_err(), OutboxError::Unavailable);
    assert_worker_state(&db.pool, task.id, "pending", 0, None).await;
    sqlx::raw_sql("DROP TRIGGER slow_outbox ON transactional_outbox; DROP FUNCTION slow_outbox_update();")
        .execute(&db.pool)
        .await
        .unwrap();
    let owned = claim(&queue).await.pop().unwrap();
    assert!(queue.settle(&owned, TaskOutcome::Completed).await.unwrap().is_settled());
    assert_worker_state(&db.pool, task.id, "completed", 1, None).await;
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
        OutboxTaskCoordinatorConfig {
            storage_timeout: Duration::from_millis(200),
        },
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
    assert_worker_state(&db.pool, task.id, "processing", 1, None).await;
    drop(held_connection);
    assert!(
        coordinator
            .execute_and_settle(&mut handler(Ok(())), owned, Duration::from_secs(1))
            .await
            .unwrap()
            .is_settled()
    );
    assert_worker_state(&db.pool, task.id, "completed", 1, None).await;
    pool.close().await;
    db.close().await;
}
