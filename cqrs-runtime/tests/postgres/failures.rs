use super::*;
use cqrs_runtime::task::queue::settlement::{SettlementResult, TaskFailureCause};

#[tokio::test]
async fn deferred_commit_failures_roll_back_claim_settlement_and_recovery() {
    let db = Database::new().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(db.pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    sqlx::query("SET statement_timeout = '10s'").execute(&pool).await.unwrap();
    let queue = PostgresTaskQueue::new(pool.clone());
    let task = new_task().with_max_attempts(1).unwrap();
    enqueue(&db.pool, vec![task.clone()]).await;
    sqlx::raw_sql(
        "CREATE FUNCTION reject_queue_commit() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'injected deferred commit failure'; END $$;",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let mut owned = None;
    for operation in ["claim", "settle", "recovery"] {
        if operation == "settle" {
            owned = Some(claim(&queue).await.pop().unwrap());
        }
        if operation == "recovery" {
            expire(&db.pool, task.id()).await;
        }
        let before: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
            .bind(task.id())
            .fetch_one(&db.pool)
            .await
            .unwrap();
        // The UPDATE succeeds; PostgreSQL rejects the deferred constraint when
        // COMMIT runs, exercising a different path from statement failure.
        sqlx::raw_sql(
            "CREATE CONSTRAINT TRIGGER reject_queue_commit AFTER UPDATE ON transactional_outbox \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_queue_commit();",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        let result = match operation {
            "claim" => queue.claim(&format(), "test", 1, Duration::from_secs(60)).await.map(|_| ()),
            "settle" => queue
                .settle(owned.as_ref().unwrap(), SettlementRequest::Completed)
                .await
                .map(|_| ()),
            _ => queue.recover_expired(1).await.map(|_| ()),
        };
        assert_eq!(result, Err(TaskProcessingError::Unavailable), "{operation}");
        let after: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
            .bind(task.id())
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(after, before, "{operation} must not publish an uncommitted transition");
        let timeout: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&pool).await.unwrap();
        assert_eq!(timeout, "10s", "commit failure must restore transaction-local settings");
        sqlx::query("DROP TRIGGER reject_queue_commit ON transactional_outbox")
            .execute(&db.pool)
            .await
            .unwrap();
    }
    let recovered = queue.recover_expired(1).await.unwrap();
    assert_eq!(recovered.processed_count(), 1);
    assert_eq!(recovered.failures().len(), 1);
    assert_eq!(recovered.failures()[0].id, task.id());
    assert_eq!(recovered.failures()[0].cause, TaskFailureCause::LeaseExpired);
    assert_worker_state(&db.pool, task.id(), "failed", 1, Some("worker lease expired")).await;
    pool.close().await;
    db.close().await;
}

#[tokio::test]
async fn failure_to_install_transaction_settings_changes_no_tasks_and_allows_later_use() {
    let db = Database::new().await;
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    // A real restricted role can begin transactions and access the queue, but
    // cannot execute the function used to install its statement timeout.
    sqlx::raw_sql(
        "CREATE ROLE queue_test LOGIN PASSWORD 'queue_test'; \
         GRANT USAGE ON SCHEMA public TO queue_test; \
         GRANT SELECT, UPDATE ON transactional_outbox TO queue_test; \
         REVOKE EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) FROM PUBLIC;",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let options = db
        .pool
        .connect_options()
        .as_ref()
        .clone()
        .username("queue_test")
        .password("queue_test");
    let pool = PgPoolOptions::new().max_connections(1).connect_with(options).await.unwrap();
    let queue = PostgresTaskQueue::new(pool.clone());
    assert_eq!(
        queue
            .claim(&format(), "restricted", 1, Duration::from_secs(60))
            .await
            .unwrap_err(),
        TaskProcessingError::Unavailable
    );
    assert_eq!(queue.recover_expired(1).await.unwrap_err(), TaskProcessingError::Unavailable);
    assert_worker_state(&db.pool, task.id(), "pending", 0, None).await;
    let admin = PostgresTaskQueue::new(db.pool.clone());
    let owned = claim(&admin).await.pop().unwrap();
    assert_eq!(
        queue.settle(&owned, SettlementRequest::Completed).await.unwrap_err(),
        TaskProcessingError::Unavailable
    );
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    assert_eq!(
        row(&db.pool, task.id()).await.get::<Option<Uuid>, _>("lock_token"),
        Some(owned.lease().lock_token())
    );
    sqlx::query("GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) TO queue_test")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        queue.settle(&owned, SettlementRequest::Completed).await.unwrap(),
        SettlementResult::Completed
    );
    assert_worker_state(&db.pool, task.id(), "completed", 1, None).await;
    pool.close().await;
    db.close().await;
}
