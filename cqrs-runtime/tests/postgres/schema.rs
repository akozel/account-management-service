use super::*;

#[tokio::test]
async fn schema_rejects_inconsistent_direct_writes_and_accepts_valid_lifecycle_states() {
    let db = Database::new().await;
    let task = new_task().with_max_attempts(3).unwrap();
    enqueue(&db.pool, vec![task.clone()]).await;
    let original: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
        .bind(task.id())
        .fetch_one(&db.pool)
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let deadline = Utc::now() + chrono::Duration::minutes(1);
    let finished = Utc::now();
    for invalid in [
        json!({"aggregate_sequence": 0}),
        json!({"task_type": ""}),
        json!({"task_version": 0}),
        json!({"payload": []}),
        json!({"metadata": "not-an-object"}),
        json!({"status": "unknown"}),
        json!({"attempts": -1}),
        json!({"max_attempts": 0}),
        json!({"attempts": 4}),
        json!({"attempts": 3}),
        json!({"locked_by": "unexpected-owner"}),
        json!({"lock_token": token}),
        json!({"locked_until": deadline}),
        json!({"finished_at": finished}),
        json!({"status": "processing", "attempts": 1, "lock_token": token, "locked_until": deadline}),
        json!({"status": "processing", "attempts": 1, "locked_by": "owner", "locked_until": deadline}),
        json!({"status": "processing", "attempts": 1, "locked_by": "owner", "lock_token": token}),
        json!({"status": "processing", "attempts": 1, "locked_by": "owner", "lock_token": token, "locked_until": deadline, "finished_at": finished}),
        json!({"status": "completed"}),
        json!({"status": "failed"}),
        json!({"status": "completed", "finished_at": finished, "locked_by": "owner", "lock_token": token, "locked_until": deadline}),
    ] {
        let mut candidate = original.clone();
        candidate
            .as_object_mut()
            .unwrap()
            .extend(invalid.as_object().unwrap().clone());
        candidate["id"] = json!(Uuid::new_v4());
        let error =
            sqlx::query("INSERT INTO transactional_outbox SELECT * FROM jsonb_populate_record(NULL::transactional_outbox, $1)")
                .bind(candidate)
                .execute(&db.pool)
                .await
                .expect_err("invalid durable state must be rejected even when bypassing the Rust API");
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("23514"),
            "{invalid}"
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "rejected direct writes must leave no rows");
    for valid in [
        json!({"status": "pending", "attempts": 1}),
        json!({"status": "processing", "attempts": 1, "locked_by": "owner", "lock_token": token, "locked_until": deadline}),
        json!({"status": "completed", "attempts": 1, "finished_at": finished}),
        json!({"status": "failed", "attempts": 3, "finished_at": finished}),
    ] {
        let mut candidate = original.clone();
        candidate.as_object_mut().unwrap().extend(valid.as_object().unwrap().clone());
        candidate["id"] = json!(Uuid::new_v4());
        sqlx::query("INSERT INTO transactional_outbox SELECT * FROM jsonb_populate_record(NULL::transactional_outbox, $1)")
            .bind(candidate)
            .execute(&db.pool)
            .await
            .unwrap();
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 5);
    db.close().await;
}
