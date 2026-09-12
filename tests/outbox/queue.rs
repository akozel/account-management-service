use super::*;

#[tokio::test]
async fn ready_time_formats_and_skip_locked_distribute_without_overlap() {
    let db = Database::new().await;
    let queue = PostgresOutboxQueue::new(db.pool.clone());
    let tasks: Vec<_> = (0..20).map(|_| new_task()).collect();
    let ids: HashSet<_> = tasks.iter().map(|m| m.id).collect();
    let delayed = new_task().scheduled(Schedule::After(Duration::from_secs(300)));
    let absolute_time = DateTime::from_timestamp_micros((Utc::now() + chrono::Duration::hours(1)).timestamp_micros()).unwrap();
    let absolute = new_task().scheduled(Schedule::At(absolute_time));
    let past = new_task().scheduled(Schedule::At(Utc::now() - chrono::Duration::minutes(1)));
    let mut unknown = new_task();
    unknown.task_version = 3;
    let mut unknown_type = new_task();
    unknown_type.task_type = "future_task".into();
    enqueue(&db.pool, tasks).await;
    enqueue(
        &db.pool,
        vec![
            delayed.clone(),
            absolute.clone(),
            past.clone(),
            unknown.clone(),
            unknown_type.clone(),
        ],
    )
    .await;
    let saved = row(&db.pool, delayed.id).await;
    let delta = saved.get::<DateTime<Utc>, _>("available_at") - saved.get::<DateTime<Utc>, _>("created_at");
    assert_eq!(delta.num_seconds(), 300);
    assert_eq!(
        row(&db.pool, absolute.id).await.get::<DateTime<Utc>, _>("available_at"),
        absolute_time
    );
    // Hold a row lock independently: another consumer must still make progress.
    let locked_id = *ids.iter().next().unwrap();
    let mut tx = db.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM transactional_outbox WHERE id = $1 FOR UPDATE")
        .bind(locked_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let supported_format = format();
    let (left, right) = tokio::join!(
        queue.claim(&supported_format, "left", 10, Duration::from_secs(60)),
        queue.claim(&supported_format, "right", 10, Duration::from_secs(60))
    );
    let claimed: Vec<_> = left.unwrap().into_iter().chain(right.unwrap()).collect();
    assert_eq!(claimed.len(), 20);
    let claimed_ids: HashSet<_> = claimed.iter().map(|m| m.id).collect();
    assert_eq!(claimed_ids.len(), 20);
    assert!(claimed_ids.contains(&past.id));
    assert!(!claimed_ids.contains(&locked_id));
    tx.rollback().await.unwrap();
    assert_eq!(claim(&queue).await[0].id, locked_id);
    assert!(claim(&queue).await.is_empty());
    for task in &claimed {
        assert!(queue.settle(task, TaskOutcome::Completed).await.unwrap().is_settled());
        assert!(!queue.settle(task, TaskOutcome::Completed).await.unwrap().is_settled());
    }
    make_ready(&db.pool, delayed.id).await;
    assert_eq!(claim(&queue).await[0].id, delayed.id);
    for id in [unknown.id, unknown_type.id] {
        let saved = row(&db.pool, id).await;
        assert_eq!(saved.get::<String, _>("status"), "pending");
        assert_eq!(saved.get::<i32, _>("attempts"), 0);
    }
    assert!(queue.claim(&format(), "test", 1, Duration::ZERO).await.is_err());
    assert!(queue.claim(&format(), "test", 1, Duration::MAX).await.is_err());
    db.close().await;
}

#[tokio::test]
async fn retries_failed_tasks_and_expired_leases_preserve_identity_and_fence_old_owners() {
    let db = Database::new().await;
    let queue = PostgresOutboxQueue::new(db.pool.clone());
    let mut msg = new_task();
    msg.max_attempts = 2;
    enqueue(&db.pool, vec![msg.clone()]).await;
    let first = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &first,
                TaskOutcome::Retry {
                    delay: Duration::from_secs(300),
                    reason: "try later".into()
                }
            )
            .await
            .unwrap()
            .is_settled()
    );
    assert!(claim(&queue).await.is_empty());
    assert_eq!(row(&db.pool, msg.id).await.get::<String, _>("last_error"), "try later");
    make_ready(&db.pool, msg.id).await;
    let second = claim(&queue).await.pop().unwrap();
    assert_eq!(second.attempt, 2);
    assert_ne!(first.lock_token, second.lock_token);
    assert!(!queue.settle(&first, TaskOutcome::Completed).await.unwrap().is_settled());
    expire(&db.pool, msg.id).await;
    assert!(!queue.settle(&second, TaskOutcome::Completed).await.unwrap().is_settled());
    assert_eq!(queue.recover_expired(10).await.unwrap().recovered, 1);
    assert_eq!(row(&db.pool, msg.id).await.get::<String, _>("status"), "failed");
    assert!(
        row(&db.pool, msg.id)
            .await
            .get::<Option<DateTime<Utc>>, _>("finished_at")
            .is_some()
    );
    sqlx::query(include_str!("../../deploy/postgres/requeue-failed-outbox.sql"))
        .bind(msg.id)
        .execute(&db.pool)
        .await
        .unwrap();
    let restarted = claim(&queue).await.pop().unwrap();
    assert_eq!(restarted.id, msg.id);
    assert_eq!(restarted.attempt, 1);
    expire(&db.pool, msg.id).await;
    assert_eq!(queue.recover_expired(10).await.unwrap().recovered, 1);
    assert_eq!(queue.recover_expired(10).await.unwrap().recovered, 0);
    assert_eq!(row(&db.pool, msg.id).await.get::<String, _>("status"), "pending");
    assert!(claim(&queue).await.is_empty());
    make_ready(&db.pool, msg.id).await;
    let last = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &last,
                TaskOutcome::Retry {
                    delay: Duration::ZERO,
                    reason: "failed twice".into()
                }
            )
            .await
            .unwrap()
            .is_settled()
    );
    assert_eq!(row(&db.pool, msg.id).await.get::<String, _>("status"), "failed");
    sqlx::query(include_str!("../../deploy/postgres/requeue-failed-outbox.sql"))
        .bind(msg.id)
        .execute(&db.pool)
        .await
        .unwrap();
    let permanent = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &permanent,
                TaskOutcome::Failed {
                    reason: "broken payload".into()
                }
            )
            .await
            .unwrap()
            .is_settled()
    );
    assert_eq!(row(&db.pool, msg.id).await.get::<i32, _>("attempts"), 1);
    let zero_delay = new_task();
    enqueue(&db.pool, vec![zero_delay.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let settled = queue
        .settle(
            &claimed,
            TaskOutcome::Retry {
                delay: Duration::ZERO,
                reason: "retry".into(),
            },
        )
        .await
        .unwrap();
    assert!(settled.is_settled());
    assert!(settled.failed().is_none());
    assert_eq!(claim(&queue).await[0].id, zero_delay.id);
    db.close().await;
}

#[tokio::test]
async fn queue_indexes_serve_ready_and_expired_queries_with_finished_history() {
    let db = Database::new().await;
    // Keep most history terminal, with a large future queue and a few due rows.
    sqlx::raw_sql(
        r#"
        INSERT INTO transactional_outbox
            (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version,
             payload, status, available_at, finished_at)
        SELECT gen_random_uuid(), 'email_reservation', n::text, 1, 'send_verification_code', 1,
            '{}', CASE WHEN n <= 10000 THEN 'completed' ELSE 'pending' END,
            now() + CASE WHEN n > 19990 THEN interval '-1 minute' ELSE interval '1 day' END,
            CASE WHEN n <= 10000 THEN now() ELSE NULL END
        FROM generate_series(1, 20000) AS n;
        INSERT INTO transactional_outbox
            (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version,
             payload, status, attempts, locked_by, lock_token, locked_until)
        SELECT gen_random_uuid(), 'email_reservation', n::text, 1, 'send_verification_code', 1,
            '{}', 'processing', 1, 'expired-worker', gen_random_uuid(),
            now() + CASE WHEN n > 1990 THEN interval '-1 minute' ELSE interval '1 day' END
        FROM generate_series(1, 2000) AS n;
        ANALYZE transactional_outbox;
    "#,
    )
    .execute(&db.pool)
    .await
    .unwrap();
    for (query, index) in [
        (
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id FROM transactional_outbox WHERE status = 'pending' AND task_type = 'send_verification_code' AND task_version = 1 AND available_at <= statement_timestamp() ORDER BY available_at, id LIMIT 4 FOR UPDATE SKIP LOCKED",
            "transactional_outbox_ready_idx",
        ),
        (
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id, attempts FROM transactional_outbox WHERE status = 'processing' AND locked_until <= statement_timestamp() ORDER BY locked_until, id LIMIT 4 FOR UPDATE SKIP LOCKED",
            "transactional_outbox_expired_idx",
        ),
    ] {
        let plan: Value = sqlx::query_scalar(query).fetch_one(&db.pool).await.unwrap();
        let text = plan.to_string();
        assert!(text.contains(index), "{text}");
        assert!(!text.contains("Seq Scan"), "{text}");
    }
    let queue = PostgresOutboxQueue::new(db.pool.clone());
    db.pool.close().await;
    assert_eq!(
        queue.claim(&format(), "test", 1, Duration::from_secs(60)).await.unwrap_err(),
        OutboxError::Unavailable
    );
    assert_eq!(queue.recover_expired(1).await.unwrap_err(), OutboxError::Unavailable);
    let dummy = ClaimedTask {
        id: Uuid::new_v4(),
        format: format(),
        payload: json!({}),
        metadata: json!({}),
        attempt: 1,
        lock_token: Uuid::new_v4(),
    };
    assert_eq!(
        queue.settle(&dummy, TaskOutcome::Completed).await.unwrap_err(),
        OutboxError::Unavailable
    );
    db.close().await;
}
