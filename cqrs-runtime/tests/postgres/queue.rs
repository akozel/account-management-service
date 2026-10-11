use super::*;
use cqrs_runtime::task::{
    data::attempt::TaskAttempt,
    queue::{
        error::QueueRequestError,
        lease::TaskLease,
        settlement::{SettlementResult, TaskFailureCause},
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_settlements_apply_exactly_one_transition_for_the_same_token() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let task = new_task().with_metadata(json!({"operation": "same-effect"})).unwrap();
    enqueue(&db.pool, vec![task.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let mut gate = db.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM transactional_outbox WHERE id = $1 FOR UPDATE")
        .bind(task.id())
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let mut settlements = tokio::task::JoinSet::new();
    for request in [
        SettlementRequest::Completed,
        SettlementRequest::Retry {
            delay: Duration::from_secs(300),
            reason: "retry winner".into(),
        },
        SettlementRequest::Failed {
            reason: "failure winner".into(),
        },
    ] {
        let queue = queue.clone();
        let claimed = claimed.clone();
        settlements.spawn(async move { (request.clone(), queue.settle(&claimed, request).await.unwrap()) });
    }
    wait_for_blocked_queries(&db.pool, "UPDATE transactional_outbox", 3).await;
    gate.commit().await.unwrap();
    let mut winner = None;
    let mut lost = 0;
    while let Some(result) = settlements.join_next().await {
        let (request, result) = result.unwrap();
        if result == SettlementResult::OwnershipLost {
            lost += 1;
        } else {
            assert!(winner.replace((request, result)).is_none());
        }
    }
    assert_eq!(lost, 2);
    match winner.unwrap() {
        (SettlementRequest::Completed, SettlementResult::Completed) => {
            assert_worker_state(&db.pool, task.id(), "completed", 1, None).await;
        }
        (SettlementRequest::Retry { .. }, SettlementResult::RetryScheduled) => {
            assert_worker_state(&db.pool, task.id(), "pending", 1, Some("retry winner")).await;
            assert!(claim(&queue).await.is_empty());
        }
        (SettlementRequest::Failed { .. }, SettlementResult::Failed(failure)) => {
            assert_eq!(failure.id, task.id());
            assert_eq!(failure.cause, TaskFailureCause::PermanentRejection);
            assert_worker_state(&db.pool, task.id(), "failed", 1, Some("failure winner")).await;
        }
        other => panic!("request and confirmed outcome disagree: {other:?}"),
    }
    let saved = row(&db.pool, task.id()).await;
    assert_eq!(&saved.get::<Value, _>("payload"), task.payload());
    assert_eq!(&saved.get::<Value, _>("metadata"), task.metadata());
    assert_eq!(
        queue.settle(&claimed, SettlementRequest::Completed).await.unwrap(),
        SettlementResult::OwnershipLost
    );
    db.close().await;
}

#[tokio::test]
async fn settlement_rechecks_lease_expiry_after_waiting_for_a_row_lock() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let mut gate = db.pool.begin().await.unwrap();
    // The settling statement initially sees the old, valid lease, then waits.
    sqlx::query("UPDATE transactional_outbox SET locked_until = now() - interval '1 second' WHERE id = $1")
        .bind(task.id())
        .execute(&mut *gate)
        .await
        .unwrap();
    let settling = tokio::spawn({
        let queue = queue.clone();
        async move { queue.settle(&claimed, SettlementRequest::Completed).await.unwrap() }
    });
    wait_for_blocked_queries(&db.pool, "UPDATE transactional_outbox", 1).await;
    gate.commit().await.unwrap();
    assert_eq!(settling.await.unwrap(), SettlementResult::OwnershipLost);
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    assert_eq!(queue.recover_expired(1).await.unwrap().retried_count(), 1);
    assert_worker_state(&db.pool, task.id(), "pending", 1, Some("worker lease expired")).await;
    db.close().await;
}

#[tokio::test]
async fn a_lease_expiring_while_settlement_waits_on_an_unchanged_row_cannot_be_completed() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    // A short real lease expires while an unchanged row is held by another
    // transaction. PostgreSQL's own clock determines when to release the lock.
    sqlx::query("UPDATE transactional_outbox SET locked_until = clock_timestamp() + interval '1 second' WHERE id = $1")
        .bind(task.id())
        .execute(&db.pool)
        .await
        .unwrap();
    let mut gate = db.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM transactional_outbox WHERE id = $1 FOR UPDATE")
        .bind(task.id())
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let settling = tokio::spawn({
        let queue = queue.clone();
        async move { queue.settle(&claimed, SettlementRequest::Completed).await.unwrap() }
    });
    wait_for_blocked_queries(&db.pool, "UPDATE transactional_outbox", 1).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let expired: bool =
                sqlx::query_scalar("SELECT locked_until <= clock_timestamp() FROM transactional_outbox WHERE id = $1")
                    .bind(task.id())
                    .fetch_one(&db.pool)
                    .await
                    .unwrap();
            if expired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    gate.commit().await.unwrap();
    assert_eq!(settling.await.unwrap(), SettlementResult::OwnershipLost);
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    assert_eq!(queue.recover_expired(1).await.unwrap().retried_count(), 1);
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_recovery_skips_locked_rows_and_reports_each_failed_transition_once() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let tasks: Vec<_> = (0..24)
        .map(|index| new_task().with_max_attempts(if index % 2 == 0 { 1 } else { 3 }).unwrap())
        .collect();
    let exhausted: HashSet<_> = tasks
        .iter()
        .filter(|task| task.max_attempts() == 1)
        .map(NewTask::id)
        .collect();
    enqueue(&db.pool, tasks.clone()).await;
    let deliveries = claim(&queue).await;
    assert_eq!(deliveries.len(), tasks.len());
    sqlx::query("UPDATE transactional_outbox SET locked_until = now() - interval '1 second'")
        .execute(&db.pool)
        .await
        .unwrap();
    let mut gate = db.pool.begin().await.unwrap();
    let locked = tasks[0].id();
    sqlx::query("SELECT id FROM transactional_outbox WHERE id = $1 FOR UPDATE")
        .bind(locked)
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(6));
    let mut recoveries = tokio::task::JoinSet::new();
    for _ in 0..6 {
        let queue = queue.clone();
        let barrier = barrier.clone();
        recoveries.spawn(async move {
            barrier.wait().await;
            queue.recover_expired(4).await.unwrap()
        });
    }
    let mut processed = 0;
    let mut retried = 0;
    let mut failures = HashSet::new();
    while let Some(result) = recoveries.join_next().await {
        let report = result.unwrap();
        assert!(report.processed_count() <= 4);
        processed += report.processed_count();
        retried += report.retried_count();
        for failure in report.failures() {
            assert!(
                failures.insert(failure.id),
                "failure must be reported by only one recovery transaction"
            );
            assert_eq!(failure.attempt, 1);
            assert_eq!(failure.cause, TaskFailureCause::LeaseExpired);
        }
    }
    assert_eq!(processed, 23);
    assert_eq!(retried, 12);
    assert_eq!(failures, exhausted.iter().copied().filter(|id| *id != locked).collect());
    assert_worker_state(&db.pool, locked, "processing", 1, None).await;
    gate.rollback().await.unwrap();
    let final_recovery = queue.recover_expired(4).await.unwrap();
    assert_eq!(final_recovery.processed_count(), 1);
    assert_eq!(final_recovery.failures()[0].id, locked);
    assert_eq!(queue.recover_expired(4).await.unwrap().processed_count(), 0);
    for task in &tasks {
        assert_worker_state(
            &db.pool,
            task.id(),
            if exhausted.contains(&task.id()) { "failed" } else { "pending" },
            1,
            Some("worker lease expired"),
        )
        .await;
        let saved = row(&db.pool, task.id()).await;
        assert_eq!(&saved.get::<Value, _>("payload"), task.payload());
        assert_eq!(&saved.get::<Value, _>("metadata"), task.metadata());
    }
    for delivery in &deliveries {
        assert_eq!(
            queue.settle(delivery, SettlementRequest::Completed).await.unwrap(),
            SettlementResult::OwnershipLost
        );
    }
    db.close().await;
}

#[tokio::test]
async fn a_mid_batch_recovery_error_rolls_back_prior_updates_and_preserves_all_leases() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let retry = new_task();
    let rejected = NewTask::new(DummyTaskV1 {
        value: "reject_recovery".into(),
    })
    .unwrap()
    .with_max_attempts(1)
    .unwrap();
    enqueue(&db.pool, vec![retry.clone(), rejected.clone()]).await;
    let deliveries = claim(&queue).await;
    sqlx::query("UPDATE transactional_outbox SET locked_until = now() - CASE WHEN id = $1 THEN interval '2 seconds' ELSE interval '1 second' END")
        .bind(retry.id()).execute(&db.pool).await.unwrap();
    let mut before = Vec::new();
    for delivery in &deliveries {
        let saved: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
            .bind(delivery.attempt().id())
            .fetch_one(&db.pool)
            .await
            .unwrap();
        before.push((delivery.attempt().id(), saved));
    }
    sqlx::query("ALTER TABLE transactional_outbox ADD CONSTRAINT reject_recovery CHECK (status = 'processing' OR payload->>'value' <> 'reject_recovery') NOT VALID")
        .execute(&db.pool).await.unwrap();
    assert_eq!(queue.recover_expired(2).await.unwrap_err(), TaskProcessingError::Unavailable);
    for (id, expected) in before {
        let saved: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(
            saved, expected,
            "a failed batch must retain its original ownership and schedule"
        );
    }
    sqlx::query("ALTER TABLE transactional_outbox DROP CONSTRAINT reject_recovery")
        .execute(&db.pool)
        .await
        .unwrap();
    let recovered = queue.recover_expired(2).await.unwrap();
    assert_eq!(recovered.retried_count(), 1);
    assert_eq!(recovered.failures().len(), 1);
    assert_eq!(recovered.failures()[0].id, rejected.id());
    assert_worker_state(&db.pool, retry.id(), "pending", 1, Some("worker lease expired")).await;
    assert_worker_state(&db.pool, rejected.id(), "failed", 1, Some("worker lease expired")).await;
    db.close().await;
}

#[cfg(target_pointer_width = "64")]
#[tokio::test]
async fn unrepresentable_requests_cannot_change_queue_state_or_consume_ownership() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    assert_eq!(
        queue
            .claim(&format(), "test", usize::MAX, Duration::from_secs(60))
            .await
            .unwrap_err(),
        TaskProcessingError::InvalidRequest(QueueRequestError::InvalidLimit)
    );
    assert_eq!(
        queue.recover_expired(usize::MAX).await.unwrap_err(),
        TaskProcessingError::InvalidRequest(QueueRequestError::InvalidLimit)
    );
    assert_worker_state(&db.pool, task.id(), "pending", 0, None).await;
    assert!(
        queue
            .claim(&format(), "test", 0, Duration::from_secs(60))
            .await
            .unwrap()
            .is_empty()
    );
    let claimed = claim(&queue).await.pop().unwrap();
    assert_eq!(
        queue
            .settle(
                &claimed,
                SettlementRequest::Retry {
                    delay: Duration::MAX,
                    reason: "invalid delay".into()
                }
            )
            .await
            .unwrap_err(),
        TaskProcessingError::InvalidRequest(QueueRequestError::InvalidRetryDelay)
    );
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    assert_eq!(
        row(&db.pool, task.id()).await.get::<Option<Uuid>, _>("lock_token"),
        Some(claimed.lease().lock_token())
    );
    assert_eq!(
        queue.settle(&claimed, SettlementRequest::Completed).await.unwrap(),
        SettlementResult::Completed
    );
    db.close().await;
}

#[tokio::test]
async fn ready_time_formats_and_skip_locked_distribute_without_overlap() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let tasks: Vec<_> = (0..20).map(|_| new_task()).collect();
    let ids: HashSet<_> = tasks.iter().map(|m| m.id()).collect();
    let delayed = new_task().scheduled(TaskSchedule::After(Duration::from_secs(300))).unwrap();
    let absolute_time = DateTime::from_timestamp_micros((Utc::now() + chrono::Duration::hours(1)).timestamp_micros()).unwrap();
    let absolute = new_task().scheduled(TaskSchedule::At(absolute_time)).unwrap();
    let past = new_task()
        .scheduled(TaskSchedule::At(Utc::now() - chrono::Duration::minutes(1)))
        .unwrap();
    let unknown = NewTask::from_payload(TaskFormat::new(format().task_type(), 3).unwrap(), json!({"value":"runtime"})).unwrap();
    let unknown_type = NewTask::from_payload(TaskFormat::new("future_task", 1).unwrap(), json!({"value":"runtime"})).unwrap();
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
    let saved = row(&db.pool, delayed.id()).await;
    let delta = saved.get::<DateTime<Utc>, _>("available_at") - saved.get::<DateTime<Utc>, _>("created_at");
    assert_eq!(delta.num_seconds(), 300);
    assert_eq!(
        row(&db.pool, absolute.id()).await.get::<DateTime<Utc>, _>("available_at"),
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
    let claimed_ids: HashSet<_> = claimed.iter().map(|m| m.attempt().id()).collect();
    assert_eq!(claimed_ids.len(), 20);
    assert!(claimed_ids.contains(&past.id()));
    assert!(!claimed_ids.contains(&locked_id));
    tx.rollback().await.unwrap();
    assert_eq!(claim(&queue).await[0].attempt().id(), locked_id);
    assert!(claim(&queue).await.is_empty());
    for task in &claimed {
        assert!(queue.settle(task, SettlementRequest::Completed).await.unwrap().is_applied());
        assert!(!queue.settle(task, SettlementRequest::Completed).await.unwrap().is_applied());
    }
    make_ready(&db.pool, delayed.id()).await;
    assert_eq!(claim(&queue).await[0].attempt().id(), delayed.id());
    for id in [unknown.id(), unknown_type.id()] {
        let saved = row(&db.pool, id).await;
        assert_eq!(saved.get::<String, _>("status"), "pending");
        assert_eq!(saved.get::<i32, _>("attempts"), 0);
    }
    assert!(queue.claim(&format(), "test", 1, Duration::ZERO).await.is_err());
    assert!(queue.claim(&format(), "test", 1, Duration::from_nanos(1)).await.is_err());
    assert!(queue.claim(&format(), "test", 1, Duration::MAX).await.is_err());
    db.close().await;
}

#[tokio::test]
async fn retries_failed_tasks_and_expired_leases_preserve_identity_and_fence_old_owners() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let mut msg = new_task();
    msg = msg.with_max_attempts(2).unwrap();
    enqueue(&db.pool, vec![msg.clone()]).await;
    let first = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &first,
                SettlementRequest::Retry {
                    delay: Duration::from_secs(300),
                    reason: "try later".into()
                }
            )
            .await
            .unwrap()
            .is_applied()
    );
    assert!(claim(&queue).await.is_empty());
    assert_eq!(row(&db.pool, msg.id()).await.get::<String, _>("last_error"), "try later");
    make_ready(&db.pool, msg.id()).await;
    let second = claim(&queue).await.pop().unwrap();
    assert_eq!(second.attempt().number(), 2);
    assert_ne!(first.lease().lock_token(), second.lease().lock_token());
    assert!(!queue.settle(&first, SettlementRequest::Completed).await.unwrap().is_applied());
    expire(&db.pool, msg.id()).await;
    assert!(
        !queue
            .settle(&second, SettlementRequest::Completed)
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(queue.recover_expired(10).await.unwrap().processed_count(), 1);
    assert_eq!(row(&db.pool, msg.id()).await.get::<String, _>("status"), "failed");
    assert!(
        row(&db.pool, msg.id())
            .await
            .get::<Option<DateTime<Utc>>, _>("finished_at")
            .is_some()
    );
    sqlx::query(REQUEUE_FAILED_OUTBOX_TASK)
        .bind(msg.id())
        .execute(&db.pool)
        .await
        .unwrap();
    let restarted = claim(&queue).await.pop().unwrap();
    assert_eq!(restarted.attempt().id(), msg.id());
    assert_eq!(restarted.attempt().number(), 1);
    expire(&db.pool, msg.id()).await;
    assert_eq!(queue.recover_expired(10).await.unwrap().processed_count(), 1);
    assert_eq!(queue.recover_expired(10).await.unwrap().processed_count(), 0);
    assert_eq!(row(&db.pool, msg.id()).await.get::<String, _>("status"), "pending");
    assert!(claim(&queue).await.is_empty());
    make_ready(&db.pool, msg.id()).await;
    let last = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &last,
                SettlementRequest::Retry {
                    delay: Duration::ZERO,
                    reason: "failed twice".into()
                }
            )
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(row(&db.pool, msg.id()).await.get::<String, _>("status"), "failed");
    sqlx::query(REQUEUE_FAILED_OUTBOX_TASK)
        .bind(msg.id())
        .execute(&db.pool)
        .await
        .unwrap();
    let permanent = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &permanent,
                SettlementRequest::Failed {
                    reason: "broken payload".into()
                }
            )
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(row(&db.pool, msg.id()).await.get::<i32, _>("attempts"), 1);
    let zero_delay = new_task();
    enqueue(&db.pool, vec![zero_delay.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let settled = queue
        .settle(
            &claimed,
            SettlementRequest::Retry {
                delay: Duration::ZERO,
                reason: "retry".into(),
            },
        )
        .await
        .unwrap();
    assert!(settled.is_applied());
    assert!(settled.failure().is_none());
    assert_eq!(claim(&queue).await[0].attempt().id(), zero_delay.id());
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
        SELECT gen_random_uuid(), 'runtime_test', n::text, 1, 'dummy_task', 1,
            '{}', CASE WHEN n <= 10000 THEN 'completed' ELSE 'pending' END,
            now() + CASE WHEN n > 19990 THEN interval '-1 minute' ELSE interval '1 day' END,
            CASE WHEN n <= 10000 THEN now() ELSE NULL END
        FROM generate_series(1, 20000) AS n;
        INSERT INTO transactional_outbox
            (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version,
             payload, status, attempts, locked_by, lock_token, locked_until)
        SELECT gen_random_uuid(), 'runtime_test', n::text, 1, 'dummy_task', 1,
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
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id FROM transactional_outbox WHERE status = 'pending' AND task_type = 'dummy_task' AND task_version = 1 AND available_at <= statement_timestamp() ORDER BY available_at, id LIMIT 4 FOR UPDATE SKIP LOCKED",
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
    let queue = PostgresTaskQueue::new(db.pool.clone());
    db.pool.close().await;
    assert_eq!(
        queue.claim(&format(), "test", 1, Duration::from_secs(60)).await.unwrap_err(),
        TaskProcessingError::Unavailable
    );
    assert_eq!(queue.recover_expired(1).await.unwrap_err(), TaskProcessingError::Unavailable);
    let dummy = LeasedTask::new(
        TaskAttempt::new(Uuid::new_v4(), format(), json!({}), json!({}), 1).unwrap(),
        TaskLease::new(Uuid::new_v4(), tokio::time::Instant::now() + Duration::from_secs(60)),
    );
    assert_eq!(
        queue.settle(&dummy, SettlementRequest::Completed).await.unwrap_err(),
        TaskProcessingError::Unavailable
    );
    db.close().await;
}
