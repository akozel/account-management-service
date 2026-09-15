use super::*;

#[tokio::test]
async fn worker_test_aggregate_covers_success_retry_exhaustion_and_permanent_failure() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresOutboxQueue::new(db.pool.clone()));
    let service = outbox_task_coordinator(queue.clone(), vec![format()]);

    let succeeded = new_task();
    enqueue(&db.pool, vec![succeeded.clone()]).await;
    assert_worker_state(&db.pool, succeeded.id, "pending", 0, None).await;
    let first = claim(&queue).await.pop().unwrap();
    assert_eq!(first.id, succeeded.id);
    assert_eq!(first.attempt, 1);
    assert_worker_state(&db.pool, succeeded.id, "processing", 1, None).await;
    assert!(
        service
            .execute_and_settle(&mut handler(Ok(())), first.clone(), Duration::from_secs(1))
            .await
            .unwrap()
            .is_settled()
    );
    assert_worker_state(&db.pool, succeeded.id, "completed", 1, None).await;
    assert!(!queue.settle(&first, TaskOutcome::Completed).await.unwrap().is_settled());
    assert_eq!(
        sqlx::query(include_str!("../../deploy/postgres/requeue-failed-outbox.sql"))
            .bind(succeeded.id)
            .execute(&db.pool)
            .await
            .unwrap()
            .rows_affected(),
        0
    );
    assert!(claim(&queue).await.is_empty());

    let mut transient = new_task();
    transient.max_attempts = 3;
    enqueue(&db.pool, vec![transient.clone()]).await;
    let first = claim(&queue).await.pop().unwrap();
    assert!(
        service
            .execute_and_settle(
                &mut handler(Err(TaskExecutionError::Retry {
                    reason: "remote busy".into(),
                    retry_after: Some(Duration::from_secs(300)),
                })),
                first.clone(),
                Duration::from_secs(1),
            )
            .await
            .unwrap()
            .is_settled()
    );
    assert_worker_state(&db.pool, transient.id, "pending", 1, Some("remote busy")).await;
    let available_at = row(&db.pool, transient.id).await.get::<DateTime<Utc>, _>("available_at");
    assert!(available_at > Utc::now() + chrono::Duration::seconds(299));
    assert!(claim(&queue).await.is_empty());
    make_ready(&db.pool, transient.id).await;
    let second = claim(&queue).await.pop().unwrap();
    assert_eq!(second.id, transient.id);
    assert_eq!(second.attempt, 2);
    assert_ne!(second.lock_token, first.lock_token);
    assert!(
        !queue
            .settle(&first, TaskOutcome::Failed { reason: "stale".into() })
            .await
            .unwrap()
            .is_settled()
    );
    assert_worker_state(&db.pool, transient.id, "processing", 2, Some("remote busy")).await;
    assert!(
        service
            .execute_and_settle(&mut handler(Ok(())), second, Duration::from_secs(1))
            .await
            .unwrap()
            .is_settled()
    );
    assert_worker_state(&db.pool, transient.id, "completed", 2, None).await;

    let mut exhausted = new_task();
    exhausted.max_attempts = 1;
    enqueue(&db.pool, vec![exhausted.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let exhausted_result = service
        .execute_and_settle(
            &mut handler(Err(TaskExecutionError::Retry {
                reason: "still unavailable".into(),
                retry_after: Some(Duration::ZERO),
            })),
            claimed,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(exhausted_result.failed().unwrap().id, exhausted.id);
    assert_eq!(exhausted_result.failed().unwrap().format, format());
    assert_eq!(exhausted_result.failed().unwrap().attempt, 1);
    assert_eq!(exhausted_result.failed().unwrap().reason, "retry limit reached");
    assert_worker_state(&db.pool, exhausted.id, "failed", 1, Some("still unavailable")).await;

    let mut permanent = new_task();
    permanent.max_attempts = 5;
    enqueue(&db.pool, vec![permanent.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let permanent_result = service
        .execute_and_settle(
            &mut handler(Err(TaskExecutionError::Permanent {
                reason: "invalid address".into(),
            })),
            claimed,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(permanent_result.failed().unwrap().id, permanent.id);
    assert_eq!(permanent_result.failed().unwrap().reason, "permanent failure");
    assert_worker_state(&db.pool, permanent.id, "failed", 1, Some("invalid address")).await;
    assert!(claim(&queue).await.is_empty());
    assert_eq!(queue.recover_expired(10).await.unwrap().recovered, 0);
    db.close().await;
}

#[tokio::test]
async fn worker_test_aggregate_covers_lease_recovery_and_fenced_settlement() {
    let db = Database::new().await;
    let queue = PostgresOutboxQueue::new(db.pool.clone());
    let mut task = new_task();
    task.max_attempts = 2;
    enqueue(&db.pool, vec![task.clone()]).await;

    let first = claim(&queue).await.pop().unwrap();
    expire(&db.pool, task.id).await;
    assert!(!queue.settle(&first, TaskOutcome::Completed).await.unwrap().is_settled());
    assert_worker_state(&db.pool, task.id, "processing", 1, None).await;
    let first_recovery = queue.recover_expired(1).await.unwrap();
    assert_eq!(first_recovery.recovered, 1);
    assert!(first_recovery.failed.is_empty());
    assert_worker_state(&db.pool, task.id, "pending", 1, Some("worker lease expired")).await;
    assert!(claim(&queue).await.is_empty());

    make_ready(&db.pool, task.id).await;
    let second = claim(&queue).await.pop().unwrap();
    assert_eq!(second.attempt, 2);
    assert_ne!(first.lock_token, second.lock_token);
    assert!(!queue.settle(&first, TaskOutcome::Completed).await.unwrap().is_settled());
    assert_worker_state(&db.pool, task.id, "processing", 2, Some("worker lease expired")).await;
    expire(&db.pool, task.id).await;
    let exhausted_recovery = queue.recover_expired(1).await.unwrap();
    assert_eq!(exhausted_recovery.recovered, 1);
    assert_eq!(exhausted_recovery.failed.len(), 1);
    assert_eq!(exhausted_recovery.failed[0].id, task.id);
    assert_eq!(exhausted_recovery.failed[0].format, format());
    assert_eq!(exhausted_recovery.failed[0].attempt, 2);
    assert_eq!(exhausted_recovery.failed[0].reason, "worker lease expired");
    assert_worker_state(&db.pool, task.id, "failed", 2, Some("worker lease expired")).await;
    assert!(!queue.settle(&second, TaskOutcome::Completed).await.unwrap().is_settled());
    assert!(claim(&queue).await.is_empty());
    assert_eq!(queue.recover_expired(1).await.unwrap().recovered, 0);
    db.close().await;
}

#[tokio::test]
async fn worker_test_aggregate_retries_timeouts_then_fails_at_attempt_limit() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresOutboxQueue::new(db.pool.clone()));
    let service = outbox_task_coordinator(queue.clone(), vec![format()]);
    let mut task = new_task();
    task.max_attempts = 2;
    enqueue(&db.pool, vec![task.clone()]).await;

    let first = claim(&queue).await.pop().unwrap();
    assert!(
        service
            .execute_and_settle(&mut HangingHandler, first, Duration::from_millis(5))
            .await
            .unwrap()
            .is_settled()
    );
    assert_worker_state(&db.pool, task.id, "pending", 1, Some("handler timed out")).await;
    assert!(row(&db.pool, task.id).await.get::<DateTime<Utc>, _>("available_at") > Utc::now());
    assert!(claim(&queue).await.is_empty());

    make_ready(&db.pool, task.id).await;
    let second = claim(&queue).await.pop().unwrap();
    assert!(
        service
            .execute_and_settle(&mut HangingHandler, second, Duration::from_millis(5))
            .await
            .unwrap()
            .is_settled()
    );
    assert_worker_state(&db.pool, task.id, "failed", 2, Some("handler timed out")).await;
    assert!(claim(&queue).await.is_empty());
    db.close().await;
}

#[tokio::test]
async fn worker_test_aggregate_redelivers_after_success_without_acknowledgement() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresOutboxQueue::new(db.pool.clone()));
    let service = outbox_task_coordinator(queue.clone(), vec![format()]);
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let deliveries = Arc::new(AtomicUsize::new(0));
    let mut handler = CountingHandler(deliveries.clone());

    let first = claim(&queue).await.pop().unwrap();
    handler.handle(&first).await.unwrap();
    // The process stops after the side effect, before acknowledging ownership.
    assert_worker_state(&db.pool, task.id, "processing", 1, None).await;
    expire(&db.pool, task.id).await;
    assert_eq!(queue.recover_expired(1).await.unwrap().recovered, 1);
    assert_worker_state(&db.pool, task.id, "pending", 1, Some("worker lease expired")).await;
    make_ready(&db.pool, task.id).await;

    let second = claim(&queue).await.pop().unwrap();
    assert_ne!(first.lock_token, second.lock_token);
    assert!(!queue.settle(&first, TaskOutcome::Completed).await.unwrap().is_settled());
    assert!(
        service
            .execute_and_settle(&mut handler, second, Duration::from_secs(1))
            .await
            .unwrap()
            .is_settled()
    );
    assert_eq!(deliveries.load(Ordering::SeqCst), 2);
    assert_worker_state(&db.pool, task.id, "completed", 2, None).await;
    db.close().await;
}
