use super::*;

#[tokio::test]
async fn two_live_workers_complete_every_task_including_old_codes_and_quarantine_bad_payloads() {
    let db = Database::new().await;
    let mut first = new_task();
    first.payload["verification_code"] = json!(1111111);
    let mut second = new_task();
    second.payload["verification_code"] = json!(2222222);
    let mut bad = new_task();
    bad.payload = json!({"email": "alice@example.com"});
    enqueue(&db.pool, vec![first.clone(), second.clone(), bad.clone()]).await;
    let writer = SharedWriter::default();
    let output = writer.clone();
    let factory: TaskHandlerFactory = Arc::new(move || {
        TaskHandlerRegistry::try_new(vec![send_verification_code_task_handler(Box::new(
            ConsoleVerificationCodeSender::new(output.clone()),
        ))])
        .unwrap()
    });
    let service = outbox_task_coordinator(Arc::new(PostgresOutboxQueue::new(db.pool.clone())), vec![format()]);
    let (stop, shutdown) = watch::channel(false);
    let config = OutboxWorkerPoolConfig {
        concurrency: 2,
        poll_interval: Duration::from_millis(10),
        ..Default::default()
    };
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
    let log = writer.text();
    assert_eq!(log.lines().count(), 2);
    for msg in [first, second] {
        assert!(log.contains(&format!("sent to alice@example.com; task_id={}", msg.id)));
        assert!(log.contains(&msg.payload["verification_code"].to_string()));
        assert_eq!(row(&db.pool, msg.id).await.get::<String, _>("status"), "completed");
    }
    assert_eq!(row(&db.pool, bad.id).await.get::<String, _>("status"), "failed");
    db.close().await;
}

#[derive(Clone, Default)]
struct SharedWriter(Arc<std::sync::Mutex<Vec<u8>>>);
impl SharedWriter {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}
impl std::io::Write for SharedWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn one_worker_pool_routes_two_registered_task_types() {
    struct AuditHandler(Arc<AtomicUsize>);
    #[async_trait]
    impl TaskHandler for AuditHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat {
                task_type: "audit".into(),
                task_version: 1,
            }]
        }
        async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let db = Database::new().await;
    let code = new_task();
    let mut audit = new_task();
    audit.task_type = "audit".into();
    audit.task_version = 1;
    audit.payload = json!({"kind":"reservation"});
    let mut unknown = new_task();
    unknown.task_type = "future".into();
    enqueue(&db.pool, vec![code.clone(), audit.clone(), unknown.clone()]).await;

    let sent = SharedWriter::default();
    let seen = Arc::new(AtomicUsize::new(0));
    let factory: TaskHandlerFactory = Arc::new({
        let seen = seen.clone();
        move || {
            TaskHandlerRegistry::try_new(vec![
                send_verification_code_task_handler(Box::new(ConsoleVerificationCodeSender::new(sent.clone()))),
                Box::new(AuditHandler(seen.clone())),
            ])
            .unwrap()
        }
    });
    let service = outbox_task_coordinator(Arc::new(PostgresOutboxQueue::new(db.pool.clone())), factory().formats());
    let (stop, shutdown) = watch::channel(false);
    let worker = tokio::spawn(outbox_worker_pool::run(
        service,
        factory,
        OutboxWorkerPoolConfig {
            concurrency: 2,
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
        shutdown,
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while row(&db.pool, code.id).await.get::<String, _>("status") != "completed"
            || row(&db.pool, audit.id).await.get::<String, _>("status") != "completed"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(row(&db.pool, unknown.id).await.get::<String, _>("status"), "pending");
    assert_eq!(row(&db.pool, unknown.id).await.get::<i32, _>("attempts"), 0);
    db.close().await;
}
