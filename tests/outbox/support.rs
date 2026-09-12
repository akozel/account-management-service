use super::*;

pub(super) struct Database {
    pub(super) pool: PgPool,
    container: ContainerAsync<GenericImage>,
}
impl Database {
    pub(super) async fn new() -> Self {
        let container = GenericImage::new("docker.io/library/postgres", "18.6-alpine")
            .with_exposed_port(5432.tcp())
            .with_wait_for(WaitFor::log(
                LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections").with_times(2),
            ))
            .with_env_var("POSTGRES_DB", "outbox_test")
            .with_env_var("POSTGRES_USER", "outbox_test")
            .with_env_var("POSTGRES_PASSWORD", "outbox_test")
            .start()
            .await
            .unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&format!("postgresql://outbox_test:outbox_test@{host}:{port}/outbox_test"))
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../../deploy/postgres/001-event-store.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../../deploy/postgres/002-transactional-outbox.sql"))
            .execute(&pool)
            .await
            .unwrap();
        Self { pool, container }
    }
    pub(super) async fn close(self) {
        self.pool.close().await;
        self.container.stop().await.unwrap();
    }
}

pub(super) fn new_task() -> NewOutboxTask {
    NewOutboxTask::new(SendVerificationCodeTaskV1 {
        email: Email::parse("alice@example.com").unwrap(),
        account_id: "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse().unwrap(),
        verification_code: 1_234_567,
    })
    .unwrap()
}
pub(super) fn format() -> TaskFormat {
    TaskFormat::of::<SendVerificationCodeTaskV1>()
}
pub(super) fn event(id: &str, sequence: usize) -> SerializedEvent {
    SerializedEvent::new(
        id.to_owned(),
        sequence,
        EmailReservation::TYPE.to_owned(),
        "email_reserved".to_owned(),
        "1".to_owned(),
        json!({"EmailReserved": {"email": "alice@example.com", "account_id":"26e91668-f9fc-4be4-bd9d-321fba42e18b", "verification_code": 1234567}}),
        json!({"correlation_id": "command"}),
    )
}

#[derive(Default, Serialize, Deserialize)]
struct WorkerTestAggregate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum WorkerTestEvent {
    Recorded,
}

impl DomainEvent for WorkerTestEvent {
    fn event_type(&self) -> String {
        "recorded".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

impl Aggregate for WorkerTestAggregate {
    const TYPE: &'static str = "worker_test";
    type Command = ();
    type Event = WorkerTestEvent;
    type Error = std::convert::Infallible;
    type Services = ();

    async fn handle(&mut self, _: (), _: &(), _: &EventSink<Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn apply(&mut self, _: Self::Event) {}
}

pub(super) fn worker_event(id: &str) -> SerializedEvent {
    SerializedEvent::new(
        id.to_owned(),
        1,
        WorkerTestAggregate::TYPE.to_owned(),
        "recorded".into(),
        "1".into(),
        json!("Recorded"),
        json!({}),
    )
}

pub(super) async fn enqueue(pool: &PgPool, tasks: Vec<NewOutboxTask>) {
    let id = Uuid::new_v4().to_string();
    PostgresEventRepositoryWithOutbox::new(pool.clone())
        .with_outbox(tasks)
        .persist::<WorkerTestAggregate>(&[worker_event(&id)], None)
        .await
        .unwrap();
}

pub(super) struct TestHandler {
    pub(super) outcome: Result<(), TaskExecutionError>,
}

#[async_trait]
impl TaskHandler for TestHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
        self.outcome.clone()
    }
}

pub(super) fn handler(outcome: Result<(), TaskExecutionError>) -> TestHandler {
    TestHandler { outcome }
}

pub(super) struct HangingHandler;

#[async_trait]
impl TaskHandler for HangingHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
        std::future::pending().await
    }
}

pub(super) struct CountingHandler(pub(super) Arc<AtomicUsize>);

#[async_trait]
impl TaskHandler for CountingHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
pub(super) async fn row(pool: &PgPool, id: Uuid) -> sqlx::postgres::PgRow {
    sqlx::query("SELECT * FROM transactional_outbox WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}
pub(super) async fn make_ready(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE transactional_outbox SET available_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}
pub(super) async fn expire(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE transactional_outbox SET locked_until = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}
pub(super) async fn claim(queue: &PostgresOutboxQueue) -> Vec<ClaimedTask> {
    queue.claim(&format(), "test", 100, Duration::from_secs(60)).await.unwrap()
}

pub(super) async fn assert_worker_state(pool: &PgPool, id: Uuid, status: &str, attempts: i32, error: Option<&str>) {
    let saved = row(pool, id).await;
    assert_eq!(saved.get::<String, _>("status"), status);
    assert_eq!(saved.get::<i32, _>("attempts"), attempts);
    assert_eq!(saved.get::<Option<String>, _>("last_error").as_deref(), error);
    let processing = status == "processing";
    let finished = status == "completed" || status == "failed";
    assert_eq!(saved.get::<Option<String>, _>("locked_by").is_some(), processing);
    assert_eq!(saved.get::<Option<Uuid>, _>("lock_token").is_some(), processing);
    assert_eq!(saved.get::<Option<DateTime<Utc>>, _>("locked_until").is_some(), processing);
    assert_eq!(saved.get::<Option<DateTime<Utc>>, _>("finished_at").is_some(), finished);
}
