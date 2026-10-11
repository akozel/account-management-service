use super::*;
use cqrs_runtime::outbox::TaskDelivery;

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
        let options = format!("postgresql://outbox_test:outbox_test@{host}:{port}/outbox_test")
            .parse()
            .unwrap();
        let pool = cqrs_runtime::postgres::connect_pool(options).await.unwrap();
        sqlx::raw_sql(EVENT_STORE_SCHEMA).execute(&pool).await.unwrap();
        sqlx::raw_sql(TRANSACTIONAL_OUTBOX_SCHEMA).execute(&pool).await.unwrap();
        Self { pool, container }
    }
    pub(super) async fn close(self) {
        self.pool.close().await;
        self.container.stop().await.unwrap();
    }
}

pub(super) fn new_task() -> NewOutboxTask {
    NewOutboxTask::new(DummyTaskV1 {
        value: "runtime".to_owned(),
    })
    .unwrap()
}
pub(super) fn format() -> TaskFormat {
    TaskFormat::of::<DummyTaskV1>().unwrap()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct DummyTaskV1 {
    pub(super) value: String,
}

impl TaskPayload for DummyTaskV1 {
    const TASK_TYPE: &'static str = "dummy_task";
    const TASK_VERSION: i32 = 1;
}

pub(super) fn event(id: &str, sequence: usize) -> SerializedEvent {
    let payload = TestEvent::Recorded {
        value: format!("event-{sequence}"),
    };
    SerializedEvent::new(
        id.to_owned(),
        sequence,
        TestAggregate::TYPE.to_owned(),
        payload.event_type(),
        "1".to_owned(),
        serde_json::to_value(payload).unwrap(),
        json!({"correlation_id": "command"}),
    )
}

#[derive(Default, Serialize, Deserialize)]
pub(super) struct TestAggregate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum TestEvent {
    Recorded { value: String },
}

impl DomainEvent for TestEvent {
    fn event_type(&self) -> String {
        "recorded".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

impl Aggregate for TestAggregate {
    const TYPE: &'static str = "runtime_test";
    type Command = ();
    type Event = TestEvent;
    type Error = std::convert::Infallible;
    type Services = ();

    async fn handle(&mut self, _: (), _: &(), _: &EventSink<Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn apply(&mut self, _: Self::Event) {}
}

pub(super) fn worker_event(id: &str) -> SerializedEvent {
    event(id, 1)
}

pub(super) async fn enqueue(pool: &PgPool, tasks: Vec<NewOutboxTask>) {
    let id = Uuid::new_v4().to_string();
    PostgresEventRepositoryWithOutbox::new(pool.clone())
        .with_outbox(tasks)
        .persist::<TestAggregate>(&[worker_event(&id)], None)
        .await
        .unwrap();
}

pub(super) struct TestHandler {
    pub(super) outcome: Result<(), TaskHandlerError>,
}

#[async_trait]
impl TaskHandler for TestHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
        self.outcome.clone()
    }
}

pub(super) fn handler(outcome: Result<(), TaskHandlerError>) -> TestHandler {
    TestHandler { outcome }
}

pub(super) struct HangingHandler;

#[async_trait]
impl TaskHandler for HangingHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
        std::future::pending().await
    }
}

pub(super) struct CountingHandler(pub(super) Arc<AtomicUsize>);

#[async_trait]
impl TaskHandler for CountingHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn handle(&mut self, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
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
