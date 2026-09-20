use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use account_management_service::{
    application::{
        email_reservation::{
            gateway::EmailReservationGateway,
            service::{RegistrationMaterialGenerator, email_reservation_service},
            tasks::send_verification_code::{
                DeliveryContext, VerificationCodeDelivery, VerificationCodeSender, send_verification_code_task_handler,
                v1::SendVerificationCodeTaskV1,
            },
        },
        outbox::{OutboxQueue, TaskExecutionError, TaskFormat},
        user_account::{
            service::{AcceptedSubmission, ContinuationError, ProfileSubmission, RegistrationService, SubmissionError},
            tasks::{CreateAccountTaskV1, RecordAccountCreatedTaskV1},
        },
    },
    domain::{EmailReservation, ReservationPhase, UserAccountId},
    infrastructure::{
        cqrs::{AggregateConflictRetryPolicy, email_reservation_gateway::CqrsEmailReservationGateway},
        postgres::{event_repository_with_outbox::PostgresEventRepositoryWithOutbox, outbox_queue::PostgresOutboxQueue},
    },
    presentation,
};
use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode, header},
};
use cqrs_es::{
    Aggregate, AggregateError, EventEnvelope, EventStore,
    persist::{EventStoreAggregateContext, PersistedEventRepository, PersistedEventStore},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use tokio::{
    sync::{Barrier, Notify},
    task::JoinSet,
    time::timeout,
};
use tower::ServiceExt;
use uuid::Uuid;

const CODE: u32 = 1_234_567;
type Store = PersistedEventStore<PostgresEventRepositoryWithOutbox, EmailReservation>;

struct TestDatabase {
    pool: PgPool,
    container: ContainerAsync<GenericImage>,
}

impl TestDatabase {
    async fn new() -> Self {
        let container = GenericImage::new("docker.io/library/postgres", "18.6-alpine")
            .with_exposed_port(5432.tcp())
            .with_wait_for(WaitFor::log(
                LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections").with_times(2),
            ))
            .with_env_var("POSTGRES_DB", "email_verification_test")
            .with_env_var("POSTGRES_USER", "email_verification_test")
            .with_env_var("POSTGRES_PASSWORD", "email_verification_test")
            .start()
            .await
            .expect("test PostgreSQL starts");
        let host = container.get_host().await.expect("container host");
        let port = container.get_host_port_ipv4(5432.tcp()).await.expect("container port");
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(&format!(
                "postgresql://email_verification_test:email_verification_test@{host}:{port}/email_verification_test"
            ))
            .await
            .expect("database connection");
        sqlx::raw_sql(include_str!("../deploy/postgres/001-event-store.sql"))
            .execute(&pool)
            .await
            .expect("event schema");
        sqlx::raw_sql(include_str!("../deploy/postgres/002-transactional-outbox.sql"))
            .execute(&pool)
            .await
            .expect("outbox schema");
        Self { pool, container }
    }

    async fn close(self) {
        self.pool.close().await;
        self.container.stop().await.expect("test PostgreSQL stops");
    }
}

fn store(pool: &PgPool) -> Store {
    PersistedEventStore::new_snapshot_store(PostgresEventRepositoryWithOutbox::new(pool.clone()), 3)
}

fn gateway(pool: &PgPool) -> Arc<dyn EmailReservationGateway> {
    let command_pool = pool.clone();
    Arc::new(CqrsEmailReservationGateway::new(
        move |messages| {
            let repository = PostgresEventRepositoryWithOutbox::new(command_pool.clone()).with_outbox(messages);
            cqrs_es::CqrsFramework::new(PersistedEventStore::new_snapshot_store(repository, 3), vec![], ())
        },
        AggregateConflictRetryPolicy::default(),
    ))
}

struct TestGenerator {
    next_id: AtomicU64,
    code: u32,
    next_token: AtomicU64,
}

impl TestGenerator {
    fn new(first_id: u64, code: u32) -> Self {
        Self {
            next_id: AtomicU64::new(first_id),
            code,
            next_token: AtomicU64::new(1),
        }
    }
}

impl RegistrationMaterialGenerator for TestGenerator {
    fn account_id(&self) -> UserAccountId {
        UserAccountId::from_uuid(Uuid::from_u128(self.next_id.fetch_add(1, Ordering::Relaxed).into()))
    }
    fn verification_code(&self) -> u32 {
        self.code
    }
    fn account_creation_token(&self) -> String {
        format!("test-token-{}", self.next_token.fetch_add(1, Ordering::Relaxed))
    }
}

struct UnusedRegistrationService;

#[async_trait]
impl RegistrationService for UnusedRegistrationService {
    async fn submit_profile(&self, _: ProfileSubmission) -> Result<AcceptedSubmission, SubmissionError> {
        unreachable!("email-verification tests do not submit profiles")
    }

    async fn create_account(&self, _: CreateAccountTaskV1) -> Result<(), ContinuationError> {
        unreachable!("email-verification tests do not continue account creation")
    }

    async fn record_account_created(&self, _: RecordAccountCreatedTaskV1) -> Result<(), ContinuationError> {
        unreachable!("email-verification tests do not continue account creation")
    }
}

fn app(pool: &PgPool, first_id: u64, code: u32) -> Router {
    presentation::http::router(
        email_reservation_service(gateway(pool), Arc::new(TestGenerator::new(first_id, code))),
        Arc::new(UnusedRegistrationService),
    )
}

async fn request(app: Router, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value, HeaderMap) {
    let response = timeout(
        Duration::from_secs(15),
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.map_or(Body::empty(), |body| Body::from(body.to_string())))
                .unwrap(),
        ),
    )
    .await
    .expect("request completes")
    .expect("router responds");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 16_384).await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body, headers)
}

async fn post(app: Router, uri: &str, body: Option<Value>) -> (StatusCode, Value, HeaderMap) {
    request(app, Method::POST, uri, body).await
}

async fn reserve(app: Router, email: &str) -> (StatusCode, Value, HeaderMap) {
    post(app, "/email-reservations", Some(json!({"email": email}))).await
}

async fn events(pool: &PgPool, email: &str) -> Vec<cqrs_es::persist::SerializedEvent> {
    PostgresEventRepositoryWithOutbox::new(pool.clone())
        .get_events::<EmailReservation>(email)
        .await
        .unwrap()
}

async fn outbox_tasks(pool: &PgPool, email: &str) -> Vec<(i64, i32, Value)> {
    sqlx::query_as("SELECT aggregate_sequence, task_version, payload FROM transactional_outbox WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY aggregate_sequence")
        .bind(EmailReservation::TYPE).bind(email).fetch_all(pool).await.unwrap()
}

async fn phase(pool: &PgPool, email: &str) -> ReservationPhase {
    store(pool).load_aggregate(email).await.unwrap().aggregate.phase().clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserve_reissue_verify_and_restart_preserve_only_the_current_pair() {
    let db = TestDatabase::new().await;
    let email = "alice+tag@example.com";
    let uri = "/email-reservations/Alice%2BTag%40Example.com";
    let (status, first, _) = reserve(app(&db.pool, 1, CODE), " Alice+Tag@Example.COM ").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(first["email"], email);
    assert!(first.get("phase").is_none());
    let first_id = first["account_id"].as_str().unwrap();
    assert_eq!(outbox_tasks(&db.pool, email).await[0].2["account_id"], first_id);
    assert_eq!(outbox_tasks(&db.pool, email).await[0].2["verification_code"], CODE);
    assert_eq!(outbox_tasks(&db.pool, email).await[0].1, 1);

    assert_eq!(reserve(app(&db.pool, 3, CODE), email).await.0, StatusCode::CONFLICT);
    assert_eq!(
        post(
            app(&db.pool, 3, CODE),
            &format!("{uri}/verify"),
            Some(json!({"account_id": first_id, "code": 7_654_321}))
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    for _ in 0..12 {
        assert_eq!(
            post(
                app(&db.pool, 3, CODE),
                &format!("{uri}/verify"),
                Some(json!({"account_id": first_id, "code": 7_654_321}))
            )
            .await
            .0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    assert_eq!(events(&db.pool, email).await.len(), 1);

    assert_eq!(
        post(app(&db.pool, 3, CODE), &format!("{uri}/verification-codes"), None)
            .await
            .0,
        StatusCode::ACCEPTED
    );
    let second_id = outbox_tasks(&db.pool, email).await[1].2["account_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(first_id, second_id);
    assert_eq!(outbox_tasks(&db.pool, email).await[1].2["verification_code"], CODE);
    assert_eq!(
        post(
            app(&db.pool, 4, CODE),
            &format!("{uri}/verify"),
            Some(json!({"account_id": first_id, "code": CODE}))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, verified, headers) = post(
        app(&db.pool, 4, CODE),
        &format!("{uri}/verify"),
        Some(json!({"account_id": second_id, "code": CODE})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    assert_eq!(verified["account_id"], second_id);
    let raw_token = verified["account_creation_token"].as_str().unwrap();
    assert_eq!(
        phase(&db.pool, email).await,
        ReservationPhase::AwaitingProfile {
            account_id: second_id.parse().unwrap(),
            token_digest: hex::encode(Sha256::digest(raw_token.as_bytes())),
        }
    );
    assert_eq!(outbox_tasks(&db.pool, email).await.len(), 2);
    assert_eq!(
        post(
            app(&db.pool, 4, CODE),
            &format!("{uri}/verify"),
            Some(json!({"account_id": second_id, "code": CODE}))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(reserve(app(&db.pool, 4, CODE), email).await.0, StatusCode::CONFLICT);

    assert_eq!(
        post(app(&db.pool, 5, 7_654_321), &format!("{uri}/verification-codes"), None)
            .await
            .0,
        StatusCode::ACCEPTED
    );
    let third_id = outbox_tasks(&db.pool, email).await[2].2["account_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(third_id, second_id);
    assert!(
        matches!(phase(&db.pool, email).await, ReservationPhase::AwaitingCode { account_id, verification_code: 7_654_321 } if account_id.to_string() == third_id)
    );
    assert_eq!(
        post(
            app(&db.pool, 6, CODE),
            &format!("{uri}/verify"),
            Some(json!({"account_id": second_id, "code": CODE}))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            app(&db.pool, 6, CODE),
            &format!("{uri}/verify"),
            Some(json!({"account_id": third_id, "code": 7_654_321}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(events(&db.pool, email).await.len(), 5);
    assert_eq!(outbox_tasks(&db.pool, email).await.len(), 3);
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_email_has_one_hold_even_across_concurrent_requests() {
    let db = TestDatabase::new().await;
    let router = app(&db.pool, 100, CODE);
    let barrier = Arc::new(Barrier::new(24));
    let mut tasks = JoinSet::new();
    for i in 0..24 {
        let app = router.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            reserve(
                app,
                if i % 2 == 0 {
                    " Race@Example.COM "
                } else {
                    "race@example.com"
                },
            )
            .await
            .0
        });
    }
    let mut accepted = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            StatusCode::ACCEPTED => accepted += 1,
            StatusCode::CONFLICT => {}
            other => panic!("unexpected status {other}"),
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(events(&db.pool, "race@example.com").await.len(), 1);
    assert_eq!(outbox_tasks(&db.pool, "race@example.com").await.len(), 1);
    assert_eq!(
        reserve(app(&db.pool, 200, CODE), "independent@example.com").await.0,
        StatusCode::ACCEPTED
    );
    assert_eq!(events(&db.pool, "independent@example.com").await.len(), 1);
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbox_and_verification_failures_leave_the_previous_state_usable() {
    let db = TestDatabase::new().await;
    let email = "rollback@example.com";
    let (_, first, _) = reserve(app(&db.pool, 301, CODE), email).await;
    let first_id = first["account_id"].as_str().unwrap();
    sqlx::raw_sql("ALTER TABLE transactional_outbox ADD CONSTRAINT reject_delivery CHECK (aggregate_sequence <> 2 AND aggregate_id <> 'refused@example.com')")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        post(
            app(&db.pool, 302, 7_654_321),
            "/email-reservations/rollback%40example.com/verification-codes",
            None
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(events(&db.pool, email).await.len(), 1);
    assert_eq!(outbox_tasks(&db.pool, email).await.len(), 1);
    assert!(
        matches!(phase(&db.pool, email).await, ReservationPhase::AwaitingCode { account_id, verification_code: CODE } if account_id.to_string() == first_id)
    );

    sqlx::raw_sql("ALTER TABLE events ADD CONSTRAINT reject_verification CHECK (event_type <> 'email_verified')")
        .execute(&db.pool)
        .await
        .unwrap();
    let (status, body, _) = post(
        app(&db.pool, 303, CODE),
        "/email-reservations/rollback%40example.com/verify",
        Some(json!({"account_id": first_id, "code": CODE})),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!body.to_string().contains("test-token"));
    assert_eq!(events(&db.pool, email).await.len(), 1);
    sqlx::raw_sql("ALTER TABLE events DROP CONSTRAINT reject_verification")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        post(
            app(&db.pool, 304, CODE),
            "/email-reservations/rollback%40example.com/verify",
            Some(json!({"account_id": first_id, "code": CODE}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(outbox_tasks(&db.pool, email).await.len(), 1);
    assert_eq!(
        reserve(app(&db.pool, 305, CODE), "refused@example.com").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(events(&db.pool, "refused@example.com").await.is_empty());
    assert!(outbox_tasks(&db.pool, "refused@example.com").await.is_empty());
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_or_invalid_requests_do_not_advance_the_email_boundary() {
    let db = TestDatabase::new().await;
    let path = "/email-reservations/missing%40example.com";
    assert_eq!(
        post(app(&db.pool, 500, CODE), &format!("{path}/verification-codes"), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            app(&db.pool, 500, CODE),
            &format!("{path}/verify"),
            Some(json!({"account_id":Uuid::from_u128(1),"code":CODE}))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(events(&db.pool, "missing@example.com").await.is_empty());
    assert!(outbox_tasks(&db.pool, "missing@example.com").await.is_empty());

    let email = "validation@example.com";
    let (_, reserved, _) = reserve(app(&db.pool, 501, CODE), email).await;
    let id = reserved["account_id"].as_str().unwrap();
    let uri = "/email-reservations/validation%40example.com/verify";
    for (body, status) in [
        (json!({"account_id":id,"code":0}), StatusCode::UNPROCESSABLE_ENTITY),
        (json!({"account_id":Uuid::from_u128(999),"code":CODE}), StatusCode::NOT_FOUND),
        (json!({"account_id":"bad","code":CODE}), StatusCode::BAD_REQUEST),
    ] {
        let (actual, response, _) = post(app(&db.pool, 502, CODE), uri, Some(body)).await;
        assert_eq!(actual, status);
        assert!(!response.to_string().contains("test-token"));
    }
    assert_eq!(events(&db.pool, email).await.len(), 1);
    assert_eq!(outbox_tasks(&db.pool, email).await.len(), 1);
    assert_eq!(
        post(app(&db.pool, 502, CODE), uri, Some(json!({"account_id":id,"code":CODE})))
            .await
            .0,
        StatusCode::OK
    );
    let account_facts: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE aggregate_type <> 'email_reservation'")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(account_facts, 0);
    db.close().await;
}

#[derive(Clone, Default)]
struct RecordingSender(Arc<Mutex<Vec<VerificationCodeDelivery>>>);

#[async_trait]
impl VerificationCodeSender for RecordingSender {
    async fn send(&mut self, message: VerificationCodeDelivery, _: DeliveryContext) -> Result<(), TaskExecutionError> {
        self.0.lock().unwrap().push(message);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_tasks_deliver_frozen_pairs_without_reactivating_old_ones() {
    let db = TestDatabase::new().await;
    let email = "delay@example.com";
    let (_, first, _) = reserve(app(&db.pool, 400, CODE), email).await;
    assert_eq!(
        post(
            app(&db.pool, 401, 7_654_321),
            "/email-reservations/delay%40example.com/verification-codes",
            None
        )
        .await
        .0,
        StatusCode::ACCEPTED
    );
    let current_id = outbox_tasks(&db.pool, email).await[1].2["account_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let queue = PostgresOutboxQueue::new(db.pool.clone());
    let mut messages = queue
        .claim(
            &TaskFormat::of::<SendVerificationCodeTaskV1>(),
            "test",
            2,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert_eq!(messages.len(), 2);
    messages.reverse();
    let sender = RecordingSender::default();
    let delivered = sender.0.clone();
    let mut handler = send_verification_code_task_handler(Box::new(sender));
    for message in &messages {
        handler.handle(message).await.unwrap();
    }
    {
        let delivered = delivered.lock().unwrap();
        assert_eq!(delivered[0].account_id.to_string(), current_id);
        assert_eq!(delivered[1].account_id.to_string(), first["account_id"].as_str().unwrap());
    }
    assert_eq!(
        post(
            app(&db.pool, 402, CODE),
            "/email-reservations/delay%40example.com/verify",
            Some(json!({"account_id": first["account_id"], "code": CODE}))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(events(&db.pool, email).await.len(), 2);
    db.close().await;
}

struct RaceGate {
    after_load: Barrier,
    first_committed: Notify,
}

struct GatedStore {
    inner: Store,
    gate: Arc<RaceGate>,
    first: bool,
    gate_once: Arc<AtomicBool>,
    wait_once: Arc<AtomicBool>,
}

impl EventStore<EmailReservation> for GatedStore {
    type AC = EventStoreAggregateContext<EmailReservation>;

    async fn load_events(
        &self,
        id: &str,
    ) -> Result<Vec<EventEnvelope<EmailReservation>>, AggregateError<account_management_service::domain::EmailReservationError>>
    {
        self.inner.load_events(id).await
    }

    async fn load_aggregate(
        &self,
        id: &str,
    ) -> Result<Self::AC, AggregateError<account_management_service::domain::EmailReservationError>> {
        let result = self.inner.load_aggregate(id).await?;
        if self.gate_once.swap(false, Ordering::SeqCst) {
            self.gate.after_load.wait().await;
        }
        Ok(result)
    }

    async fn commit(
        &self,
        events: Vec<account_management_service::domain::EmailReservationEvent>,
        context: Self::AC,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<EmailReservation>>, AggregateError<account_management_service::domain::EmailReservationError>>
    {
        if !self.first && self.wait_once.swap(false, Ordering::SeqCst) {
            self.gate.first_committed.notified().await;
        }
        let result = self.inner.commit(events, context, metadata).await;
        if self.first {
            self.gate.first_committed.notify_one();
        }
        result
    }
}

fn gated_app(pool: &PgPool, gate: Arc<RaceGate>, first: bool, first_id: u64, code: u32) -> Router {
    let command_pool = pool.clone();
    let gate_once = Arc::new(AtomicBool::new(true));
    let wait_once = Arc::new(AtomicBool::new(true));
    let gateway = Arc::new(CqrsEmailReservationGateway::new(
        move |messages| {
            let repository = PostgresEventRepositoryWithOutbox::new(command_pool.clone()).with_outbox(messages);
            let gated = GatedStore {
                inner: PersistedEventStore::new_snapshot_store(repository, 3),
                gate: gate.clone(),
                first,
                gate_once: gate_once.clone(),
                wait_once: wait_once.clone(),
            };
            cqrs_es::CqrsFramework::new(gated, vec![], ())
        },
        AggregateConflictRetryPolicy::default(),
    ));
    presentation::http::router(
        email_reservation_service(gateway, Arc::new(TestGenerator::new(first_id, code))),
        Arc::new(UnusedRegistrationService),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reissue_and_verify_races_follow_commit_order_without_stale_tasks() {
    let db = TestDatabase::new().await;
    for (name, verify_first) in [("reissue-first", false), ("verify-first", true)] {
        let email = format!("{name}@example.com");
        let (_, reserved, _) = reserve(app(&db.pool, 700, CODE), &email).await;
        let old_id = reserved["account_id"].as_str().unwrap().to_owned();
        let gate = Arc::new(RaceGate {
            after_load: Barrier::new(2),
            first_committed: Notify::new(),
        });
        let first = gated_app(&db.pool, gate.clone(), true, 701, 7_654_321);
        let second = gated_app(&db.pool, gate, false, 702, 8_765_432);
        let reissue_uri = format!("/email-reservations/{email}/verification-codes");
        let verify_uri = format!("/email-reservations/{email}/verify");
        let verify_body = json!({"account_id": old_id, "code": CODE});
        let (first_result, second_result) = if verify_first {
            tokio::join!(post(first, &verify_uri, Some(verify_body)), post(second, &reissue_uri, None))
        } else {
            tokio::join!(post(first, &reissue_uri, None), post(second, &verify_uri, Some(verify_body)))
        };
        assert_eq!(
            first_result.0,
            if verify_first { StatusCode::OK } else { StatusCode::ACCEPTED }
        );
        assert_eq!(
            second_result.0,
            if verify_first {
                StatusCode::ACCEPTED
            } else {
                StatusCode::NOT_FOUND
            }
        );
        if !verify_first {
            assert_eq!(second_result.1["code"], "registration_not_current");
            assert!(second_result.2.get(header::RETRY_AFTER).is_none());
        }
        assert!(
            matches!(phase(&db.pool, &email).await, ReservationPhase::AwaitingCode { account_id, .. } if account_id.to_string() != old_id)
        );
        assert_eq!(outbox_tasks(&db.pool, &email).await.len(), 2);
        assert_eq!(events(&db.pool, &email).await.len(), if verify_first { 3 } else { 2 });
    }
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reissues_retry_and_create_only_committed_delivery_tasks() {
    let db = TestDatabase::new().await;
    let email = "reissues@example.com";
    assert_eq!(reserve(app(&db.pool, 800, CODE), email).await.0, StatusCode::ACCEPTED);
    let gate = Arc::new(RaceGate {
        after_load: Barrier::new(2),
        first_committed: Notify::new(),
    });
    let uri = "/email-reservations/reissues%40example.com/verification-codes";
    let (first, second) = tokio::join!(
        post(gated_app(&db.pool, gate.clone(), true, 801, 7_654_321), uri, None),
        post(gated_app(&db.pool, gate, false, 802, 8_765_432), uri, None),
    );
    assert_eq!(first.0, StatusCode::ACCEPTED);
    assert_eq!(second.0, StatusCode::ACCEPTED);
    let events = events(&db.pool, email).await;
    let tasks = outbox_tasks(&db.pool, email).await;
    assert_eq!(events.len(), 3);
    assert_eq!(tasks.len(), 3);
    for (event, task) in events.iter().zip(&tasks) {
        assert_eq!(event.sequence as i64, task.0);
        let fact = event.payload.as_object().unwrap().values().next().unwrap();
        assert_eq!(fact["account_id"], task.2["account_id"]);
        assert_eq!(fact["verification_code"], task.2["verification_code"]);
    }
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_verifications_of_one_pair_issue_only_one_token_digest() {
    let db = TestDatabase::new().await;
    let email = "verify-twice@example.com";
    let (_, reserved, _) = reserve(app(&db.pool, 900, CODE), email).await;
    let id = reserved["account_id"].as_str().unwrap();
    let gate = Arc::new(RaceGate {
        after_load: Barrier::new(2),
        first_committed: Notify::new(),
    });
    let uri = "/email-reservations/verify-twice%40example.com/verify";
    let body = json!({"account_id":id,"code":CODE});
    let (first, second) = tokio::join!(
        post(gated_app(&db.pool, gate.clone(), true, 901, CODE), uri, Some(body.clone())),
        post(gated_app(&db.pool, gate, false, 902, CODE), uri, Some(body)),
    );
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::CONFLICT);
    assert_eq!(second.1["code"], "already_verified");
    assert!(second.2.get(header::RETRY_AFTER).is_none());
    assert!(second.1.get("account_creation_token").is_none());
    assert_eq!(events(&db.pool, email).await.len(), 2);
    assert_eq!(outbox_tasks(&db.pool, email).await.len(), 1);
    db.close().await;
}
