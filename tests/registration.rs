use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use account_management_service::{
    application::{
        CommandGatewayError,
        email_reservation::{
            gateway::EmailReservationGateway,
            service::{EmailReservationServiceError, RegistrationMaterialGenerator, email_reservation_service},
            tasks::send_verification_code::v1::SendVerificationCodeTaskV1,
        },
        outbox::{ClaimedTask, NewOutboxTask, TaskFormat, TaskPayload, outbox_task_coordinator},
        user_account::{
            gateway::UserAccountGateway,
            service::{
                ContinuationError, ProfileSubmission, RegistrationClock, RegistrationService, SubmissionError,
                registration_service,
            },
            tasks::{
                CreateAccountTaskV1, RecordAccountCreatedTaskV1, create_account::create_account_task_handler,
                record_account_created::record_account_created_task_handler,
            },
        },
    },
    domain::{
        Email, EmailReservation, EmailReservationCommand, EmailReservationError, EmailReservationEvent, UserAccount,
        UserAccountCommand, UserAccountError, UserAccountEvent, UserAccountId,
    },
    infrastructure::{
        cqrs::{email_reservation_gateway::CqrsEmailReservationGateway, user_account_gateway::CqrsUserAccountGateway},
        postgres::{event_repository_with_outbox::PostgresEventRepositoryWithOutbox, outbox_queue::PostgresOutboxQueue},
    },
    presentation,
};
use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{DateTime, NaiveDate, Utc};
use cqrs_es::EventStore;
use cqrs_es::persist::PersistedEventRepository;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use tower::ServiceExt;
use uuid::Uuid;

const CODE: u32 = 1_234_567;
const TOKEN: &str = "test-token-for-profile-acceptance";

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
            .with_env_var("POSTGRES_DB", "registration_test")
            .with_env_var("POSTGRES_USER", "registration_test")
            .with_env_var("POSTGRES_PASSWORD", "registration_test")
            .start()
            .await
            .expect("PostgreSQL starts");
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(&format!(
                "postgresql://registration_test:registration_test@{host}:{port}/registration_test"
            ))
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../deploy/postgres/001-event-store.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../deploy/postgres/002-transactional-outbox.sql"))
            .execute(&pool)
            .await
            .unwrap();
        Self { pool, container }
    }

    async fn close(self) {
        self.pool.close().await;
        self.container.stop().await.unwrap();
    }
}

fn email() -> Email {
    Email::parse("alice@example.com").unwrap()
}
fn id(n: u128) -> UserAccountId {
    UserAccountId::from_uuid(Uuid::from_u128(n))
}
fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}
fn time(day: u32) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&format!("2026-09-{day:02}T12:00:00Z"))
        .unwrap()
        .with_timezone(&Utc)
}

struct TestClock(Mutex<DateTime<Utc>>);
impl TestClock {
    fn new() -> Self {
        Self(Mutex::new(time(14)))
    }
    fn set(&self, value: DateTime<Utc>) {
        *self.0.lock().unwrap() = value;
    }
}
impl RegistrationClock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

struct TestGenerator;
impl RegistrationMaterialGenerator for TestGenerator {
    fn account_id(&self) -> UserAccountId {
        id(1)
    }
    fn verification_code(&self) -> u32 {
        CODE
    }
    fn account_creation_token(&self) -> String {
        TOKEN.into()
    }
}

struct NextGenerator;
impl RegistrationMaterialGenerator for NextGenerator {
    fn account_id(&self) -> UserAccountId {
        id(2)
    }
    fn verification_code(&self) -> u32 {
        7_654_321
    }
    fn account_creation_token(&self) -> String {
        "next-token".into()
    }
}

fn email_gateway(pool: &PgPool) -> Arc<dyn EmailReservationGateway> {
    let pool = pool.clone();
    Arc::new(CqrsEmailReservationGateway::new(move |tasks| {
        cqrs_es::CqrsFramework::new(
            cqrs_es::persist::PersistedEventStore::new_snapshot_store(
                PostgresEventRepositoryWithOutbox::new(pool.clone()).with_outbox(tasks),
                3,
            ),
            vec![],
            (),
        )
    }))
}

fn registration(pool: &PgPool, clock: Arc<TestClock>) -> Arc<dyn RegistrationService> {
    registration_service(email_gateway(pool), account_gateway(pool), clock)
}

fn account_gateway(pool: &PgPool) -> Arc<dyn UserAccountGateway> {
    let account_pool = pool.clone();
    Arc::new(CqrsUserAccountGateway::new(move |tasks| {
        cqrs_es::CqrsFramework::new(
            cqrs_es::persist::PersistedEventStore::new_snapshot_store(
                PostgresEventRepositoryWithOutbox::new(account_pool.clone()).with_outbox(tasks),
                1,
            ),
            vec![],
            (),
        )
    }))
}

struct RecordingAccountGateway {
    inner: Arc<dyn UserAccountGateway>,
    domain_errors: Arc<Mutex<Vec<UserAccountError>>>,
}

#[async_trait]
impl UserAccountGateway for RecordingAccountGateway {
    async fn execute_with_outbox(
        &self,
        command: UserAccountCommand,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<UserAccountError>> {
        let result = self.inner.execute_with_outbox(command, tasks).await;
        if let Err(CommandGatewayError::Domain(error)) = &result {
            self.domain_errors.lock().unwrap().push(error.clone());
        }
        result
    }
}

fn http_app(pool: &PgPool, clock: Arc<TestClock>) -> Router {
    presentation::http::router(
        email_reservation_service(email_gateway(pool), Arc::new(TestGenerator)),
        registration(pool, clock),
    )
}

async fn http_post(app: Router, uri: &str, body: Value) -> (StatusCode, Value, axum::http::HeaderMap) {
    let response = app
        .oneshot(
            Request::post(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 16_384).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap(), headers)
}

async fn verify(pool: &PgPool) {
    verify_address(pool, email()).await;
}

async fn verify_address(pool: &PgPool, address: Email) {
    let service = email_reservation_service(email_gateway(pool), Arc::new(TestGenerator));
    let reserved = service.reserve(address.clone()).await.unwrap();
    assert_eq!(reserved.account_id, id(1));
    let verified = service.confirm_email(address, id(1), CODE).await.unwrap();
    assert_eq!(verified.account_creation_token, TOKEN);
}

fn submission() -> ProfileSubmission {
    ProfileSubmission {
        email: email(),
        account_id: id(1),
        account_creation_token: TOKEN.into(),
        first_name: " Alice ".into(),
        last_name: " Smith ".into(),
        date_of_birth: NaiveDate::from_ymd_opt(1990, 5, 12).unwrap(),
    }
}

async fn email_events(pool: &PgPool) -> Vec<EmailReservationEvent> {
    PostgresEventRepositoryWithOutbox::new(pool.clone())
        .get_events::<EmailReservation>(email().as_str())
        .await
        .unwrap()
        .into_iter()
        .map(|event| serde_json::from_value(event.payload).unwrap())
        .collect()
}

async fn account_events(pool: &PgPool, account_id: UserAccountId) -> Vec<UserAccountEvent> {
    PostgresEventRepositoryWithOutbox::new(pool.clone())
        .get_events::<UserAccount>(&account_id.to_string())
        .await
        .unwrap()
        .into_iter()
        .map(|event| serde_json::from_value(event.payload).unwrap())
        .collect()
}

async fn tasks(pool: &PgPool, task_type: &str) -> Vec<(Uuid, Value)> {
    sqlx::query_as("SELECT id, payload FROM transactional_outbox WHERE task_type = $1 ORDER BY created_at, id")
        .bind(task_type)
        .fetch_all(pool)
        .await
        .unwrap()
}

fn claimed<T: TaskPayload>(row: &(Uuid, Value)) -> ClaimedTask {
    ClaimedTask {
        id: row.0,
        format: TaskFormat::of::<T>(),
        payload: row.1.clone(),
        metadata: json!({}),
        attempt: 1,
        lock_token: Uuid::new_v4(),
    }
}

#[tokio::test]
async fn full_http_registration_path_is_atomic_and_rejects_token_reuse() {
    let db = TestDatabase::new().await;
    let app = http_app(&db.pool, Arc::new(TestClock::new()));

    let (status, reserved, _) = http_post(app.clone(), "/email-reservations", json!({"email":" Alice@Example.COM "})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let account_id = reserved["account_id"].as_str().unwrap();

    let (status, verified, _) = http_post(
        app.clone(),
        "/email-reservations/alice%40example.com/verify",
        json!({"account_id": account_id, "code": CODE}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let token = verified["account_creation_token"].as_str().unwrap();
    let profile = json!({
        "email": "Alice@Example.COM",
        "account_id": account_id,
        "account_creation_token": token,
        "first_name": " Alice ",
        "last_name": " Smith ",
        "date_of_birth": "1990-05-12"
    });

    let (status, accepted, headers) = http_post(app.clone(), "/user-accounts", profile.clone()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(
        headers
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert_eq!(
        accepted,
        json!({
            "account_id": account_id,
            "status_url": format!("/email-reservations/alice%40example.com/registrations/{account_id}")
        })
    );
    let first_events = email_events(&db.pool).await;
    assert_eq!(
        first_events
            .iter()
            .filter(|event| matches!(event, EmailReservationEvent::RegistrationProfileAccepted { .. }))
            .count(),
        1
    );
    let create_tasks = tasks(&db.pool, "create_account").await;
    assert_eq!(create_tasks.len(), 1);
    let task: CreateAccountTaskV1 = serde_json::from_value(create_tasks[0].1.clone()).unwrap();
    assert_eq!(task.email, email());
    assert_eq!(task.account_id.to_string(), account_id);
    assert_eq!(task.profile.first_name, "Alice");
    assert_eq!(task.profile.last_name, "Smith");
    assert_eq!(task.profile.date_of_birth, NaiveDate::from_ymd_opt(1990, 5, 12).unwrap());

    let (status, repeated, _) = http_post(app.clone(), "/user-accounts", profile.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(repeated["code"], "profile_already_submitted");
    assert_eq!(email_events(&db.pool).await, first_events);
    assert_eq!(tasks(&db.pool, "create_account").await.len(), 1);

    let mut different = profile;
    different["last_name"] = json!("Jones");
    let (status, rejected, _) = http_post(app, "/user-accounts", different).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(rejected["code"], "profile_already_submitted");
    assert_eq!(email_events(&db.pool).await, first_events);
    assert_eq!(tasks(&db.pool, "create_account").await.len(), 1);
    db.close().await;
}

#[tokio::test]
async fn reused_http_token_is_rejected_while_create_task_redelivery_completes() {
    let db = TestDatabase::new().await;
    let clock = Arc::new(TestClock::new());
    let app = http_app(&db.pool, clock.clone());

    let (status, reserved, _) = http_post(app.clone(), "/email-reservations", json!({"email": "alice@example.com"})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let account_id = reserved["account_id"].as_str().unwrap();
    let (status, verified, _) = http_post(
        app.clone(),
        "/email-reservations/alice%40example.com/verify",
        json!({"account_id": account_id, "code": CODE}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let profile = json!({
        "email": "alice@example.com",
        "account_id": account_id,
        "account_creation_token": verified["account_creation_token"],
        "first_name": "Alice",
        "last_name": "Smith",
        "date_of_birth": "1990-05-12"
    });
    let (status, _, _) = http_post(app.clone(), "/user-accounts", profile.clone()).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let create_tasks = tasks(&db.pool, "create_account").await;
    assert_eq!(create_tasks.len(), 1);
    let create_task_id = create_tasks[0].0;
    let aggregate_rejections = Arc::new(Mutex::new(Vec::new()));
    let task_service = registration_service(
        email_gateway(&db.pool),
        Arc::new(RecordingAccountGateway {
            inner: account_gateway(&db.pool),
            domain_errors: aggregate_rejections.clone(),
        }),
        clock,
    );
    let queue = Arc::new(PostgresOutboxQueue::new(db.pool.clone()));
    let coordinator = outbox_task_coordinator(queue, vec![TaskFormat::of::<CreateAccountTaskV1>()]);
    let mut handler = create_account_task_handler(task_service);

    let first = coordinator
        .claim("create-account-worker-1", 1, Duration::from_secs(30))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(first.id, create_task_id);
    assert_eq!(first.attempt, 1);
    handler.handle(&first).await.unwrap();
    assert_eq!(account_events(&db.pool, id(1)).await.len(), 1);
    assert_eq!(tasks(&db.pool, "record_account_created").await.len(), 1);
    let first_delivery_state: (String, i32) = sqlx::query_as("SELECT status, attempts FROM transactional_outbox WHERE id = $1")
        .bind(create_task_id)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(first_delivery_state, ("processing".into(), 1));

    let (status, rejected, _) = http_post(app, "/user-accounts", profile).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(rejected["code"], "profile_already_submitted");
    assert_eq!(tasks(&db.pool, "create_account").await.len(), 1);

    sqlx::query("UPDATE transactional_outbox SET locked_until = clock_timestamp() - interval '1 second' WHERE id = $1")
        .bind(create_task_id)
        .execute(&db.pool)
        .await
        .unwrap();
    let recovery = coordinator.recover_expired(1).await.unwrap();
    assert_eq!(recovery.recovered, 1);
    sqlx::query("UPDATE transactional_outbox SET available_at = clock_timestamp() - interval '1 second' WHERE id = $1")
        .bind(create_task_id)
        .execute(&db.pool)
        .await
        .unwrap();

    let second = coordinator
        .claim("create-account-worker-2", 1, Duration::from_secs(30))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.id, create_task_id);
    assert_eq!(second.attempt, 2);
    assert_ne!(second.lock_token, first.lock_token);
    assert!(
        coordinator
            .execute_and_settle(handler.as_mut(), second, Duration::from_secs(1))
            .await
            .unwrap()
            .is_settled()
    );
    assert_eq!(*aggregate_rejections.lock().unwrap(), vec![UserAccountError::AlreadyCreated]);
    let completed: (String, i32, Option<String>, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT status, attempts, last_error, finished_at FROM transactional_outbox WHERE id = $1")
            .bind(create_task_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(completed.0, "completed");
    assert_eq!(completed.1, 2);
    assert_eq!(completed.2, None);
    assert!(completed.3.is_some());
    assert_eq!(account_events(&db.pool, id(1)).await.len(), 1);
    assert_eq!(tasks(&db.pool, "record_account_created").await.len(), 1);
    db.close().await;
}

#[tokio::test]
async fn profile_acceptance_creates_one_account_and_consumes_the_token() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let clock = Arc::new(TestClock::new());
    let service = registration(&db.pool, clock.clone());
    let accepted = service.submit_profile(submission()).await.unwrap();
    assert_eq!(accepted.account_id, id(1));
    let first_events = email_events(&db.pool).await;
    assert_eq!(first_events.len(), 3);
    let EmailReservationEvent::RegistrationProfileAccepted {
        account_id,
        consumed_token_digest,
        profile,
        ..
    } = &first_events[2]
    else {
        panic!("profile accepted event")
    };
    assert_eq!(*account_id, id(1));
    assert_eq!(consumed_token_digest, &hex::encode(Sha256::digest(TOKEN.as_bytes())));
    assert_eq!(profile.first_name, "Alice");
    assert_eq!(profile.validation_date, date(14));
    let create_tasks = tasks(&db.pool, "create_account").await;
    assert_eq!(create_tasks.len(), 1);
    let payload: CreateAccountTaskV1 = serde_json::from_value(create_tasks[0].1.clone()).unwrap();
    assert_eq!(payload.email, email());
    assert_eq!(payload.account_id, id(1));
    assert_eq!(&payload.profile, profile);
    assert!(!create_tasks[0].1.to_string().contains(TOKEN));
    let proof_service = email_reservation_service(email_gateway(&db.pool), Arc::new(NextGenerator));
    assert_eq!(
        proof_service.request_new_verification_code(email()).await,
        Err(EmailReservationServiceError::AccountCreationStarted)
    );
    assert_eq!(
        proof_service.confirm_email(email(), id(1), CODE).await,
        Err(EmailReservationServiceError::AccountCreationStarted)
    );
    assert_eq!(email_events(&db.pool).await, first_events);
    assert_eq!(tasks(&db.pool, "send_verification_code").await.len(), 1);

    clock.set(time(15));
    let mut retry = submission();
    retry.first_name = "Alice".into();
    assert_eq!(
        service.submit_profile(retry).await,
        Err(SubmissionError::Domain(EmailReservationError::ProfileAlreadySubmitted))
    );
    assert_eq!(email_events(&db.pool).await, first_events);
    assert_eq!(tasks(&db.pool, "create_account").await.len(), 1);
    let mut create_handler = create_account_task_handler(service.clone());
    let task = claimed::<CreateAccountTaskV1>(&create_tasks[0]);
    create_handler.handle(&task).await.unwrap();
    create_handler.handle(&task).await.unwrap();
    let account = account_events(&db.pool, id(1)).await;
    assert_eq!(account.len(), 1);
    let UserAccountEvent::AccountCreated {
        email: account_email,
        first_name,
        last_name,
        date_of_birth,
        created_at,
        validation_date,
        ..
    } = &account[0];
    assert_eq!(account_email, &email());
    assert_eq!(*created_at, time(15));
    assert_eq!(*validation_date, date(14));
    assert_eq!(first_name, &payload.profile.first_name);
    assert_eq!(last_name, &payload.profile.last_name);
    assert_eq!(*date_of_birth, payload.profile.date_of_birth);
    let completion = tasks(&db.pool, "record_account_created").await;
    assert_eq!(completion.len(), 1);
    let completion_payload: RecordAccountCreatedTaskV1 = serde_json::from_value(completion[0].1.clone()).unwrap();
    assert_eq!(completion_payload.email, *account_email);
    assert_eq!(completion_payload.account_id, id(1));
    assert_eq!(completion_payload.profile, payload.profile);
    assert_eq!(completion_payload.created_at, *created_at);
    let task_versions: Vec<(String, i32)> = sqlx::query_as(
        "SELECT task_type, task_version FROM transactional_outbox WHERE task_type IN ('send_verification_code', 'create_account', 'record_account_created') ORDER BY task_type",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        task_versions,
        vec![
            ("create_account".into(), 1),
            ("record_account_created".into(), 1),
            ("send_verification_code".into(), 1),
        ]
    );
    assert_eq!(email_events(&db.pool).await.len(), 3);
    clock.set(time(13));
    assert_eq!(
        service.submit_profile(submission()).await,
        Err(SubmissionError::Domain(EmailReservationError::ProfileAlreadySubmitted))
    );
    let mut completion_handler = record_account_created_task_handler(service.clone());
    completion_handler
        .handle(&claimed::<RecordAccountCreatedTaskV1>(&completion[0]))
        .await
        .unwrap();
    assert_eq!(email_events(&db.pool).await.len(), 4);
    let replayed = cqrs_es::persist::PersistedEventStore::<_, EmailReservation>::new_snapshot_store(
        PostgresEventRepositoryWithOutbox::new(db.pool.clone()),
        3,
    )
    .load_aggregate(email().as_str())
    .await
    .unwrap();
    assert!(
        matches!(replayed.aggregate.phase(), account_management_service::domain::ReservationPhase::AccountCreated { account_id, created_at, .. } if *account_id == id(1) && *created_at == time(15))
    );
    assert_eq!(
        service.submit_profile(submission()).await,
        Err(SubmissionError::Domain(EmailReservationError::ProfileAlreadySubmitted))
    );
    assert_eq!(tasks(&db.pool, "create_account").await.len(), 1);
    db.close().await;
}

#[tokio::test]
async fn invalid_profile_and_failed_outbox_commit_leave_token_usable() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let service = registration(&db.pool, Arc::new(TestClock::new()));
    let mut invalid = submission();
    invalid.first_name = "  ".into();
    assert_eq!(
        service.submit_profile(invalid).await,
        Err(SubmissionError::Domain(EmailReservationError::EmptyFirstName))
    );
    let mut invalid = submission();
    invalid.date_of_birth = date(15);
    assert_eq!(
        service.submit_profile(invalid).await,
        Err(SubmissionError::Domain(EmailReservationError::FutureDateOfBirth))
    );
    let mut invalid = submission();
    invalid.account_creation_token = "wrong".into();
    assert_eq!(
        service.submit_profile(invalid).await,
        Err(SubmissionError::Domain(EmailReservationError::InvalidAccountCreationToken))
    );
    assert_eq!(email_events(&db.pool).await.len(), 2);
    assert!(tasks(&db.pool, "create_account").await.is_empty());

    sqlx::query("ALTER TABLE transactional_outbox ADD CONSTRAINT reject_create CHECK (task_type <> 'create_account')")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(service.submit_profile(submission()).await, Err(SubmissionError::Unavailable));
    assert_eq!(email_events(&db.pool).await.len(), 2);
    assert!(tasks(&db.pool, "create_account").await.is_empty());
    sqlx::query("ALTER TABLE transactional_outbox DROP CONSTRAINT reject_create")
        .execute(&db.pool)
        .await
        .unwrap();
    service.submit_profile(submission()).await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn stale_or_mismatched_tasks_cannot_create_or_complete_accounts() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let service = registration(&db.pool, Arc::new(TestClock::new()));
    let missing = RecordAccountCreatedTaskV1 {
        email: email(),
        account_id: id(1),
        profile: account_management_service::domain::RegistrationProfile::new(
            "Alice".into(),
            "Jones".into(),
            submission().date_of_birth,
            date(14),
        ),
        created_at: time(14),
    };
    assert_eq!(
        service.record_account_created(missing.clone()).await,
        Err(ContinuationError::Permanent)
    );
    service.submit_profile(submission()).await.unwrap();
    let mut malformed = claimed::<CreateAccountTaskV1>(&tasks(&db.pool, "create_account").await[0]);
    malformed.payload = json!({});
    assert!(matches!(
        create_account_task_handler(service.clone()).handle(&malformed).await,
        Err(account_management_service::application::outbox::TaskExecutionError::Permanent { .. })
    ));
    malformed.format.task_version = 2;
    assert!(matches!(
        create_account_task_handler(service.clone()).handle(&malformed).await,
        Err(account_management_service::application::outbox::TaskExecutionError::Permanent { .. })
    ));
    let mut malformed_completion = claimed::<RecordAccountCreatedTaskV1>(&tasks(&db.pool, "create_account").await[0]);
    malformed_completion.payload = json!({});
    assert!(matches!(
        record_account_created_task_handler(service.clone())
            .handle(&malformed_completion)
            .await,
        Err(account_management_service::application::outbox::TaskExecutionError::Permanent { .. })
    ));
    malformed_completion.format.task_version = 2;
    assert!(matches!(
        record_account_created_task_handler(service.clone())
            .handle(&malformed_completion)
            .await,
        Err(account_management_service::application::outbox::TaskExecutionError::Permanent { .. })
    ));
    assert_eq!(
        service.record_account_created(missing.clone()).await,
        Err(ContinuationError::Permanent)
    );
    let mut payload: CreateAccountTaskV1 = serde_json::from_value(tasks(&db.pool, "create_account").await[0].1.clone()).unwrap();
    service.create_account(payload.clone()).await.unwrap();
    payload.profile.last_name = "Jones".into();
    assert_eq!(
        service.create_account(payload.clone()).await,
        Err(ContinuationError::Permanent)
    );
    payload.profile.last_name = "Smith".into();
    payload.email = Email::parse("bob@example.com").unwrap();
    assert_eq!(service.create_account(payload).await, Err(ContinuationError::Permanent));
    assert_eq!(account_events(&db.pool, id(1)).await.len(), 1);
    assert!(account_events(&db.pool, id(2)).await.is_empty());
    assert_eq!(email_events(&db.pool).await.len(), 3);
    db.close().await;
}

#[tokio::test]
async fn concurrent_profile_submissions_have_one_accepted_intent() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let service = registration(&db.pool, Arc::new(TestClock::new()));
    let (first, second) = tokio::join!(service.submit_profile(submission()), service.submit_profile(submission()));
    assert!(first.is_ok() ^ second.is_ok());
    assert_eq!(
        first.err().or_else(|| second.err()),
        Some(SubmissionError::Domain(EmailReservationError::ProfileAlreadySubmitted)),
    );
    assert_eq!(email_events(&db.pool).await.len(), 3);
    assert_eq!(tasks(&db.pool, "create_account").await.len(), 1);

    let bob = Email::parse("bob@example.com").unwrap();
    verify_address(&db.pool, bob.clone()).await;
    let mut one = submission();
    one.email = bob.clone();
    let mut two = one.clone();
    two.last_name = "Jones".into();
    let (first, second) = tokio::join!(service.submit_profile(one), service.submit_profile(two));
    assert!(first.is_ok() ^ second.is_ok());
    assert_eq!(
        first.err().or_else(|| second.err()),
        Some(SubmissionError::Domain(EmailReservationError::ProfileAlreadySubmitted)),
    );
    let bob_tasks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE task_type = 'create_account' AND aggregate_id = $1")
            .bind(bob.as_str())
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(bob_tasks, 1);
    db.close().await;
}

async fn reissue(pool: &PgPool, address: Email) -> Result<(), CommandGatewayError<EmailReservationError>> {
    let task = NewOutboxTask::new(SendVerificationCodeTaskV1 {
        email: address.clone(),
        account_id: id(2),
        verification_code: 7_654_321,
    })
    .unwrap();
    email_gateway(pool)
        .execute_with_outbox(
            EmailReservationCommand::RequestVerificationCode {
                email: address,
                account_id: id(2),
                verification_code: 7_654_321,
            },
            vec![task],
        )
        .await
}

#[tokio::test]
async fn profile_submission_and_code_reissue_follow_commit_order() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let service = registration(&db.pool, Arc::new(TestClock::new()));
    let (submit_result, reissue_result) = tokio::join!(service.submit_profile(submission()), reissue(&db.pool, email()));
    if submit_result.is_ok() {
        assert!(matches!(
            reissue_result,
            Err(CommandGatewayError::Conflict | CommandGatewayError::Domain(EmailReservationError::AccountCreationStarted))
        ));
        assert_eq!(tasks(&db.pool, "create_account").await.len(), 1);
        assert_eq!(tasks(&db.pool, "send_verification_code").await.len(), 1);
    } else {
        assert!(reissue_result.is_ok());
        assert!(matches!(
            submit_result,
            Err(SubmissionError::Conflict | SubmissionError::Domain(EmailReservationError::NotCurrentAccountId))
        ));
        assert!(tasks(&db.pool, "create_account").await.is_empty());
        assert_eq!(tasks(&db.pool, "send_verification_code").await.len(), 2);
    }

    let bob = Email::parse("bob@example.com").unwrap();
    verify_address(&db.pool, bob.clone()).await;
    reissue(&db.pool, bob.clone()).await.unwrap();
    let mut stale = submission();
    stale.email = bob;
    assert_eq!(
        service.submit_profile(stale).await,
        Err(SubmissionError::Domain(EmailReservationError::NotCurrentAccountId))
    );
    db.close().await;
}

#[tokio::test]
async fn concurrent_create_delivery_and_account_id_collision_fail_closed() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let service = registration(&db.pool, Arc::new(TestClock::new()));
    service.submit_profile(submission()).await.unwrap();
    let task: CreateAccountTaskV1 = serde_json::from_value(tasks(&db.pool, "create_account").await[0].1.clone()).unwrap();
    let (first, second) = tokio::join!(service.create_account(task.clone()), service.create_account(task.clone()));
    if first == Err(ContinuationError::Retry) {
        service.create_account(task.clone()).await.unwrap();
    }
    if second == Err(ContinuationError::Retry) {
        service.create_account(task).await.unwrap();
    }
    assert!(first.is_ok() || second.is_ok());
    assert_eq!(account_events(&db.pool, id(1)).await.len(), 1);
    assert_eq!(tasks(&db.pool, "record_account_created").await.len(), 1);
    assert_eq!(email_events(&db.pool).await.len(), 3);

    let bob = Email::parse("bob@example.com").unwrap();
    verify_address(&db.pool, bob.clone()).await;
    let mut other = submission();
    other.email = bob.clone();
    service.submit_profile(other).await.unwrap();
    let bob_task: CreateAccountTaskV1 = serde_json::from_value(
        sqlx::query_scalar::<_, Value>(
            "SELECT payload FROM transactional_outbox WHERE task_type = 'create_account' AND aggregate_id = $1",
        )
        .bind(bob.as_str())
        .fetch_one(&db.pool)
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(service.create_account(bob_task).await, Err(ContinuationError::Permanent));
    assert!(
        tasks(&db.pool, "record_account_created")
            .await
            .iter()
            .all(|(_, payload)| payload["email"] != bob.as_str())
    );
    assert_eq!(account_events(&db.pool, id(1)).await.len(), 1);
    db.close().await;
}

#[tokio::test]
async fn account_event_and_completion_task_roll_back_together() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let service = registration(&db.pool, Arc::new(TestClock::new()));
    service.submit_profile(submission()).await.unwrap();
    let task: CreateAccountTaskV1 = serde_json::from_value(tasks(&db.pool, "create_account").await[0].1.clone()).unwrap();
    sqlx::query(
        "ALTER TABLE transactional_outbox ADD CONSTRAINT reject_completion CHECK (task_type <> 'record_account_created')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(service.create_account(task.clone()).await, Err(ContinuationError::Retry));
    assert!(account_events(&db.pool, id(1)).await.is_empty());
    assert_eq!(email_events(&db.pool).await.len(), 3);
    assert!(tasks(&db.pool, "record_account_created").await.is_empty());
    sqlx::query("ALTER TABLE transactional_outbox DROP CONSTRAINT reject_completion")
        .execute(&db.pool)
        .await
        .unwrap();
    service.create_account(task).await.unwrap();
    assert_eq!(account_events(&db.pool, id(1)).await.len(), 1);
    assert_eq!(tasks(&db.pool, "record_account_created").await.len(), 1);
    db.close().await;
}

struct LostAckEmailGateway(Arc<dyn EmailReservationGateway>);

#[async_trait]
impl EmailReservationGateway for LostAckEmailGateway {
    async fn execute(&self, command: EmailReservationCommand) -> Result<(), CommandGatewayError<EmailReservationError>> {
        self.0.execute(command).await?;
        Err(CommandGatewayError::Unavailable)
    }
    async fn execute_with_outbox(
        &self,
        command: EmailReservationCommand,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<EmailReservationError>> {
        self.0.execute_with_outbox(command, tasks).await?;
        Err(CommandGatewayError::Unavailable)
    }
}

struct LostAckAccountGateway(Arc<dyn UserAccountGateway>);

#[async_trait]
impl UserAccountGateway for LostAckAccountGateway {
    async fn execute_with_outbox(
        &self,
        command: UserAccountCommand,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<UserAccountError>> {
        self.0.execute_with_outbox(command, tasks).await?;
        Err(CommandGatewayError::Unavailable)
    }
}

struct FailCompletionGateway(Arc<dyn EmailReservationGateway>);

#[async_trait]
impl EmailReservationGateway for FailCompletionGateway {
    async fn execute(&self, command: EmailReservationCommand) -> Result<(), CommandGatewayError<EmailReservationError>> {
        if matches!(command, EmailReservationCommand::RecordAccountCreated { .. }) {
            return Err(CommandGatewayError::Unavailable);
        }
        self.0.execute(command).await
    }
    async fn execute_with_outbox(
        &self,
        command: EmailReservationCommand,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<EmailReservationError>> {
        self.0.execute_with_outbox(command, tasks).await
    }
}

#[tokio::test]
async fn completion_task_recovers_account_committed_before_email_confirmation() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let failing = registration_service(
        Arc::new(FailCompletionGateway(email_gateway(&db.pool))),
        account_gateway(&db.pool),
        Arc::new(TestClock::new()),
    );
    failing.submit_profile(submission()).await.unwrap();
    let task_row = tasks(&db.pool, "create_account").await;
    let mut handler = create_account_task_handler(failing.clone());
    handler.handle(&claimed::<CreateAccountTaskV1>(&task_row[0])).await.unwrap();
    assert_eq!(account_events(&db.pool, id(1)).await.len(), 1);
    assert_eq!(email_events(&db.pool).await.len(), 3);
    let completion = tasks(&db.pool, "record_account_created").await;
    assert_eq!(completion.len(), 1);
    assert!(matches!(
        record_account_created_task_handler(failing)
            .handle(&claimed::<RecordAccountCreatedTaskV1>(&completion[0]))
            .await,
        Err(account_management_service::application::outbox::TaskExecutionError::Retry { .. })
    ));
    let normal = registration(&db.pool, Arc::new(TestClock::new()));
    record_account_created_task_handler(normal.clone())
        .handle(&claimed::<RecordAccountCreatedTaskV1>(&completion[0]))
        .await
        .unwrap();
    assert_eq!(email_events(&db.pool).await.len(), 4);
    normal
        .record_account_created(serde_json::from_value(completion[0].1.clone()).unwrap())
        .await
        .unwrap();
    assert_eq!(email_events(&db.pool).await.len(), 4);
    db.close().await;
}

#[tokio::test]
async fn unknown_profile_commit_outcome_still_consumes_the_token() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let service = registration_service(
        Arc::new(LostAckEmailGateway(email_gateway(&db.pool))),
        Arc::new(LostAckAccountGateway(account_gateway(&db.pool))),
        Arc::new(TestClock::new()),
    );
    assert_eq!(service.submit_profile(submission()).await, Err(SubmissionError::Unavailable));
    assert_eq!(
        service.submit_profile(submission()).await,
        Err(SubmissionError::Domain(EmailReservationError::ProfileAlreadySubmitted))
    );
    assert_eq!(tasks(&db.pool, "create_account").await.len(), 1);
    let task: CreateAccountTaskV1 = serde_json::from_value(tasks(&db.pool, "create_account").await[0].1.clone()).unwrap();
    assert_eq!(service.create_account(task.clone()).await, Err(ContinuationError::Retry));
    service.create_account(task).await.unwrap();
    assert_eq!(account_events(&db.pool, id(1)).await.len(), 1);
    assert_eq!(email_events(&db.pool).await.len(), 3);
    let completion: RecordAccountCreatedTaskV1 =
        serde_json::from_value(tasks(&db.pool, "record_account_created").await[0].1.clone()).unwrap();
    assert_eq!(
        service.record_account_created(completion.clone()).await,
        Err(ContinuationError::Retry)
    );
    assert_eq!(email_events(&db.pool).await.len(), 4);
    service.record_account_created(completion).await.unwrap();
    assert_eq!(email_events(&db.pool).await.len(), 4);
    db.close().await;
}

#[tokio::test]
async fn competing_completion_workers_record_one_event() {
    let db = TestDatabase::new().await;
    verify(&db.pool).await;
    let service = registration(&db.pool, Arc::new(TestClock::new()));
    service.submit_profile(submission()).await.unwrap();
    let create: CreateAccountTaskV1 = serde_json::from_value(tasks(&db.pool, "create_account").await[0].1.clone()).unwrap();
    service.create_account(create).await.unwrap();
    let completion: RecordAccountCreatedTaskV1 =
        serde_json::from_value(tasks(&db.pool, "record_account_created").await[0].1.clone()).unwrap();
    let (first, second) = tokio::join!(
        service.record_account_created(completion.clone()),
        service.record_account_created(completion.clone()),
    );
    assert!(first.is_ok() || second.is_ok());
    if first == Err(ContinuationError::Retry) {
        service.record_account_created(completion.clone()).await.unwrap();
    }
    if second == Err(ContinuationError::Retry) {
        service.record_account_created(completion).await.unwrap();
    }
    assert_eq!(email_events(&db.pool).await.len(), 4);
    assert_eq!(tasks(&db.pool, "record_account_created").await.len(), 1);
    db.close().await;
}
