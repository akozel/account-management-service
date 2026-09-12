use std::{collections::HashMap, sync::Arc};

use account_management_service::{
    domain::{Email, UserAccount, UserAccountCommand, UserAccountError, UserAccountEvent, UserAccountId},
    infrastructure::postgres::event_repository_with_outbox::PostgresEventRepositoryWithOutbox,
};
use chrono::{DateTime, NaiveDate, Utc};
use cqrs_es::{
    AggregateError, CqrsFramework, EventEnvelope, EventStore,
    persist::{EventStoreAggregateContext, PersistedEventRepository, PersistedEventStore},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use tokio::sync::{Barrier, Notify};
use uuid::Uuid;

type Store = PersistedEventStore<PostgresEventRepositoryWithOutbox, UserAccount>;

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
            .with_env_var("POSTGRES_DB", "user_account_test")
            .with_env_var("POSTGRES_USER", "user_account_test")
            .with_env_var("POSTGRES_PASSWORD", "user_account_test")
            .start()
            .await
            .expect("test PostgreSQL starts");
        let host = container.get_host().await.expect("container host");
        let port = container.get_host_port_ipv4(5432.tcp()).await.expect("container port");
        let pool = PgPoolOptions::new()
            .max_connections(12)
            .connect(&format!(
                "postgresql://user_account_test:user_account_test@{host}:{port}/user_account_test"
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

fn id(value: u128) -> UserAccountId {
    UserAccountId::from_uuid(Uuid::from_u128(value))
}

fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}

fn timestamp(day: u32) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&format!("2026-09-{day:02}T12:00:00Z"))
        .unwrap()
        .with_timezone(&Utc)
}

fn command(account_id: UserAccountId, email: &str) -> UserAccountCommand {
    UserAccountCommand::CreateAccount {
        account_id,
        email: Email::parse(email).unwrap(),
        first_name: " Alice ".into(),
        last_name: " Smith ".into(),
        date_of_birth: NaiveDate::from_ymd_opt(1990, 5, 12).unwrap(),
        validation_date: date(14),
        created_at: timestamp(14),
    }
}

fn store(pool: &PgPool) -> Store {
    PersistedEventStore::new_snapshot_store(PostgresEventRepositoryWithOutbox::new(pool.clone()), 1)
}

fn framework(pool: &PgPool) -> CqrsFramework<UserAccount, Store> {
    CqrsFramework::new(store(pool), vec![], ())
}

async fn events(pool: &PgPool, account_id: UserAccountId) -> Vec<cqrs_es::persist::SerializedEvent> {
    PostgresEventRepositoryWithOutbox::new(pool.clone())
        .get_events::<UserAccount>(&account_id.to_string())
        .await
        .unwrap()
}

#[tokio::test]
async fn first_creation_and_exact_retry_keep_one_event_snapshot_and_timestamp() {
    let db = TestDatabase::new().await;
    let account_id = id(1);
    let key = account_id.to_string();
    framework(&db.pool)
        .execute(&key, command(account_id, " Alice@Example.com "))
        .await
        .unwrap();
    let first_events = events(&db.pool, account_id).await;
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].sequence, 1);
    assert_eq!(first_events[0].event_type, "account_created");
    assert_eq!(first_events[0].event_version, "1");
    let UserAccountEvent::AccountCreated {
        account_id: event_id,
        email,
        first_name,
        last_name,
        validation_date,
        created_at,
        ..
    } = serde_json::from_value(first_events[0].payload.clone()).unwrap();
    assert_eq!(event_id, account_id);
    assert_eq!(email.as_str(), "alice@example.com");
    assert_eq!((first_name.as_str(), last_name.as_str()), ("Alice", "Smith"));
    assert_eq!(validation_date, date(14));
    assert_eq!(created_at, timestamp(14));

    let mut retry = command(account_id, "alice@example.com");
    let UserAccountCommand::CreateAccount { created_at, .. } = &mut retry;
    *created_at = timestamp(15);
    assert!(matches!(
        framework(&db.pool).execute(&key, retry).await,
        Err(AggregateError::UserError(UserAccountError::AlreadyCreated))
    ));
    assert_eq!(events(&db.pool, account_id).await, first_events);
    let snapshot_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM snapshots WHERE aggregate_type = 'user_account' AND aggregate_id = $1")
            .bind(&key)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(snapshot_count, 1);

    let snapshot = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .get_snapshot::<UserAccount>(&key)
        .await
        .unwrap()
        .unwrap();
    let saved: UserAccount = serde_json::from_value(snapshot.aggregate).unwrap();
    assert_eq!(saved.created().unwrap().created_at(), timestamp(14));
    let replayed =
        PersistedEventStore::<_, UserAccount>::new_event_store(PostgresEventRepositoryWithOutbox::new(db.pool.clone()))
            .load_aggregate(&key)
            .await
            .unwrap();
    assert_eq!(replayed.aggregate, saved);
    let outbox_count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(outbox_count, 0);
    db.close().await;
}

#[tokio::test]
async fn invalid_first_command_and_different_intent_never_replace_an_account() {
    let db = TestDatabase::new().await;
    let account_id = id(2);
    let key = account_id.to_string();
    let mut invalid = command(account_id, "alice@example.com");
    let UserAccountCommand::CreateAccount { first_name, .. } = &mut invalid;
    *first_name = "  ".into();
    assert!(matches!(
        framework(&db.pool).execute(&key, invalid).await,
        Err(AggregateError::UserError(UserAccountError::EmptyFirstName))
    ));
    assert!(events(&db.pool, account_id).await.is_empty());

    framework(&db.pool)
        .execute(&key, command(account_id, "alice@example.com"))
        .await
        .unwrap();
    assert!(matches!(
        framework(&db.pool)
            .execute(&key, command(account_id, "bob@example.com"))
            .await,
        Err(AggregateError::UserError(UserAccountError::AlreadyExistsWithDifferentData))
    ));
    let mut changed_date = command(account_id, "alice@example.com");
    let UserAccountCommand::CreateAccount { validation_date, .. } = &mut changed_date;
    *validation_date = date(15);
    assert!(matches!(
        framework(&db.pool).execute(&key, changed_date).await,
        Err(AggregateError::UserError(UserAccountError::AlreadyExistsWithDifferentData))
    ));
    assert_eq!(events(&db.pool, account_id).await.len(), 1);
    framework(&db.pool)
        .execute(&id(3).to_string(), command(id(3), "bob@example.com"))
        .await
        .unwrap();
    assert_eq!(events(&db.pool, id(3)).await.len(), 1);
    db.close().await;
}

struct RaceGate {
    loaded: Barrier,
    first_committed: Notify,
}

struct GatedStore {
    inner: Store,
    gate: Arc<RaceGate>,
    first: bool,
}

impl EventStore<UserAccount> for GatedStore {
    type AC = EventStoreAggregateContext<UserAccount>;

    async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<UserAccount>>, AggregateError<UserAccountError>> {
        self.inner.load_events(id).await
    }

    async fn load_aggregate(&self, id: &str) -> Result<Self::AC, AggregateError<UserAccountError>> {
        let context = self.inner.load_aggregate(id).await?;
        self.gate.loaded.wait().await;
        Ok(context)
    }

    async fn commit(
        &self,
        events: Vec<UserAccountEvent>,
        context: Self::AC,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<UserAccount>>, AggregateError<UserAccountError>> {
        if !self.first {
            self.gate.first_committed.notified().await;
        }
        let result = self.inner.commit(events, context, metadata).await;
        if self.first {
            self.gate.first_committed.notify_one();
        }
        result
    }
}

fn gated_framework(pool: &PgPool, gate: Arc<RaceGate>, first: bool) -> CqrsFramework<UserAccount, GatedStore> {
    CqrsFramework::new(
        GatedStore {
            inner: store(pool),
            gate,
            first,
        },
        vec![],
        (),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_creates_have_one_winner_and_retry_uses_aggregate_state() {
    let db = TestDatabase::new().await;
    for (account_id, first_email, second_email) in [
        (id(4), "alice@example.com", "alice@example.com"),
        (id(5), "alice@example.com", "bob@example.com"),
        (id(6), "bob@example.com", "alice@example.com"),
    ] {
        let key = account_id.to_string();
        let gate = Arc::new(RaceGate {
            loaded: Barrier::new(2),
            first_committed: Notify::new(),
        });
        let first = gated_framework(&db.pool, gate.clone(), true);
        let second = gated_framework(&db.pool, gate, false);
        let (winner, loser) = tokio::join!(
            first.execute(&key, command(account_id, first_email)),
            second.execute(&key, command(account_id, second_email)),
        );
        assert!(winner.is_ok());
        assert!(matches!(loser, Err(AggregateError::AggregateConflict)));
        let retry = framework(&db.pool).execute(&key, command(account_id, second_email)).await;
        if second_email == first_email {
            assert!(matches!(
                retry,
                Err(AggregateError::UserError(UserAccountError::AlreadyCreated))
            ));
        } else {
            assert!(matches!(
                retry,
                Err(AggregateError::UserError(UserAccountError::AlreadyExistsWithDifferentData))
            ));
        }
        let committed = events(&db.pool, account_id).await;
        assert_eq!(committed.len(), 1);
        let UserAccountEvent::AccountCreated { email, created_at, .. } =
            serde_json::from_value(committed[0].payload.clone()).unwrap();
        assert_eq!(email.as_str(), first_email);
        assert_eq!(created_at, timestamp(14));
        let snapshot = store(&db.pool).load_aggregate(&key).await.unwrap();
        assert_eq!(snapshot.aggregate.created().unwrap().email().as_str(), first_email);
    }
    let key_seven = id(7).to_string();
    let key_eight = id(8).to_string();
    let first_framework = framework(&db.pool);
    let second_framework = framework(&db.pool);
    let (first, second) = tokio::join!(
        first_framework.execute(&key_seven, command(id(7), "seven@example.com")),
        second_framework.execute(&key_eight, command(id(8), "eight@example.com")),
    );
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert_eq!(events(&db.pool, id(7)).await.len(), 1);
    assert_eq!(events(&db.pool, id(8)).await.len(), 1);
    db.close().await;
}
