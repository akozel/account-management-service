use super::*;

#[tokio::test]
async fn explicit_batches_commit_once_and_rollback_with_events_and_snapshots() {
    let db = Database::new().await;
    let first = new_task();
    let mut second = new_task();
    second = second
        .with_metadata(json!({"correlation_id": "delivery", "custom": {"worker": true}}))
        .unwrap();
    let repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone()).with_outbox(vec![first.clone(), second.clone()]);
    let events = [event("batch", 1)];
    repo.persist::<TestAggregate>(&events, Some(("batch".into(), json!({"state": "original"}), 1)))
        .await
        .unwrap();
    for msg in [&first, &second] {
        let saved = row(&db.pool, msg.id()).await;
        assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);
        assert_eq!(saved.get::<Value, _>("payload"), *msg.payload());
        assert_eq!(saved.get::<Value, _>("metadata"), *msg.metadata());
        assert_eq!(saved.get::<String, _>("status"), "pending");
        assert_eq!(saved.get::<i32, _>("attempts"), 0);
    }
    // One event may atomically enqueue several explicit tasks.
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let invalid = new_task();
    assert!(invalid.clone().with_max_attempts(0).is_err());
    // Inject a storage rejection without constructing an invalid public DTO.
    sqlx::query("ALTER TABLE transactional_outbox ADD CONSTRAINT reject_invalid_task CHECK (max_attempts < 10) NOT VALID")
        .execute(&db.pool)
        .await
        .unwrap();
    let bad_repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .with_outbox(vec![new_task().with_max_attempts(1).unwrap(), invalid]);
    assert!(
        bad_repo
            .persist::<TestAggregate>(&[event("batch", 2)], Some(("batch".into(), json!({"state": "bad"}), 2)))
            .await
            .is_err()
    );
    sqlx::query("ALTER TABLE transactional_outbox DROP CONSTRAINT reject_invalid_task")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(repo.get_events::<TestAggregate>("batch").await.unwrap().len(), 1);
    assert_eq!(
        repo.get_snapshot::<TestAggregate>("batch").await.unwrap().unwrap().aggregate,
        json!({"state": "original"})
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let fresh = PostgresEventRepositoryWithOutbox::new(db.pool.clone()).with_outbox(vec![new_task()]);
    assert!(matches!(
        fresh
            .persist::<TestAggregate>(&[event("batch", 2)], Some(("batch".into(), json!({}), 4)))
            .await,
        Err(PersistenceError::OptimisticLockError)
    ));
    let next_task = new_task();
    let next_repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone()).with_outbox(vec![next_task.clone()]);
    next_repo
        .persist::<TestAggregate>(&[event("batch", 2)], Some(("batch".into(), json!({"state": "next"}), 2)))
        .await
        .unwrap();
    assert_eq!(row(&db.pool, next_task.id()).await.get::<i64, _>("aggregate_sequence"), 2);
    assert_eq!(
        next_repo
            .get_snapshot::<TestAggregate>("batch")
            .await
            .unwrap()
            .unwrap()
            .aggregate,
        json!({"state": "next"})
    );
    assert!(fresh.persist::<TestAggregate>(&[], None).await.is_err());
    assert!(new_task().scheduled(TaskSchedule::After(Duration::MAX)).is_err());
    // A duplicate task ID is not an optimistic aggregate conflict. The whole
    // statement still rolls its otherwise valid new event back.
    assert!(matches!(
        repo.persist::<TestAggregate>(&[event("duplicate-task", 1)], None).await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(repo.get_events::<TestAggregate>("duplicate-task").await.unwrap().is_empty());
    db.close().await;
}

use cqrs_es::{
    EventEnvelope,
    persist::{ReplayStream, SerializedSnapshot},
};

const AGGREGATE_ID: &str = "outbox-test-aggregate";

#[derive(Default, Serialize, Deserialize)]
struct StreamAggregate;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum TestEvent {
    Recorded { value: String },
}

impl DomainEvent for TestEvent {
    fn event_type(&self) -> String {
        "recorded".to_owned()
    }

    fn event_version(&self) -> String {
        "1.0".to_owned()
    }
}

impl Aggregate for StreamAggregate {
    const TYPE: &'static str = "transactional_outbox_test";

    type Command = ();
    type Event = TestEvent;
    type Error = std::convert::Infallible;
    type Services = ();

    async fn handle(
        &mut self,
        _command: Self::Command,
        _service: &Self::Services,
        _sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn apply(&mut self, _event: Self::Event) {}
}

fn serialized_event(sequence: usize, value: &str) -> SerializedEvent {
    let event = TestEvent::Recorded { value: value.to_owned() };
    SerializedEvent::new(
        AGGREGATE_ID.to_owned(),
        sequence,
        StreamAggregate::TYPE.to_owned(),
        event.event_type(),
        event.event_version(),
        serde_json::to_value(event).expect("test event must serialize"),
        json!({ "correlation_id": format!("correlation-{sequence}") }),
    )
}

async fn collect_stream(stream: &mut ReplayStream) -> Vec<EventEnvelope<StreamAggregate>> {
    let mut events = Vec::new();
    while let Some(event) = stream.next::<StreamAggregate>(&[]).await {
        events.push(event.expect("event stream must not fail"));
    }
    events
}

fn stream_values(events: Vec<EventEnvelope<StreamAggregate>>) -> Vec<(String, usize, TestEvent)> {
    events
        .into_iter()
        .map(|event| (event.aggregate_id, event.sequence, event.payload))
        .collect()
}

fn expected_stream_values(events: &[SerializedEvent]) -> Vec<(String, usize, TestEvent)> {
    events
        .iter()
        .map(|event| {
            (
                event.aggregate_id.clone(),
                event.sequence,
                serde_json::from_value(event.payload.clone()).expect("test event must deserialize"),
            )
        })
        .collect()
}

async fn row_count(pool: &PgPool, table: &str) -> i64 {
    let query = match table {
        "events" => "SELECT COUNT(*) FROM events WHERE aggregate_type = $1 AND aggregate_id = $2",
        "transactional_outbox" => "SELECT COUNT(*) FROM transactional_outbox WHERE aggregate_type = $1 AND aggregate_id = $2",
        _ => panic!("unsupported test table: {table}"),
    };
    sqlx::query_scalar(query)
        .bind(StreamAggregate::TYPE)
        .bind(AGGREGATE_ID)
        .fetch_one(pool)
        .await
        .expect("row count must succeed")
}

#[tokio::test]
async fn persists_events_and_snapshot_without_implicit_outbox() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());
    let events = vec![serialized_event(1, "first")];
    let snapshot = json!({ "state": "recorded" });

    repository
        .persist::<StreamAggregate>(&events, Some((AGGREGATE_ID.to_owned(), snapshot.clone(), 1)))
        .await
        .expect("event batch must persist");

    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);

    assert_eq!(
        repository
            .get_events::<StreamAggregate>(AGGREGATE_ID)
            .await
            .expect("events must load"),
        events.clone()
    );
    assert_eq!(
        repository
            .get_last_events::<StreamAggregate>(AGGREGATE_ID, 0)
            .await
            .expect("events after sequence one must load"),
        events
    );
    assert_eq!(
        repository
            .get_snapshot::<StreamAggregate>(AGGREGATE_ID)
            .await
            .expect("snapshot must load"),
        Some(SerializedSnapshot {
            aggregate_id: AGGREGATE_ID.to_owned(),
            aggregate: snapshot,
            current_sequence: 1,
            current_snapshot: 1,
        })
    );

    let mut aggregate_stream = repository
        .stream_events::<StreamAggregate>(AGGREGATE_ID)
        .await
        .expect("aggregate event stream must start");
    assert_eq!(
        stream_values(collect_stream(&mut aggregate_stream).await),
        expected_stream_values(&events)
    );

    let mut all_events_stream = repository
        .stream_all_events::<StreamAggregate>()
        .await
        .expect("all-events stream must start");
    assert_eq!(
        stream_values(collect_stream(&mut all_events_stream).await),
        expected_stream_values(&events)
    );

    database.close().await;
}

#[tokio::test]
async fn updates_snapshot_for_a_later_event_batch() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());
    let first_snapshot = json!({ "state": "first" });
    let second_snapshot = json!({ "state": "second" });

    repository
        .persist::<StreamAggregate>(
            &[serialized_event(1, "first")],
            Some((AGGREGATE_ID.to_owned(), first_snapshot, 1)),
        )
        .await
        .expect("first snapshot must persist");
    repository
        .persist::<StreamAggregate>(
            &[serialized_event(2, "second")],
            Some((AGGREGATE_ID.to_owned(), second_snapshot.clone(), 2)),
        )
        .await
        .expect("second snapshot must persist");

    assert_eq!(
        repository
            .get_snapshot::<StreamAggregate>(AGGREGATE_ID)
            .await
            .expect("snapshot must load"),
        Some(SerializedSnapshot {
            aggregate_id: AGGREGATE_ID.to_owned(),
            aggregate: second_snapshot,
            current_sequence: 2,
            current_snapshot: 2,
        })
    );
    assert_eq!(row_count(&database.pool, "events").await, 2);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);

    database.close().await;
}

#[tokio::test]
async fn maps_duplicate_event_sequence_to_optimistic_lock_error() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());

    repository
        .persist::<StreamAggregate>(&[serialized_event(1, "original")], None)
        .await
        .expect("initial event must persist");
    let result = repository
        .persist::<StreamAggregate>(&[serialized_event(1, "duplicate")], None)
        .await;

    assert!(matches!(result, Err(PersistenceError::OptimisticLockError)));
    assert_eq!(row_count(&database.pool, "events").await, 1);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);

    database.close().await;
}

#[tokio::test]
async fn rejects_multi_event_snapshot_only_and_tasks_without_event_before_writing() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());

    assert!(
        repository
            .persist::<StreamAggregate>(
                &[serialized_event(1, "first"), serialized_event(2, "second")],
                Some((AGGREGATE_ID.to_owned(), json!({"state": "second"}), 1)),
            )
            .await
            .is_err()
    );
    assert!(
        repository
            .persist::<StreamAggregate>(&[], Some((AGGREGATE_ID.to_owned(), json!({"state": "snapshot"}), 1)))
            .await
            .is_err()
    );
    assert!(
        PostgresEventRepositoryWithOutbox::new(database.pool.clone())
            .with_outbox(vec![new_task()])
            .persist::<StreamAggregate>(&[], None)
            .await
            .is_err()
    );
    assert_eq!(row_count(&database.pool, "events").await, 0);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);
    assert_eq!(repository.get_snapshot::<StreamAggregate>(AGGREGATE_ID).await.unwrap(), None);
    database.close().await;
}

#[tokio::test]
async fn rolls_back_events_and_outbox_when_snapshot_lock_is_stale() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());
    let initial_snapshot = json!({ "state": "initial" });

    repository
        .persist::<StreamAggregate>(
            &[serialized_event(1, "first")],
            Some((AGGREGATE_ID.to_owned(), initial_snapshot.clone(), 1)),
        )
        .await
        .expect("initial snapshot must persist");
    let result = repository
        .persist::<StreamAggregate>(
            &[serialized_event(2, "second")],
            Some((AGGREGATE_ID.to_owned(), json!({ "state": "stale" }), 3)),
        )
        .await;

    assert!(matches!(result, Err(PersistenceError::OptimisticLockError)));
    assert_eq!(row_count(&database.pool, "events").await, 1);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);
    assert_eq!(
        repository
            .get_snapshot::<StreamAggregate>(AGGREGATE_ID)
            .await
            .expect("snapshot must load"),
        Some(SerializedSnapshot {
            aggregate_id: AGGREGATE_ID.to_owned(),
            aggregate: initial_snapshot,
            current_sequence: 1,
            current_snapshot: 1,
        })
    );

    database.close().await;
}
