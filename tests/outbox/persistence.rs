use super::*;

#[tokio::test]
async fn explicit_batches_commit_once_and_rollback_with_events_and_snapshots() {
    let db = Database::new().await;
    let first = new_task();
    let mut second = new_task();
    second.metadata = json!({"correlation_id": "delivery", "custom": {"worker": true}});
    let repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone()).with_outbox(vec![first.clone(), second.clone()]);
    let events = [event("batch", 1), event("batch", 2)];
    repo.persist::<EmailReservation>(&events, Some(("batch".into(), json!({"state": "original"}), 1)))
        .await
        .unwrap();
    for msg in [&first, &second] {
        let saved = row(&db.pool, msg.id).await;
        assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 2);
        assert_eq!(saved.get::<Value, _>("payload"), msg.payload);
        assert_eq!(saved.get::<Value, _>("metadata"), msg.metadata);
        assert_eq!(saved.get::<String, _>("status"), "pending");
        assert_eq!(saved.get::<i32, _>("attempts"), 0);
    }
    // Two events and two explicit tasks, not the Cartesian product.
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let mut invalid = new_task();
    invalid.max_attempts = 0;
    let bad_repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone()).with_outbox(vec![new_task(), invalid]);
    assert!(
        bad_repo
            .persist::<EmailReservation>(&[event("batch", 3)], Some(("batch".into(), json!({"state": "bad"}), 2)))
            .await
            .is_err()
    );
    assert_eq!(repo.get_events::<EmailReservation>("batch").await.unwrap().len(), 2);
    assert_eq!(
        repo.get_snapshot::<EmailReservation>("batch")
            .await
            .unwrap()
            .unwrap()
            .aggregate,
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
            .persist::<EmailReservation>(&[event("batch", 3)], Some(("batch".into(), json!({}), 4)))
            .await,
        Err(PersistenceError::OptimisticLockError)
    ));
    let next_task = new_task();
    let next_repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone()).with_outbox(vec![next_task.clone()]);
    next_repo
        .persist::<EmailReservation>(&[event("batch", 3)], Some(("batch".into(), json!({"state": "next"}), 2)))
        .await
        .unwrap();
    assert_eq!(row(&db.pool, next_task.id).await.get::<i64, _>("aggregate_sequence"), 3);
    assert_eq!(
        next_repo
            .get_snapshot::<EmailReservation>("batch")
            .await
            .unwrap()
            .unwrap()
            .aggregate,
        json!({"state": "next"})
    );
    assert!(fresh.persist::<EmailReservation>(&[], None).await.is_err());
    let mut huge = new_task();
    huge.schedule = Schedule::After(Duration::MAX);
    assert!(
        PostgresEventRepositoryWithOutbox::new(db.pool.clone())
            .with_outbox(vec![huge])
            .persist::<EmailReservation>(&[event("overflow", 1)], None)
            .await
            .is_err()
    );
    // An explicit duplicate ID rolls back its new event, rather than skipping work.
    assert!(
        repo.persist::<EmailReservation>(&[event("duplicate-task", 1)], None)
            .await
            .is_err()
    );
    assert!(
        repo.get_events::<EmailReservation>("duplicate-task")
            .await
            .unwrap()
            .is_empty()
    );
    db.close().await;
}

use cqrs_es::{
    EventEnvelope,
    persist::{ReplayStream, SerializedSnapshot},
};

const AGGREGATE_ID: &str = "outbox-test-aggregate";

#[derive(Default, Serialize, Deserialize)]
struct TestAggregate;

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

impl Aggregate for TestAggregate {
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
        TestAggregate::TYPE.to_owned(),
        event.event_type(),
        event.event_version(),
        serde_json::to_value(event).expect("test event must serialize"),
        json!({ "correlation_id": format!("correlation-{sequence}") }),
    )
}

async fn collect_stream(stream: &mut ReplayStream) -> Vec<EventEnvelope<TestAggregate>> {
    let mut events = Vec::new();
    while let Some(event) = stream.next::<TestAggregate>(&[]).await {
        events.push(event.expect("event stream must not fail"));
    }
    events
}

fn stream_values(events: Vec<EventEnvelope<TestAggregate>>) -> Vec<(String, usize, TestEvent)> {
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
        .bind(TestAggregate::TYPE)
        .bind(AGGREGATE_ID)
        .fetch_one(pool)
        .await
        .expect("row count must succeed")
}

#[tokio::test]
async fn persists_events_and_snapshot_without_implicit_outbox() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());
    let events = vec![serialized_event(1, "first"), serialized_event(2, "second")];
    let snapshot = json!({ "state": "recorded" });

    repository
        .persist::<TestAggregate>(&events, Some((AGGREGATE_ID.to_owned(), snapshot.clone(), 1)))
        .await
        .expect("event batch must persist");

    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);

    assert_eq!(
        repository
            .get_events::<TestAggregate>(AGGREGATE_ID)
            .await
            .expect("events must load"),
        events
    );
    assert_eq!(
        repository
            .get_last_events::<TestAggregate>(AGGREGATE_ID, 1)
            .await
            .expect("events after sequence one must load"),
        vec![events[1].clone()]
    );
    assert_eq!(
        repository
            .get_snapshot::<TestAggregate>(AGGREGATE_ID)
            .await
            .expect("snapshot must load"),
        Some(SerializedSnapshot {
            aggregate_id: AGGREGATE_ID.to_owned(),
            aggregate: snapshot,
            current_sequence: 2,
            current_snapshot: 1,
        })
    );

    let mut aggregate_stream = repository
        .stream_events::<TestAggregate>(AGGREGATE_ID)
        .await
        .expect("aggregate event stream must start");
    assert_eq!(
        stream_values(collect_stream(&mut aggregate_stream).await),
        expected_stream_values(&events)
    );

    let mut all_events_stream = repository
        .stream_all_events::<TestAggregate>()
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
        .persist::<TestAggregate>(
            &[serialized_event(1, "first")],
            Some((AGGREGATE_ID.to_owned(), first_snapshot, 1)),
        )
        .await
        .expect("first snapshot must persist");
    repository
        .persist::<TestAggregate>(
            &[serialized_event(2, "second")],
            Some((AGGREGATE_ID.to_owned(), second_snapshot.clone(), 2)),
        )
        .await
        .expect("second snapshot must persist");

    assert_eq!(
        repository
            .get_snapshot::<TestAggregate>(AGGREGATE_ID)
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
        .persist::<TestAggregate>(&[serialized_event(1, "original")], None)
        .await
        .expect("initial event must persist");
    let result = repository
        .persist::<TestAggregate>(&[serialized_event(1, "duplicate")], None)
        .await;

    assert!(matches!(result, Err(PersistenceError::OptimisticLockError)));
    assert_eq!(row_count(&database.pool, "events").await, 1);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);

    database.close().await;
}

#[tokio::test]
async fn rolls_back_events_and_outbox_when_snapshot_lock_is_stale() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());
    let initial_snapshot = json!({ "state": "initial" });

    repository
        .persist::<TestAggregate>(
            &[serialized_event(1, "first")],
            Some((AGGREGATE_ID.to_owned(), initial_snapshot.clone(), 1)),
        )
        .await
        .expect("initial snapshot must persist");
    let result = repository
        .persist::<TestAggregate>(
            &[serialized_event(2, "second")],
            Some((AGGREGATE_ID.to_owned(), json!({ "state": "stale" }), 3)),
        )
        .await;

    assert!(matches!(result, Err(PersistenceError::OptimisticLockError)));
    assert_eq!(row_count(&database.pool, "events").await, 1);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);
    assert_eq!(
        repository
            .get_snapshot::<TestAggregate>(AGGREGATE_ID)
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
