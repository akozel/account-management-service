use super::*;
use cqrs_es::{EventStore, persist::PersistedEventStore};
use cqrs_runtime::{
    command::{AggregateConflictRetryPolicy, CommandGatewayError},
    postgres::{PostgresCommandConfig, command_executor},
};
use thiserror::Error;

#[derive(Default, Serialize, Deserialize)]
struct CommandAggregate {
    values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum CommandEvent {
    Recorded { values: Vec<String> },
}

impl DomainEvent for CommandEvent {
    fn event_type(&self) -> String {
        "recorded".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Clone)]
enum Command {
    Record { value: String, expected_count: usize },
    Reject,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("rejected")]
struct Rejected;

impl Aggregate for CommandAggregate {
    const TYPE: &'static str = "command_factory_test";
    type Command = Command;
    type Event = CommandEvent;
    type Error = Rejected;
    type Services = ();

    async fn handle(&mut self, command: Command, _: &(), sink: &EventSink<Self>) -> Result<(), Rejected> {
        match command {
            Command::Record { value, expected_count } if self.values.len() == expected_count => {
                let mut values = self.values.clone();
                values.push(value);
                sink.write(CommandEvent::Recorded { values }, self).await;
                Ok(())
            }
            _ => Err(Rejected),
        }
    }

    fn apply(&mut self, event: CommandEvent) {
        let CommandEvent::Recorded { values } = event;
        self.values = values;
    }
}

fn config(interval: usize) -> PostgresCommandConfig {
    PostgresCommandConfig::new(interval, AggregateConflictRetryPolicy::default()).unwrap()
}

fn record(value: &str, expected_count: usize) -> Command {
    Command::Record {
        value: value.to_owned(),
        expected_count,
    }
}

#[tokio::test]
async fn factory_publishes_atomic_work_and_preserves_rejections_and_rollbacks() {
    let db = Database::new().await;
    let executor = command_executor::<CommandAggregate>(db.pool.clone(), config(1));
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let task = new_task();
    executor
        .execute("accepted", record("first", 0), vec![task.clone()])
        .await
        .unwrap();
    let events = repository.get_events::<CommandAggregate>("accepted").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, json!({"Recorded": {"values": ["first"]}}));
    assert!(
        repository
            .get_snapshot::<CommandAggregate>("accepted")
            .await
            .unwrap()
            .is_some()
    );
    let saved = row(&db.pool, task.id()).await;
    assert_eq!(saved.get::<String, _>("aggregate_id"), "accepted");
    assert_eq!(saved.get::<String, _>("aggregate_type"), CommandAggregate::TYPE);
    assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);

    let rejected_task = new_task();
    assert_eq!(
        executor
            .execute("rejected", Command::Reject, vec![rejected_task.clone()])
            .await,
        Err(CommandGatewayError::Domain(Rejected))
    );
    assert!(
        repository
            .get_events::<CommandAggregate>("rejected")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repository
            .get_snapshot::<CommandAggregate>("rejected")
            .await
            .unwrap()
            .is_none()
    );
    let rejected_count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE id = $1")
        .bind(rejected_task.id())
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(rejected_count, 0);

    // Reusing an existing task ID fails publication and rolls back event/snapshot.
    assert_eq!(
        executor.execute("rollback", record("uncommitted", 0), vec![task]).await,
        Err(CommandGatewayError::Unavailable)
    );
    assert!(
        repository
            .get_events::<CommandAggregate>("rollback")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repository
            .get_snapshot::<CommandAggregate>("rollback")
            .await
            .unwrap()
            .is_none()
    );

    executor.execute("accepted", record("second", 1), vec![]).await.unwrap();
    assert_eq!(repository.get_events::<CommandAggregate>("accepted").await.unwrap().len(), 2);
    let task_count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(task_count, 1, "events-only execution must not reuse the previous task list");
    db.close().await;
}

#[tokio::test]
async fn shared_executor_isolates_parallel_command_task_lists() {
    let db = Database::new().await;
    let executor = command_executor::<CommandAggregate>(db.pool.clone(), PostgresCommandConfig::default());
    let mut commands = tokio::task::JoinSet::new();
    for index in 0..16 {
        let executor = executor.clone();
        commands.spawn(async move {
            let id = format!("parallel-{index}");
            let task = NewOutboxTask::new(DummyTaskV1 { value: id.clone() }).unwrap();
            executor.execute(&id, record(&id, 0), vec![task.clone()]).await.unwrap();
            (id, task)
        });
    }
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    while let Some(result) = commands.join_next().await {
        let (id, task) = result.unwrap();
        let saved = row(&db.pool, task.id()).await;
        assert_eq!(saved.get::<String, _>("aggregate_id"), id);
        assert_eq!(saved.get::<String, _>("aggregate_type"), CommandAggregate::TYPE);
        assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);
        assert_eq!(&saved.get::<Value, _>("payload"), task.payload());
        let events = repository.get_events::<CommandAggregate>(&id).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, json!({"Recorded": {"values": [id]}}));
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 16);
    db.close().await;
}

#[tokio::test]
async fn configured_snapshot_interval_replays_snapshot_and_later_events() {
    let db = Database::new().await;
    let executor = command_executor::<CommandAggregate>(db.pool.clone(), config(2));
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    executor.execute("snapshot", record("first", 0), vec![]).await.unwrap();
    assert!(
        repository
            .get_snapshot::<CommandAggregate>("snapshot")
            .await
            .unwrap()
            .is_none()
    );
    executor.execute("snapshot", record("second", 1), vec![]).await.unwrap();
    let snapshot = repository
        .get_snapshot::<CommandAggregate>("snapshot")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.current_sequence, 2);
    assert_eq!(snapshot.aggregate, json!({"values": ["first", "second"]}));
    // The third and fourth calls require state from the snapshot, then its tail.
    executor.execute("snapshot", record("third", 2), vec![]).await.unwrap();
    let store = PersistedEventStore::<_, CommandAggregate>::new_snapshot_store(repository, 2);
    let context = store.load_aggregate("snapshot").await.unwrap();
    assert_eq!(context.current_sequence, 3);
    assert_eq!(context.aggregate.values, vec!["first", "second", "third"]);
    executor.execute("snapshot", record("fourth", 3), vec![]).await.unwrap();
    let snapshot = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .get_snapshot::<CommandAggregate>("snapshot")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.current_sequence, 4);
    assert_eq!(snapshot.aggregate, json!({"values": ["first", "second", "third", "fourth"]}));
    db.close().await;
}
