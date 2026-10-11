#![cfg(all(feature = "postgres", feature = "host"))]

use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use cqrs_es::{
    Aggregate, AggregateContext, AggregateError, CqrsFramework, DomainEvent, EventEnvelope, EventStore, event_sink::EventSink,
};
use cqrs_runtime::{
    command::{CommandGatewayError, executor::CqrsCommandExecutor, retry::AggregateConflictRetryPolicy},
    host::error::HostError,
    postgres::command_factory::{PostgresCommandConfig, command_executor},
    runtime::{self, dispatcher_factory::TaskDispatcherFactory, worker_pool_config::TaskWorkerPoolConfig},
    task::{
        data::{attempt::TaskAttempt, format::TaskFormat},
        executor::default_executor::task_executor,
        handling::{adapter::TaskAdapter, dispatcher::TaskDispatcher, handler::TaskHandlerError},
        queue::{
            error::TaskProcessingError,
            lease::{LeasedTask, TaskLease},
            port::TaskQueue,
            settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult, TaskFailure, TaskFailureCause},
        },
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// One test in a separate executable owns the irreversible process-global logger.
struct RecordingLogger(Mutex<Vec<String>>);
static LOGGER: RecordingLogger = RecordingLogger(Mutex::new(Vec::new()));
impl log::Log for RecordingLogger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push(record.args().to_string());
    }
    fn flush(&self) {}
}

#[derive(Default, Serialize, Deserialize)]
struct ProbeAggregate;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ProbeEvent;
impl DomainEvent for ProbeEvent {
    fn event_type(&self) -> String {
        "recorded".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}
impl Aggregate for ProbeAggregate {
    const TYPE: &'static str = "log_fallback";
    type Command = ();
    type Event = ProbeEvent;
    type Error = io::Error;
    type Services = ();
    async fn handle(&mut self, _: (), _: &(), _: &EventSink<Self>) -> Result<(), io::Error> {
        Ok(())
    }
    fn apply(&mut self, _: ProbeEvent) {}
}
struct ProbeContext(ProbeAggregate);
impl AggregateContext<ProbeAggregate> for ProbeContext {
    fn aggregate(&mut self) -> &mut ProbeAggregate {
        &mut self.0
    }
}
struct UnavailableStore;
impl EventStore<ProbeAggregate> for UnavailableStore {
    type AC = ProbeContext;
    async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<ProbeAggregate>>, AggregateError<io::Error>> {
        Err(AggregateError::UnexpectedError(Box::new(io::Error::other("private_source"))))
    }
    async fn load_aggregate(&self, _: &str) -> Result<ProbeContext, AggregateError<io::Error>> {
        Err(AggregateError::UnexpectedError(Box::new(io::Error::other("private_source"))))
    }
    async fn commit(
        &self,
        _: Vec<ProbeEvent>,
        _: ProbeContext,
        _: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<ProbeAggregate>>, AggregateError<io::Error>> {
        Err(AggregateError::UnexpectedError(Box::new(io::Error::other("private_source"))))
    }
}

fn format() -> TaskFormat {
    TaskFormat::new("private_type\nmail@example.com", 1).unwrap()
}
struct FailingAdapter;
#[async_trait]
impl TaskAdapter for FailingAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }
    async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
        Err(TaskHandlerError::Permanent {
            reason: "private_handler_reason".into(),
        })
    }
}
struct Queue {
    pending: Mutex<Option<LeasedTask>>,
    stop: CancellationToken,
}
#[async_trait]
impl TaskQueue for Queue {
    async fn claim(&self, _: &TaskFormat, _: &str, _: usize, _: Duration) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        Ok(self.pending.lock().unwrap().take().into_iter().collect())
    }
    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        assert_eq!(
            request,
            SettlementRequest::Failed {
                reason: "private_handler_reason".into()
            }
        );
        self.stop.cancel();
        Ok(SettlementResult::Failed(TaskFailure {
            id: task.attempt().id(),
            format: task.attempt().format().clone(),
            attempt: task.attempt().number(),
            cause: TaskFailureCause::PermanentRejection,
        }))
    }
    async fn recover_expired(&self, _: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        Ok(LeaseRecoveryReport::default())
    }
}

#[tokio::test]
async fn diagnostics_fall_back_to_log_without_a_tracing_subscriber_and_redact_private_data() {
    assert!(!tracing::dispatcher::has_been_set());
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    pool.close().await;
    let executor = command_executor::<ProbeAggregate>(pool, PostgresCommandConfig::default());
    assert!(matches!(
        executor.execute("private_id", (), vec![]).await,
        Err(CommandGatewayError::Unavailable)
    ));
    let executor = CqrsCommandExecutor::<ProbeAggregate, _>::new(
        |_| CqrsFramework::new(UnavailableStore, vec![], ()),
        AggregateConflictRetryPolicy::default(),
    );
    assert!(matches!(
        executor.execute("private_id", (), vec![]).await,
        Err(CommandGatewayError::Unavailable)
    ));
    let panic = tokio::spawn(async {
        panic!("private_panic_payload");
    })
    .await
    .unwrap_err();
    let panic_id = panic.id().to_string();
    HostError::TaskRuntimeJoin(panic).log("host stopped");
    let cancelled = tokio::spawn(std::future::pending::<()>());
    cancelled.abort();
    let cancelled = cancelled.await.unwrap_err();
    let cancelled_id = cancelled.id().to_string();
    HostError::TaskRuntimeJoin(cancelled).log("host stopped");

    let stop = CancellationToken::new();
    let id = Uuid::new_v4();
    let queue = Arc::new(Queue {
        pending: Mutex::new(Some(LeasedTask::new(
            TaskAttempt::new(
                id,
                format(),
                json!({"secret": "private_payload"}),
                json!({"secret": "private_metadata"}),
                1,
            )
            .unwrap(),
            TaskLease::new(Uuid::new_v4(), tokio::time::Instant::now() + Duration::from_secs(60)),
        ))),
        stop: stop.clone(),
    });
    let factory: TaskDispatcherFactory = Arc::new(|| TaskDispatcher::try_new(vec![Box::new(FailingAdapter)]));
    let config =
        TaskWorkerPoolConfig::new(1, 1, Duration::from_secs(1), Duration::from_secs(15), Duration::from_secs(60)).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        runtime::supervisor::run(queue.clone(), task_executor(queue), factory, config, stop),
    )
    .await
    .unwrap()
    .unwrap();
    let log = LOGGER.0.lock().unwrap().join("\n");
    assert_eq!(log.matches("command execution failed").count(), 2);
    assert!(log.contains("storage_category=\"pool_closed\""));
    assert!(log.contains("category=\"unexpected\""));
    assert!(log.contains(&panic_id));
    assert!(log.contains(&cancelled_id));
    assert!(log.contains("panicked=true cancelled=false"));
    assert!(log.contains("panicked=false cancelled=true"));
    assert!(log.contains("concurrency=1 queue_capacity=1 lease_seconds=60"));
    assert!(log.contains("task failed"));
    assert!(log.contains(&id.to_string()));
    assert!(log.contains("task_type=unknown task_version=1 attempt=1 reason=permanent failure"));
    assert!(!log.contains("private_"));
    assert!(!log.contains("mail@example.com"));
    assert!(!tracing::dispatcher::has_been_set());
}
