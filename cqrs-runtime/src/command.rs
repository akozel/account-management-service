//! Generic command execution with bounded optimistic-conflict retries.

use std::{error::Error, future::Future, marker::PhantomData, time::Duration};

use cqrs_es::{Aggregate, AggregateError, CqrsFramework, EventStore};
use thiserror::Error;

use crate::outbox::NewOutboxTask;

/// Stable error categories exposed by aggregate-specific command gateways.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CommandGatewayError<D> {
    #[error("domain command failed a domain rule: {0}")]
    Domain(D),
    #[error("domain command conflicted with a concurrent change")]
    Conflict,
    #[error("command gateway is temporarily unavailable")]
    Unavailable,
}

/// Controls the bounded, in-process retry of one unchanged aggregate command.
///
/// `max_attempts` counts the initial execution. Only optimistic aggregate
/// conflicts are retried; failures with an unknown commit outcome are returned.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AggregateConflictRetryPolicy {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
    jitter_ratio: f64,
}

impl AggregateConflictRetryPolicy {
    /// Creates an explicit retry policy. `jitter_ratio` is symmetric, so `0.2`
    /// means ±20%.
    pub fn new(max_attempts: usize, initial_backoff: Duration, max_backoff: Duration, jitter_ratio: f64) -> Self {
        assert!(
            max_attempts > 0,
            "aggregate conflict retry policy requires at least one attempt"
        );
        assert!(initial_backoff <= max_backoff, "initial backoff must not exceed the cap");
        assert!(
            jitter_ratio.is_finite() && (0.0..=1.0).contains(&jitter_ratio),
            "jitter ratio must be finite and between zero and one"
        );
        Self {
            max_attempts,
            initial_backoff,
            max_backoff,
            jitter_ratio,
        }
    }

    fn backoff_bounds(self, retry: usize) -> (Duration, Duration) {
        debug_assert!(retry > 0);
        let exponent = u32::try_from(retry - 1).unwrap_or(u32::MAX);
        let multiplier = 2_u32.checked_pow(exponent).unwrap_or(u32::MAX);
        let backoff = self.initial_backoff.saturating_mul(multiplier).min(self.max_backoff);
        let lower = backoff.mul_f64(1.0 - self.jitter_ratio);
        let upper = backoff.mul_f64(1.0 + self.jitter_ratio).min(self.max_backoff);
        (lower, upper)
    }

    fn backoff(self, retry: usize) -> Duration {
        let (lower, upper) = self.backoff_bounds(retry);
        if lower == upper {
            lower
        } else {
            Duration::from_secs_f64(rand::random_range(lower.as_secs_f64()..=upper.as_secs_f64()))
        }
    }
}

impl Default for AggregateConflictRetryPolicy {
    fn default() -> Self {
        Self::new(3, Duration::from_millis(5), Duration::from_millis(50), 0.2)
    }
}

/// Executes commands for one aggregate type through frameworks supplied by the
/// composition root.
///
/// The framework is built exactly once per [`execute`](Self::execute) call.
/// Every optimistic-conflict attempt reloads aggregate state through that same
/// framework and repeats clones of the original command. The immutable outbox
/// task list is handed to the factory once and therefore cannot be regenerated
/// or changed between attempts.
pub struct CqrsCommandExecutor<A, BuildFramework> {
    build_framework: BuildFramework,
    retry_policy: AggregateConflictRetryPolicy,
    aggregate: PhantomData<fn() -> A>,
}

impl<A, BuildFramework> CqrsCommandExecutor<A, BuildFramework> {
    pub fn new(build_framework: BuildFramework, retry_policy: AggregateConflictRetryPolicy) -> Self {
        Self {
            build_framework,
            retry_policy,
            aggregate: PhantomData,
        }
    }
}

impl<A, BuildFramework> CqrsCommandExecutor<A, BuildFramework>
where
    A: Aggregate,
    A::Command: Clone + Send,
{
    pub async fn execute<ES>(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<A::Error>>
    where
        BuildFramework: Fn(Vec<NewOutboxTask>) -> CqrsFramework<A, ES> + Send + Sync,
        ES: EventStore<A> + 'static,
        ES::AC: Send,
    {
        let framework = (self.build_framework)(tasks);
        retry_on_aggregate_conflict(self.retry_policy, || framework.execute(aggregate_id, command.clone()))
            .await
            .map_err(map_aggregate_error)
    }
}

async fn retry_on_aggregate_conflict<T, D, Operation, OperationFuture>(
    policy: AggregateConflictRetryPolicy,
    mut operation: Operation,
) -> Result<T, AggregateError<D>>
where
    D: Error,
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, AggregateError<D>>>,
{
    for attempt in 1..=policy.max_attempts {
        let result = operation().await;
        if matches!(result, Err(AggregateError::AggregateConflict)) && attempt < policy.max_attempts {
            tokio::time::sleep(policy.backoff(attempt)).await;
            continue;
        }
        return result;
    }
    unreachable!("aggregate conflict retry policy always has at least one attempt")
}

fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::UserError(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_) => CommandGatewayError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use cqrs_es::{AggregateContext, DomainEvent, EventEnvelope, event_sink::EventSink};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;
    use crate::outbox::{NewOutboxTask, TaskPayload};

    #[derive(Clone, Debug, Error, Eq, PartialEq)]
    #[error("rejected")]
    struct DummyError;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum DummyCommand {
        Record(String),
        Reject,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    enum DummyEvent {
        Recorded(String),
    }

    impl DomainEvent for DummyEvent {
        fn event_type(&self) -> String {
            "recorded".to_owned()
        }

        fn event_version(&self) -> String {
            "1".to_owned()
        }
    }

    #[derive(Default, Serialize, Deserialize)]
    struct DummyAggregate;

    impl Aggregate for DummyAggregate {
        const TYPE: &'static str = "dummy";
        type Command = DummyCommand;
        type Event = DummyEvent;
        type Error = DummyError;
        type Services = Arc<Mutex<Vec<DummyCommand>>>;

        async fn handle(
            &mut self,
            command: Self::Command,
            seen: &Self::Services,
            sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            seen.lock().unwrap().push(command.clone());
            match command {
                DummyCommand::Record(value) => {
                    sink.write(DummyEvent::Recorded(value), self).await;
                    Ok(())
                }
                DummyCommand::Reject => Err(DummyError),
            }
        }

        fn apply(&mut self, _: Self::Event) {}
    }

    struct DummyContext(DummyAggregate);

    impl AggregateContext<DummyAggregate> for DummyContext {
        fn aggregate(&mut self) -> &mut DummyAggregate {
            &mut self.0
        }
    }

    #[derive(Clone)]
    struct DummyStore {
        commits: Arc<AtomicUsize>,
        conflicts: usize,
        load_error: Option<fn() -> AggregateError<DummyError>>,
    }

    impl EventStore<DummyAggregate> for DummyStore {
        type AC = DummyContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::AC, AggregateError<DummyError>> {
            if let Some(error) = self.load_error {
                return Err(error());
            }
            Ok(DummyContext(DummyAggregate))
        }

        async fn commit(
            &self,
            _: Vec<DummyEvent>,
            _: Self::AC,
            _: HashMap<String, String>,
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            if self.commits.fetch_add(1, Ordering::SeqCst) < self.conflicts {
                Err(AggregateError::AggregateConflict)
            } else {
                Ok(Vec::new())
            }
        }
    }

    #[derive(Serialize)]
    struct DummyTask {
        value: &'static str,
    }

    impl TaskPayload for DummyTask {
        const TASK_TYPE: &'static str = "dummy";
        const TASK_VERSION: i32 = 1;
    }

    fn no_delay_policy(max_attempts: usize) -> AggregateConflictRetryPolicy {
        AggregateConflictRetryPolicy::new(max_attempts, Duration::ZERO, Duration::ZERO, 0.0)
    }

    #[tokio::test]
    async fn executor_builds_once_and_retries_the_unchanged_command_and_tasks() {
        let builds = Arc::new(AtomicUsize::new(0));
        let commits = Arc::new(AtomicUsize::new(0));
        let commands = Arc::new(Mutex::new(Vec::new()));
        let captured_tasks = Arc::new(Mutex::new(Vec::new()));
        let executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
            {
                let builds = builds.clone();
                let commits = commits.clone();
                let commands = commands.clone();
                let captured_tasks = captured_tasks.clone();
                move |tasks| {
                    builds.fetch_add(1, Ordering::SeqCst);
                    captured_tasks.lock().unwrap().push(tasks);
                    CqrsFramework::new(
                        DummyStore {
                            commits: commits.clone(),
                            conflicts: 2,
                            load_error: None,
                        },
                        vec![],
                        commands.clone(),
                    )
                }
            },
            no_delay_policy(3),
        );
        let task = NewOutboxTask::new(DummyTask { value: "same" }).unwrap();

        executor
            .execute("aggregate", DummyCommand::Record("same".to_owned()), vec![task.clone()])
            .await
            .unwrap();

        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(commits.load(Ordering::SeqCst), 3);
        assert_eq!(
            *commands.lock().unwrap(),
            vec![
                DummyCommand::Record("same".to_owned()),
                DummyCommand::Record("same".to_owned()),
                DummyCommand::Record("same".to_owned()),
            ]
        );
        assert_eq!(*captured_tasks.lock().unwrap(), vec![vec![task]]);
    }

    #[tokio::test]
    async fn executor_maps_domain_conflict_and_technical_errors_without_wrong_retries() {
        let domain_executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
            |_| {
                CqrsFramework::new(
                    DummyStore {
                        commits: Arc::default(),
                        conflicts: 0,
                        load_error: None,
                    },
                    vec![],
                    Arc::default(),
                )
            },
            no_delay_policy(3),
        );
        assert_eq!(
            domain_executor.execute("aggregate", DummyCommand::Reject, vec![]).await,
            Err(CommandGatewayError::Domain(DummyError))
        );

        let commits = Arc::new(AtomicUsize::new(0));
        let conflict_executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
            {
                let commits = commits.clone();
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: commits.clone(),
                            conflicts: usize::MAX,
                            load_error: None,
                        },
                        vec![],
                        Arc::default(),
                    )
                }
            },
            no_delay_policy(3),
        );
        assert_eq!(
            conflict_executor
                .execute("aggregate", DummyCommand::Record("value".to_owned()), vec![])
                .await,
            Err(CommandGatewayError::Conflict)
        );
        assert_eq!(commits.load(Ordering::SeqCst), 3);

        for error in [
            || AggregateError::DatabaseConnectionError(Box::new(io::Error::other("connection"))),
            || AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            || AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
        ] {
            let executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: Arc::default(),
                            conflicts: 0,
                            load_error: Some(error),
                        },
                        vec![],
                        Arc::default(),
                    )
                },
                no_delay_policy(3),
            );
            assert_eq!(
                executor
                    .execute("aggregate", DummyCommand::Record("value".to_owned()), vec![])
                    .await,
                Err(CommandGatewayError::Unavailable)
            );
        }
    }

    #[test]
    fn default_policy_has_bounded_exponential_jitter() {
        let policy = AggregateConflictRetryPolicy::default();

        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.backoff_bounds(1), (Duration::from_millis(4), Duration::from_millis(6)));
        assert_eq!(
            policy.backoff_bounds(2),
            (Duration::from_millis(8), Duration::from_millis(12))
        );
        assert_eq!(
            policy.backoff_bounds(5),
            (Duration::from_millis(40), Duration::from_millis(50))
        );
        for retry in 1..=5 {
            let bounds = policy.backoff_bounds(retry);
            for _ in 0..100 {
                assert!((bounds.0..=bounds.1).contains(&policy.backoff(retry)));
            }
        }
    }

    #[test]
    fn maps_every_framework_error_category() {
        assert_eq!(
            map_aggregate_error(AggregateError::UserError(DummyError)),
            CommandGatewayError::Domain(DummyError)
        );
        assert_eq!(
            map_aggregate_error::<DummyError>(AggregateError::AggregateConflict),
            CommandGatewayError::Conflict
        );
        for error in [
            AggregateError::DatabaseConnectionError(Box::new(io::Error::other("db"))),
            AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
        ] {
            assert_eq!(map_aggregate_error::<DummyError>(error), CommandGatewayError::Unavailable);
        }
    }

    #[test]
    fn dummy_task_serializes_without_service_types() {
        let task = NewOutboxTask::new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(task.payload, json!({"value": "runtime"}));
    }

    #[test]
    #[should_panic(expected = "at least one attempt")]
    fn retry_policy_rejects_zero_attempts() {
        AggregateConflictRetryPolicy::new(0, Duration::ZERO, Duration::ZERO, 0.0);
    }
}
