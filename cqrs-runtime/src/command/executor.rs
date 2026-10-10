use super::{AggregateConflictRetryPolicy, CommandGatewayError, retry::retry_on_aggregate_conflict};
use crate::outbox::NewOutboxTask;
use cqrs_es::{Aggregate, AggregateError, CqrsFramework, EventStore};
use std::{error::Error, marker::PhantomData};
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

pub(super) fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::UserError(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_) => CommandGatewayError::Unavailable,
    }
}
