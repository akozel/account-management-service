//! Command gateways with bounded optimistic-conflict retry.
use crate::task::data::new_task::NewTask;
use async_trait::async_trait;
use cqrs_es::Aggregate;
use thiserror::Error;
pub mod executor;
pub mod retry;

/// Technical command boundary; the application gateway selects the aggregate ID.
///
/// Implementations own persistence and retry policy without interpreting domain
/// rejections. Tasks belong to this invocation and must not leak into other calls.
#[async_trait]
pub trait CommandExecutor<A>: Send + Sync
where
    A: Aggregate,
    A::Command: Send,
{
    async fn execute(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>>;
}

/// Expected domain rejection, exhausted optimistic conflict, or unavailable storage.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CommandGatewayError<D> {
    #[error("domain command failed a domain rule: {0}")]
    Domain(D),
    #[error("domain command conflicted with a concurrent change")]
    Conflict,
    #[error("command gateway is temporarily unavailable")]
    Unavailable,
}
