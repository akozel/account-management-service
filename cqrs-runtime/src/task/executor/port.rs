//! Single-attempt execution boundary for standard or custom executors.
use crate::task::{
    handling::dispatcher::TaskDispatcher,
    queue::{error::TaskProcessingError, lease::LeasedTask, settlement::SettlementResult},
};
use async_trait::async_trait;
use std::time::Duration;
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Effective storage budget used by worker startup to validate the full lease.
    fn storage_timeout(&self) -> Duration;
    /// Apply the invocation and remaining-lease budgets, then settle its outcome.
    /// A settled result can be retry/pending; it is not necessarily completion.
    /// LeaseBudgetExhausted skips unsafe execution/settlement and leaves recovery
    /// to the durable queue. The handler never owns or renews platform leases.
    async fn execute_and_settle(
        &self,
        dispatcher: &mut TaskDispatcher,
        task: LeasedTask,
        handler_timeout: Duration,
    ) -> Result<SettlementResult, TaskProcessingError>;
}
