//! Durable ownership transitions implemented by queue adapters.
use crate::task::{
    data::format::TaskFormat,
    queue::{
        error::TaskProcessingError,
        lease::LeasedTask,
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
    },
};
use async_trait::async_trait;
use std::time::Duration;
/// Adapters atomically claim ready work, increment attempts and issue fresh tokens.
/// Anchor local lease time before requesting ownership. Fence settlement by token
/// AND database expiry; cancellation may leave an unknown committed result.
/// Durable recovery must retain abandoned work. No FIFO or exactly-once effects.
#[async_trait]
pub trait TaskQueue: Send + Sync {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError>;
    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError>;
    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError>;
}
