//! Individual task ownership and settlement, independent of business sequencing.
//!
//! Eligible tasks from one aggregate may be claimed concurrently, including
//! higher source-event sequences while earlier tasks remain nonterminal.
//! Aggregate state and atomic publication of dependent work establish the
//! workflow order. A lease token protects acknowledgement of one claim;
//! execution can repeat after an operation succeeds without acknowledgement.

use super::{error::OutboxError, task::TaskFormat};
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

/// Fixed reserve for dispatch/scheduling uncertainty beyond the explicit I/O budgets.
pub const LEASE_SAFETY_MARGIN: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct ClaimedTask {
    pub id: Uuid,
    pub format: TaskFormat,
    pub payload: Value,
    pub metadata: Value,
    pub attempt: i32,
    pub lock_token: Uuid,
    /// Conservative, process-local lease deadline on the runtime's monotonic clock.
    /// The queue anchors this BEFORE requesting the database lease, so pool waits,
    /// transport, commit, batch collection, and mailbox delay consume this budget.
    /// Never derive it by comparing database UTC with the worker's wall clock;
    /// it is not persisted and does not replace database token/expiry checks.
    pub lease_deadline: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TaskOutcome {
    Completed,
    Retry { delay: Duration, reason: String },
    Failed { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailedTask {
    pub id: Uuid,
    pub format: TaskFormat,
    pub attempt: i32,
    pub reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SettleResult {
    OwnershipLost,
    Settled { failed: Option<FailedTask> },
}

impl SettleResult {
    pub fn is_settled(&self) -> bool {
        matches!(self, Self::Settled { .. })
    }

    pub fn failed(&self) -> Option<&FailedTask> {
        match self {
            Self::Settled { failed } => failed.as_ref(),
            Self::OwnershipLost => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryResult {
    pub recovered: u64,
    pub failed: Vec<FailedTask>,
}

#[async_trait]
pub trait OutboxQueue: Send + Sync {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<ClaimedTask>, OutboxError>;
    async fn settle(&self, task: &ClaimedTask, outcome: TaskOutcome) -> Result<SettleResult, OutboxError>;
    async fn recover_expired(&self, limit: usize) -> Result<RecoveryResult, OutboxError>;
}
