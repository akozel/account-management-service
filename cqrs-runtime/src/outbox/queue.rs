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
    /// Stable durable operation identity; reuse it for redelivery deduplication.
    pub id: Uuid,
    pub format: TaskFormat,
    pub payload: Value,
    pub metadata: Value,
    pub attempt: i32,
    /// Platform ownership credential, changed on claim; not an idempotency key.
    pub lock_token: Uuid,
    /// Conservative, process-local lease deadline on the runtime's monotonic clock.
    /// The queue anchors this BEFORE requesting the database lease, so pool waits,
    /// transport, commit, batch collection, and mailbox delay consume this budget.
    /// Never derive it by comparing database UTC with the worker's wall clock;
    /// it is not persisted and does not replace database token/expiry checks.
    /// Business handlers receive this context but must not modify/reset it.
    pub lease_deadline: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TaskOutcome {
    /// The operation's contract was satisfied, independently of workflow completion.
    Completed,
    /// Return to pending with backoff, or fail if the delivery budget is exhausted.
    Retry { delay: Duration, reason: String },
    /// Stop delivery; a previous attempt may already have performed the effect.
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
    /// Storage rejected stale, expired, or non-processing ownership; no transition confirmed.
    OwnershipLost,
    /// Storage confirmed an outcome, which can be pending, completed, or failed.
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

/// Durable ownership port implemented by infrastructure, not business handlers.
///
/// Implementations must cooperate with cancellation, persist readiness/attempt
/// limits, and fence settlement against both token and expiry. An abandoned
/// storage await may have committed: expiry recovery must retain that work.
/// Returning a claimed task never establishes FIFO or exactly-once effects.
#[async_trait]
pub trait OutboxQueue: Send + Sync {
    /// Atomically claim ready tasks of one format, increment attempts, and issue
    /// fresh tokens. Supply a conservative monotonic deadline anchored before
    /// the lease request, not a new full lease calculated when results arrive.
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<ClaimedTask>, OutboxError>;
    /// Conditionally persist the outcome only for current unexpired ownership.
    /// Return confirmed failure transitions only after commit for safe logging.
    async fn settle(&self, task: &ClaimedTask, outcome: TaskOutcome) -> Result<SettleResult, OutboxError>;
    /// Invalidate expired ownership and schedule retry or terminal failure.
    /// Report confirmed transitions; never perform the business operation here.
    async fn recover_expired(&self, limit: usize) -> Result<RecoveryResult, OutboxError>;
}
