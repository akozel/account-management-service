//! Durable delivery ownership, independent of aggregate sequencing.
use super::{ClaimedTask, OutboxError, TaskFormat};
use async_trait::async_trait;
use std::time::Duration;
use uuid::Uuid;

/// Fixed reserve beyond explicit I/O and invocation budgets.
pub const LEASE_SAFETY_MARGIN: Duration = Duration::from_secs(5);

/// Requested transition; storage may reject it or exhaust the attempt budget.
#[derive(Clone, Debug, PartialEq)]
pub enum SettlementRequest {
    Completed,
    Retry { delay: Duration, reason: String },
    Failed { reason: String },
}

/// Fixed safe diagnostics, never a handler's arbitrary error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskFailureCause {
    PermanentRejection,
    AttemptLimitReached,
    LeaseExpired,
}
impl TaskFailureCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PermanentRejection => "permanent failure",
            Self::AttemptLimitReached => "retry limit reached",
            Self::LeaseExpired => "worker lease expired",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskFailure {
    pub id: Uuid,
    pub format: TaskFormat,
    pub attempt: i32,
    pub cause: TaskFailureCause,
}

/// Only confirmed transitions are returned, never before transaction commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SettlementResult {
    OwnershipLost,
    Completed,
    RetryScheduled,
    Failed(TaskFailure),
}
impl SettlementResult {
    /// Applied also includes a durable retry, not just completed work.
    pub fn is_applied(&self) -> bool {
        !matches!(self, Self::OwnershipLost)
    }
    pub fn failure(&self) -> Option<&TaskFailure> {
        match self {
            Self::Failed(failure) => Some(failure),
            _ => None,
        }
    }
}

/// Recovery report after commit. Total is derived, not a redundant counter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LeaseRecoveryReport {
    retried_count: usize,
    failures: Vec<TaskFailure>,
}
impl LeaseRecoveryReport {
    pub fn new(retried_count: usize, failures: Vec<TaskFailure>) -> Self {
        Self { retried_count, failures }
    }
    pub fn processed_count(&self) -> usize {
        self.retried_count + self.failures.len()
    }
    pub fn retried_count(&self) -> usize {
        self.retried_count
    }
    pub fn failures(&self) -> &[TaskFailure] {
        &self.failures
    }
}

/// Adapters atomically claim ready work, increment attempts and issue fresh tokens.
/// Anchor local lease time before requesting ownership. Fence settlement by token
/// AND database expiry; cancellation may leave an unknown committed result.
/// Durable recovery must retain abandoned work. No FIFO or exactly-once effects.
#[async_trait]
pub trait OutboxQueue: Send + Sync {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<ClaimedTask>, OutboxError>;
    async fn settle(&self, task: &ClaimedTask, request: SettlementRequest) -> Result<SettlementResult, OutboxError>;
    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, OutboxError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn confirmed_results_and_recovery_counts_are_unambiguous() {
        let failure = TaskFailure {
            id: Uuid::new_v4(),
            format: TaskFormat::new("test", 1).unwrap(),
            attempt: 1,
            cause: TaskFailureCause::AttemptLimitReached,
        };
        for result in [SettlementResult::Completed, SettlementResult::RetryScheduled] {
            assert!(result.is_applied());
            assert!(result.failure().is_none());
        }
        assert!(!SettlementResult::OwnershipLost.is_applied());
        assert!(SettlementResult::OwnershipLost.failure().is_none());
        assert_eq!(SettlementResult::Failed(failure.clone()).failure(), Some(&failure));
        let report = LeaseRecoveryReport::new(2, vec![failure]);
        assert_eq!(report.processed_count(), 3);
        assert_eq!(report.retried_count(), 2);
    }
}
