//! Platform ownership token and original conservative deadline.
use crate::task::data::attempt::TaskAttempt;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

/// Fixed reserve beyond explicit I/O and invocation budgets.
pub const LEASE_SAFETY_MARGIN: Duration = Duration::from_secs(5);
/// Platform-only credential and conservative monotonic deadline.
/// Anchor BEFORE the lease request; never refresh on collection or dispatch.
/// An already-expired deadline is valid context and must remain expired.
/// Database token/expiry checks remain authoritative.
#[derive(Clone, Debug)]
pub struct TaskLease {
    lock_token: Uuid,
    deadline: Instant,
}
impl TaskLease {
    pub fn new(lock_token: Uuid, deadline: Instant) -> Self {
        Self { lock_token, deadline }
    }
    pub fn lock_token(&self) -> Uuid {
        self.lock_token
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

/// Platform ownership context; business handlers receive only attempt().
#[derive(Clone, Debug)]
pub struct LeasedTask {
    attempt: TaskAttempt,
    lease: TaskLease,
}
impl LeasedTask {
    pub fn new(attempt: TaskAttempt, lease: TaskLease) -> Self {
        Self { attempt, lease }
    }
    pub fn attempt(&self) -> &TaskAttempt {
        &self.attempt
    }
    pub fn lease(&self) -> &TaskLease {
        &self.lease
    }
}

#[cfg(test)]
mod tests {
    use crate::task::test_support::claimed;

    #[test]
    fn cloning_preserves_ownership() {
        let task = claimed();
        assert_eq!(task.clone().lease().deadline(), task.lease().deadline());
        assert_eq!(task.clone().lease().lock_token(), task.lease().lock_token());
    }
}
