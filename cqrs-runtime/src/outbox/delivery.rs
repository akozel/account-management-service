//! Handler-visible data is separate from queue ownership credentials.
use super::{
    TaskFormat, TaskValidationError,
    task::{validate_metadata, validate_payload},
};
use serde_json::Value;
use tokio::time::Instant;
use uuid::Uuid;

/// One delivery attempt's immutable business input. No token or lease clock.
#[derive(Clone, Debug)]
pub struct TaskDelivery {
    id: Uuid,
    format: TaskFormat,
    payload: Value,
    metadata: Value,
    attempt: i32,
}
impl TaskDelivery {
    pub fn new(id: Uuid, format: TaskFormat, payload: Value, metadata: Value, attempt: i32) -> Result<Self, TaskValidationError> {
        validate_payload(&payload)?;
        validate_metadata(&metadata)?;
        if attempt <= 0 {
            return Err(TaskValidationError::InvalidAttempt);
        }
        Ok(Self {
            id,
            format,
            payload,
            metadata,
            attempt,
        })
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn format(&self) -> &TaskFormat {
        &self.format
    }
    pub fn payload(&self) -> &Value {
        &self.payload
    }
    pub fn metadata(&self) -> &Value {
        &self.metadata
    }
    pub fn attempt(&self) -> i32 {
        self.attempt
    }
}

/// Platform-only credential and conservative monotonic deadline.
/// Anchor BEFORE the lease request; never refresh on collection or dispatch.
/// An already-expired deadline is valid context and must remain expired.
/// Database token/expiry checks remain authoritative.
#[derive(Clone, Debug)]
pub struct ClaimLease {
    lock_token: Uuid,
    deadline: Instant,
}
impl ClaimLease {
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

/// Queue/coordinator context; business handlers receive only delivery().
#[derive(Clone, Debug)]
pub struct ClaimedTask {
    delivery: TaskDelivery,
    lease: ClaimLease,
}
impl ClaimedTask {
    pub fn new(delivery: TaskDelivery, lease: ClaimLease) -> Self {
        Self { delivery, lease }
    }
    pub fn delivery(&self) -> &TaskDelivery {
        &self.delivery
    }
    pub fn lease(&self) -> &ClaimLease {
        &self.lease
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn invalid_delivery_is_rejected_and_cloning_preserves_ownership() {
        let format = TaskFormat::new("test", 1).unwrap();
        let build = |payload, metadata, attempt| TaskDelivery::new(Uuid::new_v4(), format.clone(), payload, metadata, attempt);
        assert!(build(json!([]), json!({}), 1).is_err());
        assert!(build(json!({}), json!([]), 1).is_err());
        assert!(build(json!({}), json!({}), 0).is_err());
        let lease = ClaimLease::new(Uuid::new_v4(), Instant::now());
        let task = ClaimedTask::new(build(json!({}), json!({}), 1).unwrap(), lease);
        assert_eq!(task.clone().lease().deadline(), task.lease().deadline());
        assert_eq!(task.clone().lease().lock_token(), task.lease().lock_token());
    }
}
