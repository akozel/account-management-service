//! Durable work contracts. Payload versions are independent of domain events.
pub mod coordinator;
pub mod error;
pub mod queue;
pub mod task;
pub mod task_handler;
pub mod task_handler_registry;
pub use coordinator::{OutboxTaskCoordinator, outbox_task_coordinator, retry_delay};
pub use error::OutboxError;
pub use queue::{ClaimedTask, FailedTask, OutboxQueue, RecoveryResult, SettleResult, TaskOutcome};
pub use task::{NewOutboxTask, Schedule, TaskFormat, TaskPayload};
pub use task_handler::{TaskExecutionError, TaskHandler};
pub use task_handler_registry::{TaskHandlerRegistry, TaskHandlerRegistryError};

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::application::email_reservation::tasks::send_verification_code::{
        DeliveryContext, VerificationCodeDelivery, VerificationCodeSender, v1::SendVerificationCodeTaskV1,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use uuid::Uuid;
    pub(crate) fn format() -> TaskFormat {
        TaskFormat::of::<SendVerificationCodeTaskV1>()
    }
    pub(crate) fn claimed() -> ClaimedTask {
        ClaimedTask {
            id: Uuid::new_v4(),
            format: format(),
            payload: json!({"email":"alice@example.com", "account_id":"26e91668-f9fc-4be4-bd9d-321fba42e18b", "verification_code":1234567}),
            metadata: json!({"correlation_id":"test"}),
            attempt: 2,
            lock_token: Uuid::new_v4(),
        }
    }

    #[derive(Default)]
    pub(crate) struct Queue {
        pub(crate) incoming: Mutex<VecDeque<ClaimedTask>>,
        pub(crate) outcomes: Mutex<Vec<(Uuid, TaskOutcome)>>,
        pub(crate) requested: Mutex<Vec<TaskFormat>>,
        pub(crate) claim_error: AtomicBool,
        pub(crate) recovery_error: AtomicBool,
        pub(crate) settle_error: AtomicBool,
        pub(crate) lost_owner: AtomicBool,
        pub(crate) recovered: AtomicUsize,
        pub(crate) hang_claim: AtomicBool,
        pub(crate) hang_recovery: AtomicBool,
        pub(crate) hang_settle: AtomicBool,
        pub(crate) failing_format: Mutex<Option<TaskFormat>>,
    }
    #[async_trait]
    impl OutboxQueue for Queue {
        async fn claim(&self, format: &TaskFormat, _: &str, limit: usize, _: Duration) -> Result<Vec<ClaimedTask>, OutboxError> {
            self.requested.lock().unwrap().push(format.clone());
            if self.hang_claim.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.claim_error.load(Ordering::SeqCst) || self.failing_format.lock().unwrap().as_ref() == Some(format) {
                return Err(OutboxError::Unavailable);
            }
            let mut incoming = self.incoming.lock().unwrap();
            let mut result = Vec::new();
            while result.len() < limit && incoming.front().is_some_and(|message| message.format == *format) {
                result.push(incoming.pop_front().unwrap());
            }
            Ok(result)
        }
        async fn settle(&self, message: &ClaimedTask, outcome: TaskOutcome) -> Result<SettleResult, OutboxError> {
            self.outcomes.lock().unwrap().push((message.id, outcome));
            if self.hang_settle.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.settle_error.load(Ordering::SeqCst) {
                return Err(OutboxError::Unavailable);
            }
            if self.lost_owner.load(Ordering::SeqCst) {
                return Ok(SettleResult::OwnershipLost);
            }
            let failed = match &self.outcomes.lock().unwrap().last().unwrap().1 {
                TaskOutcome::Failed { .. } => Some(FailedTask {
                    id: message.id,
                    format: message.format.clone(),
                    attempt: message.attempt,
                    reason: "permanent failure",
                }),
                _ => None,
            };
            Ok(SettleResult::Settled { failed })
        }
        async fn recover_expired(&self, _: usize) -> Result<RecoveryResult, OutboxError> {
            self.recovered.fetch_add(1, Ordering::SeqCst);
            if self.hang_recovery.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.recovery_error.load(Ordering::SeqCst) {
                return Err(OutboxError::Unavailable);
            }
            Ok(RecoveryResult::default())
        }
    }

    #[derive(Clone)]
    pub(crate) struct Sender {
        pub(crate) records: Arc<Mutex<Vec<(VerificationCodeDelivery, DeliveryContext)>>>,
        pub(crate) result: Result<(), TaskExecutionError>,
        pub(crate) hang: bool,
    }
    #[async_trait]
    impl VerificationCodeSender for Sender {
        async fn send(&mut self, message: VerificationCodeDelivery, context: DeliveryContext) -> Result<(), TaskExecutionError> {
            self.records.lock().unwrap().push((message, context));
            if self.hang {
                std::future::pending::<()>().await;
            }
            self.result.clone()
        }
    }
    pub(crate) fn sender() -> Sender {
        Sender {
            records: Arc::default(),
            result: Ok(()),
            hang: false,
        }
    }
}
