//! Durable work contracts. Payload versions are independent of domain events.
//!
//! Business sequence belongs to aggregate state and accepted transitions.
//! Application commits a dependent task with the event that establishes its
//! prerequisite. Tasks may execute concurrently and in any order, even when
//! they share a source aggregate or event sequence. Source-event sequence is
//! provenance, not a queue ordering barrier.
//!
//! Each handler performs one target command or external effect. A successor
//! committed with a successful command may run before its parent's settlement.
//! Required sender idempotency keys are persisted in payload or task metadata
//! and reused unchanged; queue ownership tokens only protect settlement.

pub mod coordinator;
pub mod error;
pub mod queue;
pub mod task;
pub mod task_handler;
pub mod task_handler_registry;

pub use coordinator::{
    OutboxTaskCoordinator, OutboxTaskCoordinatorConfig, OutboxTaskCoordinatorConfigError, outbox_task_coordinator,
    outbox_task_coordinator_with_config, retry_delay,
};
pub use error::OutboxError;
pub use queue::{ClaimedTask, FailedTask, OutboxQueue, RecoveryResult, SettleResult, TaskOutcome};
pub use task::{NewOutboxTask, Schedule, TaskFormat, TaskPayload};
pub use task_handler::{TaskExecutionError, TaskHandler};
pub use task_handler_registry::{TaskHandlerRegistry, TaskHandlerRegistryError};

#[cfg(test)]
pub(crate) mod test_support {
    use std::{
        collections::VecDeque,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use serde::Serialize;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    #[derive(Serialize)]
    pub(crate) struct DummyTask {
        pub(crate) value: &'static str,
    }

    impl TaskPayload for DummyTask {
        const TASK_TYPE: &'static str = "dummy_task";
        const TASK_VERSION: i32 = 1;
    }

    pub(crate) fn format() -> TaskFormat {
        TaskFormat::of::<DummyTask>()
    }

    pub(crate) fn claimed() -> ClaimedTask {
        ClaimedTask {
            id: Uuid::new_v4(),
            format: format(),
            payload: json!({"value": "runtime"}),
            metadata: json!({"correlation_id": "test"}),
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
        pub(crate) claim_delay: Mutex<Duration>,
    }

    #[async_trait]
    impl OutboxQueue for Queue {
        async fn claim(&self, format: &TaskFormat, _: &str, limit: usize, _: Duration) -> Result<Vec<ClaimedTask>, OutboxError> {
            self.requested.lock().unwrap().push(format.clone());
            let delay = *self.claim_delay.lock().unwrap();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
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

        async fn settle(&self, task: &ClaimedTask, outcome: TaskOutcome) -> Result<SettleResult, OutboxError> {
            self.outcomes.lock().unwrap().push((task.id, outcome));
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
                    id: task.id,
                    format: task.format.clone(),
                    attempt: task.attempt,
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

    pub(crate) struct Handler {
        pub(crate) result: Result<(), TaskExecutionError>,
        pub(crate) hang: bool,
    }

    #[async_trait]
    impl TaskHandler for Handler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }

        async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
            if self.hang {
                std::future::pending::<()>().await;
            }
            self.result.clone()
        }
    }

    pub(crate) fn handler() -> Handler {
        Handler {
            result: Ok(()),
            hang: false,
        }
    }

    #[cfg(feature = "worker")]
    pub(crate) struct CountingHandler(pub(crate) std::sync::Arc<AtomicUsize>);

    #[cfg(feature = "worker")]
    #[async_trait]
    impl TaskHandler for CountingHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }

        async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
}
