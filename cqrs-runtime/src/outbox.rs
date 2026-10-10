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

mod coordinator;
mod delivery;
mod error;
mod handler;
mod queue;
mod registry;
mod retry;
mod task;

pub use coordinator::{
    OutboxTaskCoordinator, OutboxTaskCoordinatorConfig, OutboxTaskCoordinatorConfigError, outbox_task_coordinator,
    outbox_task_coordinator_with_config,
};
pub use delivery::{ClaimLease, ClaimedTask, TaskDelivery};
pub use error::{OutboxError, QueueRequestError};
pub use handler::{TaskHandler, TaskHandlerError};
pub use queue::{
    LEASE_SAFETY_MARGIN, LeaseRecoveryReport, OutboxQueue, SettlementRequest, SettlementResult, TaskFailure, TaskFailureCause,
};
pub use registry::{TaskHandlerRegistry, TaskHandlerRegistryError};
pub use retry::redelivery_backoff;
pub use task::{NewOutboxTask, TaskFormat, TaskFormatError, TaskPayload, TaskSchedule, TaskValidationError};
#[cfg(test)]
pub(crate) mod test_support;
