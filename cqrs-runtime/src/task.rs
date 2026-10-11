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

pub mod data;
pub mod executor;
pub mod handling;
pub mod queue;

#[cfg(test)]
pub(crate) mod test_support;
