//! Reusable CQRS, event persistence, transactional outbox, and worker runtime.
//!
//! Application implements [`outbox::TaskPayload`] and [`outbox::TaskHandler`].
//! The runtime owns task delivery and leases; aggregates own business sequence.
//! PostgreSQL adapters are under `postgres` when that feature is enabled,
//! and the supervisor is under `worker` when the worker feature is enabled.

pub mod command;
pub mod outbox;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "worker")]
pub mod worker;
