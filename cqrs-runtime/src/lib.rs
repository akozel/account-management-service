//! Reusable CQRS, event persistence, transactional outbox, and worker runtime.
//!
//! Application implements [`outbox::TaskPayload`] and [`outbox::TaskHandler`].
//! The runtime owns task delivery and leases; aggregates own business sequence.
//! PostgreSQL adapters are under `postgres` when that feature is enabled,
//! the supervisor is under `worker` when the worker feature is enabled, and
//! process lifecycle is under `host` when the optional host feature is enabled.

#[cfg(feature = "host")]
pub mod host;

mod diagnostics;
mod env;

#[cfg(feature = "logging")]
pub mod logging;

pub mod command;
pub mod outbox;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "worker")]
pub mod worker;
