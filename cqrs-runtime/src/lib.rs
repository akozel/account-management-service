//! Reusable CQRS, event persistence, transactional outbox, and worker runtime.
//!
//! Application implements [`task::data::format::TaskPayload`] and [`task::handling::handler::TaskHandler`].
//! The runtime owns task delivery and leases; aggregates own business sequence.
//! PostgreSQL adapters are under `postgres` when that feature is enabled,
//! the supervisor is under `runtime` when the worker feature is enabled, and
//! process lifecycle is under `host` when the optional host feature is enabled.

#[cfg(feature = "host")]
pub mod host;

mod diagnostics;
mod env;

#[cfg(feature = "logging")]
pub mod logging;

pub mod command;
pub mod task;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "worker")]
pub mod runtime;
