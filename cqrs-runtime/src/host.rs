//! Process lifecycle coordination, independent of the inbound transport.
//!
//! Requires the optional `host` feature and a running Tokio runtime. The host
//! owns signal observation, shared cancellation and component draining. The
//! caller constructs adapters and closes external resources after [`lifecycle::run`].
//! Outbox enforces its own drain budget; the host imposes no inbound or cleanup
//! timeout. Dropping a polled host cancels its token and detaches the outbox task
//! for independent drain, without guaranteeing inbound or resource cleanup.

pub mod error;
pub mod lifecycle;
pub mod signal;
