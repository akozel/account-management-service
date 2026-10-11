//! Process lifecycle coordination, independent of the inbound transport.
//!
//! Requires the optional `host` feature and a running Tokio runtime. The host
//! owns signal observation, shared cancellation and component draining. The
//! caller constructs adapters and closes external resources after [`run`].
//! Outbox enforces its own drain budget; the host imposes no inbound or cleanup
//! timeout. Dropping a polled host cancels its token and detaches the outbox task
//! for independent drain, without guaranteeing inbound or resource cleanup.

mod error;
mod lifecycle;
mod signal;

pub use error::HostError;
pub use lifecycle::run;
pub use signal::shutdown_signal;
