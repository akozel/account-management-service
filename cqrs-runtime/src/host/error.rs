#[cfg(test)]
use crate::worker::WorkerRegistryError;
use std::{fmt, io};

use crate::worker::OutboxWorkerPoolError;
use thiserror::Error;
use tokio::task::JoinError;
use tracing::error;

/// A signal observation or component failure encountered by the process host.
#[derive(Error)]
pub enum HostError {
    #[error("failed to observe a shutdown signal")]
    ShutdownSignal(#[source] io::Error),
    #[error("inbound adapter failed")]
    Inbound(#[source] io::Error),
    #[error("inbound adapter terminated before shutdown")]
    InboundStopped,
    #[error("outbox supervisor task failed")]
    OutboxSupervisorTask(#[source] JoinError),
    #[error("outbox supervisor failed")]
    OutboxSupervisor(#[source] OutboxWorkerPoolError),
    #[error("outbox supervisor terminated before shutdown")]
    OutboxSupervisorStopped,
}

// Debug is also used by Rust's Result termination report. Never format a
// JoinError directly: both its Debug and Display can reveal the panic payload.
impl fmt::Debug for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShutdownSignal(error) => f.debug_struct("ShutdownSignal").field("kind", &error.kind()).finish(),
            Self::Inbound(error) => f.debug_struct("Inbound").field("kind", &error.kind()).finish(),
            Self::OutboxSupervisor(error) => f.debug_tuple("OutboxSupervisor").field(error).finish(),
            Self::OutboxSupervisorTask(error) => f
                .debug_struct("OutboxSupervisorTask")
                .field("task_id", &error.id())
                .field("panicked", &error.is_panic())
                .field("cancelled", &error.is_cancelled())
                .finish(),
            Self::InboundStopped | Self::OutboxSupervisorStopped => fmt::Display::fmt(self, f),
        }
    }
}

impl HostError {
    /// Log the failure without formatting a supervisor task's panic payload.
    pub fn log(&self, message: &'static str) {
        match self {
            Self::OutboxSupervisorTask(source) => error!(
                error = %self, task_id = %source.id(), panicked = source.is_panic(),
                cancelled = source.is_cancelled(), "{message}"
            ),
            _ => error!(error = ?self, "{message}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn io_and_registry_diagnostics_preserve_sources_without_rendering_private_text() {
        use crate::outbox::{TaskFormat, TaskHandlerRegistryError};

        let (_, log) = crate::diagnostics::test_support::capture(async {
            for error in [
                HostError::Inbound(io::Error::new(io::ErrorKind::PermissionDenied, "private_io_source")),
                HostError::ShutdownSignal(io::Error::other("private_signal_source")),
                HostError::OutboxSupervisor(OutboxWorkerPoolError::Registry(WorkerRegistryError::Build {
                    worker_index: 3,
                    source: TaskHandlerRegistryError::DuplicateFormat(
                        TaskFormat::new("private_format\nmail@example.com", 2).unwrap(),
                    ),
                })),
            ] {
                assert!(std::error::Error::source(&error).is_some());
                assert!(!format!("{error:?} {error}").contains("private_"));
                error.log("additional failure during host shutdown");
            }
        })
        .await;
        assert!(log.contains("PermissionDenied"));
        assert!(log.contains("unknown"));
        assert!(!log.contains("private_"));
        assert!(!log.contains("mail@example.com"));
    }

    #[derive(Clone, Default)]
    struct RecordingWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for RecordingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RecordingWriter {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[tokio::test]
    async fn supervisor_task_diagnostics_and_debug_redact_panic_payloads() {
        let writer = RecordingWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let panic = tokio::spawn(async {
            panic!("private_panic_payload_sentinel");
        })
        .await
        .unwrap_err();
        let task_id = panic.id().to_string();
        let error = HostError::OutboxSupervisorTask(panic);
        tracing::subscriber::with_default(subscriber, || error.log("service stopped with an error"));
        let log = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(log.contains(&task_id));
        assert!(log.contains("panicked=true cancelled=false"));
        assert!(!log.contains("private_panic_payload_sentinel"));
        let debug = format!("{error:?}");
        assert!(debug.contains("panicked: true"));
        assert!(!debug.contains("private_panic_payload_sentinel"));
        // Keep the original error as a source without rendering its payload.
        assert!(std::error::Error::source(&error).is_some());
        let task = tokio::spawn(std::future::pending::<()>());
        task.abort();
        let error = HostError::OutboxSupervisorTask(task.await.unwrap_err());
        assert!(format!("{error:?}").contains("cancelled: true"));
    }
}
