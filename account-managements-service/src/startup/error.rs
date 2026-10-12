//! Typed executable failures with redacted diagnostics.
use crate::config::ServiceConfigError;
use cqrs_runtime::{host::error::HostError, logging::LoggingInitError, runtime::dispatcher_factory::WorkerSetupError};
use std::{fmt, io};
use thiserror::Error;
#[derive(Error)]
pub(crate) enum ServiceError {
    #[error("unable to load dotenv configuration")]
    Dotenv(#[from] dotenvy::Error),
    #[error("{0}")]
    Configuration(#[from] ServiceConfigError),
    #[error("unable to initialize process logger")]
    Logging(#[from] LoggingInitError),
    #[error("unable to connect to PostgreSQL")]
    Database(#[from] sqlx::Error),
    #[error("unable to prepare task runtime")]
    TaskPreparation(#[from] WorkerSetupError),
    #[error("unable to bind HTTP listener")]
    Bind(#[from] io::Error),
    #[error("{0}")]
    Host(#[from] HostError),
}

impl fmt::Debug for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Host(error) => f.debug_tuple("Host").field(error).finish(),
            _ => f
                .debug_struct("ServiceError")
                .field("stage", &self.stage())
                .field("message", &self.to_string())
                .finish(),
        }
    }
}

impl ServiceError {
    fn stage(&self) -> &'static str {
        match self {
            Self::Dotenv(_) => "dotenv",
            Self::Configuration(_) => "configuration",
            Self::Logging(_) => "logging",
            Self::Database(_) => "database",
            Self::TaskPreparation(_) => "task_preparation",
            Self::Bind(_) => "http_bind",
            Self::Host(_) => "host",
        }
    }

    pub(crate) fn log(&self) {
        match self {
            Self::Host(error) => error.log("service stopped with an error"),
            _ => tracing::error!(stage = self.stage(), error = %self, "service startup failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_runtime::{runtime::supervisor::TaskRuntimeError, task::handling::dispatcher::TaskDispatcherError};
    use std::{
        error::Error,
        sync::{Arc, Mutex},
    };

    fn preparation_error() -> ServiceError {
        WorkerSetupError::Build {
            worker_index: 0,
            source: TaskDispatcherError::Empty,
        }
        .into()
    }

    #[derive(Clone, Default)]
    struct Writer(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Writer {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    #[test]
    fn startup_diagnostics_redact_arbitrary_sources_and_keep_stage_and_source() {
        let writer = Writer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            // Prime callsite interest before parallel tests with no subscriber.
            ServiceError::Bind(io::Error::other("prime")).log();
            tracing::callsite::rebuild_interest_cache();
            writer.0.lock().unwrap().clear();
            for error in [
                ServiceError::Database(sqlx::Error::Protocol("private_database_source".into())),
                ServiceError::Bind(io::Error::other("private_bind_source")),
                preparation_error(),
                ServiceError::Dotenv(dotenvy::Error::LineParse("private_env_line".into(), 0)),
                ServiceError::Configuration(ServiceConfigError::Missing("DATABASE_URL")),
                ServiceError::Host(HostError::InboundStopped),
                ServiceError::Host(HostError::Inbound(io::Error::other("private_inbound_source"))),
                ServiceError::Host(HostError::ShutdownSignal(io::Error::other("private_signal_source"))),
                ServiceError::Host(HostError::TaskRuntime(TaskRuntimeError::WorkerSetup(
                    WorkerSetupError::Build {
                        worker_index: 2,
                        source: TaskDispatcherError::DuplicateFormat(
                            cqrs_runtime::task::data::format::TaskFormat::new("private_format\nmail@example.com", 1).unwrap(),
                        ),
                    },
                ))),
            ] {
                assert!(error.source().is_some());
                assert!(!format!("{error:?} {error}").contains("private_"));
                error.log();
            }
        });
        let log = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        for stage in ["database", "http_bind", "task_preparation", "dotenv", "configuration"] {
            assert!(log.contains(stage));
        }
        assert!(log.contains("service stopped with an error"));
        assert!(!log.contains("private_"));
    }
}
