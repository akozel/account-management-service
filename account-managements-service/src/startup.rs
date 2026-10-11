//! Executable error reporting, dotenv loading and caller-owned pool cleanup.

use std::{fmt, future::Future, io, path::PathBuf};

use cqrs_runtime::{host::HostError, logging::LoggingInitError, worker::WorkerRegistryError};
use sqlx::PgPool;
use thiserror::Error;

use crate::config::ServiceConfigError;

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
    #[error("unable to prepare outbox workers")]
    OutboxPreparation(#[from] WorkerRegistryError),
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
            Self::OutboxPreparation(_) => "outbox_preparation",
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

pub(crate) fn load_dotenv() -> Result<(), ServiceError> {
    load_dotenv_with(dotenvy::dotenv)
}

fn load_dotenv_with(load: impl FnOnce() -> Result<PathBuf, dotenvy::Error>) -> Result<(), ServiceError> {
    match load() {
        Ok(_) => Ok(()),
        Err(error) if error.not_found() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn with_pool_cleanup(
    pool: &PgPool,
    work: impl Future<Output = Result<(), ServiceError>>,
) -> Result<(), ServiceError> {
    let result = work.await;
    pool.close().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_runtime::{outbox::TaskHandlerRegistryError, worker::OutboxWorkerPoolError};
    use std::{
        error::Error,
        sync::{Arc, Mutex},
    };

    fn preparation_error() -> ServiceError {
        WorkerRegistryError::Build {
            worker_index: 0,
            source: TaskHandlerRegistryError::Empty,
        }
        .into()
    }

    #[test]
    fn dotenv_absence_is_optional_other_failures_are_typed_and_redacted() {
        assert!(load_dotenv_with(|| Ok(PathBuf::from(".env"))).is_ok());
        assert!(load_dotenv_with(|| Err(dotenvy::Error::Io(io::ErrorKind::NotFound.into()))).is_ok());
        for source in [
            dotenvy::Error::Io(io::Error::new(io::ErrorKind::PermissionDenied, "private_path")),
            dotenvy::Error::LineParse("DATABASE_URL=private_password".into(), 4),
        ] {
            let error = load_dotenv_with(|| Err(source)).unwrap_err();
            assert!(matches!(error, ServiceError::Dotenv(_)));
            assert!(error.source().is_some());
            assert!(!format!("{error:?} {error}").contains("private_"));
        }
    }

    #[tokio::test]
    async fn cleanup_waits_for_work_and_preserves_success_and_startup_or_host_failure() {
        for result in [
            Ok(()),
            Err(preparation_error()),
            Err(ServiceError::Bind(io::Error::other("private_bind_source"))),
            Err(ServiceError::Host(HostError::InboundStopped)),
        ] {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .unwrap();
            let check = pool.clone();
            let expected = result.as_ref().err().map(ToString::to_string);
            let actual = with_pool_cleanup(&pool, async move {
                tokio::task::yield_now().await;
                assert!(!check.is_closed(), "cleanup must follow the operation");
                result
            })
            .await;
            assert!(pool.is_closed());
            assert_eq!(actual.as_ref().err().map(ToString::to_string), expected);
        }
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
                ServiceError::Host(HostError::OutboxSupervisor(OutboxWorkerPoolError::Registry(
                    WorkerRegistryError::Build {
                        worker_index: 2,
                        source: TaskHandlerRegistryError::DuplicateFormat(
                            cqrs_runtime::outbox::TaskFormat::new("private_format\nmail@example.com", 1).unwrap(),
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
        for stage in ["database", "http_bind", "outbox_preparation", "dotenv", "configuration"] {
            assert!(log.contains(stage));
        }
        assert!(log.contains("service stopped with an error"));
        assert!(!log.contains("private_"));
    }
}
