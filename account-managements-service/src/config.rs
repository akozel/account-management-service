//! Binary-owned startup configuration. No database connection or logger installation.

use std::{
    collections::HashMap,
    env::VarError,
    fmt,
    net::{AddrParseError, SocketAddr},
};

use cqrs_runtime::{
    logging::LoggingConfig,
    postgres::task_queue::config::{PostgresTaskQueueConfig, PostgresTaskQueueConfigError},
    runtime::config::{TaskRuntimeConfig, TaskRuntimeConfigError},
};
use sqlx::postgres::PgConnectOptions;
use thiserror::Error;
use tracing::level_filters::ParseLevelFilterError;

const ENV_KEYS: [&str; 10] = [
    "DATABASE_URL",
    "BIND_ADDRESS",
    "RUST_LOG",
    "OUTBOX_STORAGE_TIMEOUT_MS",
    "OUTBOX_WORKER_CONCURRENCY",
    "OUTBOX_QUEUE_CAPACITY",
    "OUTBOX_POLL_INTERVAL_MS",
    "OUTBOX_HANDLER_TIMEOUT_SECS",
    "OUTBOX_LEASE_SECS",
    "OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS",
];

pub(crate) struct ServiceConfig {
    database: PgConnectOptions,
    bind_address: SocketAddr,
    logging: LoggingConfig,
    task_runtime: TaskRuntimeConfig,
    task_queue: PostgresTaskQueueConfig,
}

#[derive(Error)]
pub(crate) enum ServiceConfigError {
    #[error("missing required setting: {0}")]
    Missing(&'static str),
    #[error("non-Unicode setting: {name}")]
    NonUnicode {
        name: &'static str,
        #[source]
        source: VarError,
    },
    #[error("DATABASE_URL must use postgres:// or postgresql://")]
    DatabaseScheme,
    #[error("invalid DATABASE_URL")]
    Database(#[source] sqlx::Error),
    #[error("BIND_ADDRESS must be a numeric IPv4/IPv6 address and port")]
    Bind(#[source] AddrParseError),
    #[error("invalid RUST_LOG level")]
    Logging(#[source] ParseLevelFilterError),
    #[error("invalid task_runtime configuration: {0}")]
    Tasks(#[source] TaskRuntimeConfigError),
    #[error("invalid PostgreSQL task_runtime configuration: {0}")]
    PostgresQueue(#[source] PostgresTaskQueueConfigError),
}

impl fmt::Debug for ServiceConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Debug for ServiceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // PgConnectOptions derives Debug including its password. Never delegate to it.
        f.debug_struct("ServiceConfig")
            .field("database", &"[redacted]")
            .field("bind_address", &self.bind_address)
            .field("logging", &self.logging)
            .field("task_runtime", &self.task_runtime)
            .field("task_queue", &self.task_queue)
            .finish()
    }
}

impl ServiceConfig {
    pub(crate) fn from_env() -> Result<Self, ServiceConfigError> {
        Self::from_environment(|name| std::env::var(name))
    }

    fn from_environment(mut read: impl FnMut(&str) -> Result<String, VarError>) -> Result<Self, ServiceConfigError> {
        let mut settings = HashMap::new();
        for name in ENV_KEYS {
            match read(name) {
                Ok(value) => {
                    settings.insert(name, value);
                }
                Err(VarError::NotPresent) => {}
                Err(source) => return Err(ServiceConfigError::NonUnicode { name, source }),
            }
        }
        Self::from_lookup(|name| settings.get(name).cloned())
    }

    pub(crate) fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, ServiceConfigError> {
        let url = lookup("DATABASE_URL").ok_or(ServiceConfigError::Missing("DATABASE_URL"))?;
        let scheme = url.split_once("://").map(|(scheme, _)| scheme);
        if !scheme.is_some_and(|scheme| scheme.eq_ignore_ascii_case("postgres") || scheme.eq_ignore_ascii_case("postgresql")) {
            return Err(ServiceConfigError::DatabaseScheme);
        }
        let database = url.parse().map_err(ServiceConfigError::Database)?;
        let bind_address = lookup("BIND_ADDRESS")
            .ok_or(ServiceConfigError::Missing("BIND_ADDRESS"))?
            .parse()
            .map_err(ServiceConfigError::Bind)?;
        let logging = LoggingConfig::from_lookup(&mut lookup).map_err(ServiceConfigError::Logging)?;
        let task_runtime = TaskRuntimeConfig::from_lookup(&mut lookup).map_err(ServiceConfigError::Tasks)?;
        let task_queue = PostgresTaskQueueConfig::from_lookup(&mut lookup).map_err(ServiceConfigError::PostgresQueue)?;
        Ok(Self {
            database,
            bind_address,
            logging,
            task_runtime,
            task_queue,
        })
    }

    pub(crate) fn database(&self) -> &PgConnectOptions {
        &self.database
    }
    pub(crate) fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }
    pub(crate) fn logging(&self) -> &LoggingConfig {
        &self.logging
    }
    pub(crate) fn task_runtime(&self) -> &TaskRuntimeConfig {
        &self.task_runtime
    }
    pub(crate) fn task_queue(&self) -> &PostgresTaskQueueConfig {
        &self.task_queue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn config(overrides: &[(&str, &str)]) -> Result<ServiceConfig, ServiceConfigError> {
        ServiceConfig::from_lookup(|name| {
            overrides
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
                .or_else(|| match name {
                    "DATABASE_URL" => Some("postgresql://user:private_password@localhost:5432/service".into()),
                    "BIND_ADDRESS" => Some("127.0.0.1:3000".into()),
                    _ => None,
                })
        })
    }

    #[test]
    fn validates_required_settings_numeric_addresses_and_defaults_without_connecting() {
        assert!(matches!(
            ServiceConfig::from_lookup(|_| None),
            Err(ServiceConfigError::Missing("DATABASE_URL"))
        ));
        assert!(matches!(
            ServiceConfig::from_lookup(|key| (key == "DATABASE_URL").then(|| "postgres://localhost/service".into())),
            Err(ServiceConfigError::Missing("BIND_ADDRESS"))
        ));
        for address in ["127.0.0.1:0", "0.0.0.0:3000", "[::1]:3000"] {
            let config = config(&[("BIND_ADDRESS", address)]).unwrap();
            assert_eq!(config.bind_address(), address.parse::<SocketAddr>().unwrap());
            assert_eq!(config.database().get_host(), "localhost");
            assert_eq!(config.database().get_database(), Some("service"));
            assert_eq!(config.logging().level_filter(), tracing::level_filters::LevelFilter::INFO);
            assert_eq!(config.task_runtime().worker_pool().concurrency(), 128);
            assert_eq!(config.task_runtime().worker_pool().queue_capacity(), 128);
            assert_eq!(config.task_queue().statement_timeout(), std::time::Duration::from_secs(4));
            assert!(!format!("{config:?}").contains("private_password"));
        }
        for address in ["localhost:3000", "", "127.0.0.1", "127.0.0.1:65536"] {
            let error = config(&[("BIND_ADDRESS", address)]).unwrap_err();
            assert!(matches!(error, ServiceConfigError::Bind(_)));
            assert!(error.source().is_some());
        }
    }

    #[test]
    fn url_and_log_errors_preserve_sources_without_exposing_values() {
        for url in [
            "",
            "postgres:private_password",
            "postgresql:/localhost/service",
            "http://private_password@localhost/service",
        ] {
            assert!(matches!(
                config(&[("DATABASE_URL", url)]),
                Err(ServiceConfigError::DatabaseScheme)
            ));
        }
        for url in [
            "postgres://localhost:invalid/private_password",
            "postgres://localhost/service?sslmode=private_password",
        ] {
            let error = config(&[("DATABASE_URL", url)]).unwrap_err();
            assert!(matches!(error, ServiceConfigError::Database(_)));
            assert!(error.source().is_some());
            assert!(!format!("{error:?} {error}").contains("private_password"));
        }
        assert!(matches!(
            config(&[("RUST_LOG", "private_password")]),
            Err(ServiceConfigError::Logging(_))
        ));
        assert_eq!(
            config(&[("RUST_LOG", "5")]).unwrap().logging().level_filter(),
            tracing::level_filters::LevelFilter::TRACE
        );
    }

    #[test]
    fn nested_settings_preserve_units_integer_boundaries_and_lease_validation() {
        let value = config(&[
            ("OUTBOX_WORKER_CONCURRENCY", "2"),
            ("OUTBOX_QUEUE_CAPACITY", "3"),
            ("OUTBOX_LEASE_SECS", "120"),
            ("OUTBOX_STORAGE_TIMEOUT_MS", "7000"),
            ("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS", "5000"),
        ])
        .unwrap();
        assert_eq!(value.task_runtime().worker_pool().concurrency(), 2);
        assert_eq!(value.task_runtime().worker_pool().queue_capacity(), 3);
        assert_eq!(
            value.task_runtime().storage().storage_timeout(),
            std::time::Duration::from_secs(7)
        );
        assert_eq!(value.task_queue().statement_timeout(), std::time::Duration::from_secs(5));
        for (key, bad) in [
            ("OUTBOX_WORKER_CONCURRENCY", "0"),
            ("OUTBOX_QUEUE_CAPACITY", "129"),
            ("OUTBOX_STORAGE_TIMEOUT_MS", "18446744073709551616"),
            ("OUTBOX_LEASE_SECS", "53"),
            ("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS", "0"),
            ("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS", "2147483648"),
        ] {
            let error = config(&[(key, bad)]).unwrap_err();
            assert!(matches!(
                error,
                ServiceConfigError::Tasks(_) | ServiceConfigError::PostgresQueue(_)
            ));
            assert!(error.source().is_some());
        }
    }

    #[test]
    fn environment_snapshot_rejects_non_unicode_without_changing_process_environment() {
        let error = ServiceConfig::from_environment(|name| {
            if name == "RUST_LOG" {
                Err(VarError::NotUnicode("private_env_value".into()))
            } else {
                Err(VarError::NotPresent)
            }
        })
        .unwrap_err();
        assert!(matches!(error, ServiceConfigError::NonUnicode { name: "RUST_LOG", .. }));
        assert!(error.source().is_some());
        assert!(!format!("{error:?} {error}").contains("private_env_value"));
        let config = ServiceConfig::from_environment(|name| match name {
            "DATABASE_URL" => Ok("postgres://localhost/service".into()),
            "BIND_ADDRESS" => Ok("[::1]:3000".into()),
            _ => Err(VarError::NotPresent),
        })
        .unwrap();
        assert!(config.bind_address().is_ipv6());
    }
}
