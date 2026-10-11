//! Explicit process logger setup; importing this module installs nothing.

use std::fmt;

use thiserror::Error;
use tracing::level_filters::{LevelFilter, ParseLevelFilterError};
use tracing_subscriber::util::{SubscriberInitExt, TryInitError};

/// Process log level. Defaults and explicit construction do not read the environment.
#[derive(Clone, Debug)]
pub struct LoggingConfig {
    level_filter: LevelFilter,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self::new(LevelFilter::INFO)
    }
}

impl LoggingConfig {
    pub fn new(level_filter: LevelFilter) -> Self {
        Self { level_filter }
    }

    /// Preserve LevelFilter parsing, including case, numeric aliases and empty input.
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, ParseLevelFilterError> {
        Ok(Self::new(lookup("RUST_LOG").as_deref().unwrap_or("info").parse()?))
    }

    pub fn from_env() -> Result<Self, ParseLevelFilterError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn level_filter(&self) -> LevelFilter {
        self.level_filter
    }

    /// Explicitly install the fmt subscriber and existing log compatibility bridge.
    /// Returns an error when a global subscriber or log logger is already installed.
    pub fn try_init(&self) -> Result<(), LoggingInitError> {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_thread_names(true)
            .with_max_level(self.level_filter)
            .finish()
            .try_init()
            .map_err(|source| LoggingInitError { source })
    }
}

/// Preserves the initialization cause without formatting it implicitly.
#[derive(Error)]
#[error("unable to initialize process logger")]
pub struct LoggingInitError {
    #[source]
    source: TryInitError,
}

impl fmt::Debug for LoggingInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsing_preserves_defaults_levels_aliases_and_rejects_directives() {
        assert_eq!(LoggingConfig::default().level_filter(), LevelFilter::INFO);
        assert_eq!(
            LoggingConfig::from_lookup(|_| None).unwrap().level_filter(),
            LevelFilter::INFO
        );
        for (name, alias, expected) in [
            ("off", "0", LevelFilter::OFF),
            ("error", "1", LevelFilter::ERROR),
            ("warn", "2", LevelFilter::WARN),
            ("info", "3", LevelFilter::INFO),
            ("debug", "4", LevelFilter::DEBUG),
            ("trace", "5", LevelFilter::TRACE),
        ] {
            for value in [name.to_owned(), name.to_uppercase(), alias.to_owned()] {
                assert_eq!(
                    LoggingConfig::from_lookup(|key| {
                        assert_eq!(key, "RUST_LOG");
                        Some(value.clone())
                    })
                    .unwrap()
                    .level_filter(),
                    expected
                );
            }
        }
        assert_eq!(
            LoggingConfig::from_lookup(|_| Some(String::new())).unwrap().level_filter(),
            LevelFilter::ERROR
        );
        for value in ["invalid", "6", "-1", " info ", "cqrs_runtime=debug", "info,sqlx=warn"] {
            assert!(LoggingConfig::from_lookup(|_| Some(value.into())).is_err());
        }
    }
}
