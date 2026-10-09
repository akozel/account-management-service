//! Process-wide structured logging configuration.

use tracing::level_filters::{LevelFilter, ParseLevelFilterError};

pub struct LogingConfig;

impl LogingConfig {
    pub fn level_filter_from_env() -> Result<LevelFilter, ParseLevelFilterError> {
        Self::level_filter_from_lookup(|name| std::env::var(name).ok())
    }

    pub fn default_setup(level_filter: LevelFilter) {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_thread_names(true)
            .with_max_level(level_filter)
            .init();
    }

    fn level_filter_from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<LevelFilter, ParseLevelFilterError> {
        lookup("RUST_LOG").as_deref().unwrap_or("info").parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_level_defaults_to_info_and_accepts_supported_levels() {
        assert_eq!(LogingConfig::level_filter_from_lookup(|_| None).unwrap(), LevelFilter::INFO);

        for (value, expected) in [
            ("off", LevelFilter::OFF),
            ("error", LevelFilter::ERROR),
            ("warn", LevelFilter::WARN),
            ("info", LevelFilter::INFO),
            ("debug", LevelFilter::DEBUG),
            ("trace", LevelFilter::TRACE),
        ] {
            assert_eq!(
                LogingConfig::level_filter_from_lookup(|_| Some(value.to_owned())).unwrap(),
                expected
            );
        }

        assert!(LogingConfig::level_filter_from_lookup(|_| Some("invalid".to_owned())).is_err());
    }
}
