use std::time::Duration;
use thiserror::Error;
const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 4_000;
/// Transaction-local PostgreSQL statement cap, not an end-to-end I/O budget.
#[derive(Clone, Debug)]
pub struct PostgresTaskQueueConfig {
    statement_timeout: Duration,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PostgresTaskQueueConfigError {
    #[error("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS must be an unsigned integer")]
    InvalidEnvironmentValue,
    #[error("statement timeout must be 1..=2147483647 milliseconds")]
    InvalidStatementTimeout,
}
impl Default for PostgresTaskQueueConfig {
    fn default() -> Self {
        Self::new(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS)).expect("valid default statement timeout")
    }
}
impl PostgresTaskQueueConfig {
    pub fn new(statement_timeout: Duration) -> Result<Self, PostgresTaskQueueConfigError> {
        if statement_timeout.as_millis() == 0 || statement_timeout.as_millis() > i32::MAX as u128 {
            return Err(PostgresTaskQueueConfigError::InvalidStatementTimeout);
        }
        Ok(Self { statement_timeout })
    }
    pub fn statement_timeout(&self) -> Duration {
        self.statement_timeout
    }
    pub fn from_env() -> Result<Self, PostgresTaskQueueConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, PostgresTaskQueueConfigError> {
        let millis = crate::env::read_u64(
            &mut lookup,
            "OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS",
            DEFAULT_STATEMENT_TIMEOUT_MS,
        )
        .map_err(|_| PostgresTaskQueueConfigError::InvalidEnvironmentValue)?;
        Self::new(Duration::from_millis(millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_validates_settings_in_an_isolated_process() {
        use crate::env::test_support;
        if let Some(scenario) = test_support::scenario() {
            match scenario.as_str() {
                "defaults" => assert_eq!(
                    PostgresTaskQueueConfig::from_env().unwrap().statement_timeout(),
                    Duration::from_secs(4)
                ),
                "override" => assert_eq!(
                    PostgresTaskQueueConfig::from_env().unwrap().statement_timeout(),
                    Duration::from_millis(125)
                ),
                "invalid" => assert_eq!(
                    PostgresTaskQueueConfig::from_env().unwrap_err(),
                    PostgresTaskQueueConfigError::InvalidEnvironmentValue
                ),
                "zero" | "overflow" => assert_eq!(
                    PostgresTaskQueueConfig::from_env().unwrap_err(),
                    PostgresTaskQueueConfigError::InvalidStatementTimeout
                ),
                _ => panic!("unknown test scenario"),
            }
            return;
        }
        for (scenario, value) in [
            ("defaults", None),
            ("override", Some("125")),
            ("invalid", Some("invalid")),
            ("zero", Some("0")),
            ("overflow", Some("2147483648")),
        ] {
            test_support::isolated(
                concat!(module_path!(), "::from_env_validates_settings_in_an_isolated_process"),
                scenario,
                |child| {
                    if let Some(value) = value {
                        child.env("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS", value);
                    }
                },
            );
        }
    }

    #[test]
    fn statement_timeout_config_validates_defaults_overrides_and_server_range() {
        assert_eq!(
            PostgresTaskQueueConfig::from_lookup(|_| None).unwrap().statement_timeout(),
            Duration::from_secs(4)
        );
        assert_eq!(
            PostgresTaskQueueConfig::from_lookup(|_| Some("125".into()))
                .unwrap()
                .statement_timeout(),
            Duration::from_millis(125)
        );
        assert!(PostgresTaskQueueConfig::from_lookup(|_| Some(i32::MAX.to_string())).is_ok());
        for value in ["0", "invalid", "-1", "2147483648", "18446744073709551616"] {
            assert!(PostgresTaskQueueConfig::from_lookup(|_| Some(value.into())).is_err());
        }
        for statement_timeout in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
            assert!(PostgresTaskQueueConfig::new(statement_timeout).is_err());
        }
    }
}
