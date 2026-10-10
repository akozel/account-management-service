use std::time::Duration;
use thiserror::Error;
/// Transaction-local PostgreSQL statement cap, not an end-to-end I/O budget.
#[derive(Clone, Debug)]
pub struct PostgresOutboxQueueConfig {
    statement_timeout: Duration,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PostgresOutboxQueueConfigError {
    #[error("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS must be an unsigned integer")]
    InvalidEnvironmentValue,
    #[error("statement timeout must be 1..=2147483647 milliseconds")]
    InvalidStatementTimeout,
}
impl Default for PostgresOutboxQueueConfig {
    fn default() -> Self {
        Self {
            statement_timeout: Duration::from_secs(4),
        }
    }
}
impl PostgresOutboxQueueConfig {
    pub fn new(statement_timeout: Duration) -> Result<Self, PostgresOutboxQueueConfigError> {
        let config = Self { statement_timeout };
        config.validate()?;
        Ok(config)
    }
    pub fn statement_timeout(&self) -> Duration {
        self.statement_timeout
    }
    pub fn from_env() -> Result<Self, PostgresOutboxQueueConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, PostgresOutboxQueueConfigError> {
        let millis = lookup("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| PostgresOutboxQueueConfigError::InvalidEnvironmentValue)?
            .unwrap_or(4_000);
        Self::new(Duration::from_millis(millis))
    }
    pub fn validate(&self) -> Result<(), PostgresOutboxQueueConfigError> {
        if self.statement_timeout.as_millis() == 0 || self.statement_timeout.as_millis() > i32::MAX as u128 {
            Err(PostgresOutboxQueueConfigError::InvalidStatementTimeout)
        } else {
            Ok(())
        }
    }
}
