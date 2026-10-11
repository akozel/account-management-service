use std::time::Duration;
use thiserror::Error;
const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 4_000;
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
        Self::new(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS)).expect("valid default statement timeout")
    }
}
impl PostgresOutboxQueueConfig {
    pub fn new(statement_timeout: Duration) -> Result<Self, PostgresOutboxQueueConfigError> {
        if statement_timeout.as_millis() == 0 || statement_timeout.as_millis() > i32::MAX as u128 {
            return Err(PostgresOutboxQueueConfigError::InvalidStatementTimeout);
        }
        Ok(Self { statement_timeout })
    }
    pub fn statement_timeout(&self) -> Duration {
        self.statement_timeout
    }
    pub fn from_env() -> Result<Self, PostgresOutboxQueueConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, PostgresOutboxQueueConfigError> {
        let millis = crate::env::read_u64(
            &mut lookup,
            "OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS",
            DEFAULT_STATEMENT_TIMEOUT_MS,
        )
        .map_err(|_| PostgresOutboxQueueConfigError::InvalidEnvironmentValue)?;
        Self::new(Duration::from_millis(millis))
    }
}
