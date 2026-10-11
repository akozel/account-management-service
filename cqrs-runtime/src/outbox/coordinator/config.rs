use std::time::Duration;
use thiserror::Error;
const DEFAULT_STORAGE_TIMEOUT_MS: u64 = 6_000;
/// Validated end-to-end storage budget. Defaults do not read the environment.
#[derive(Clone, Debug)]
pub struct OutboxTaskCoordinatorConfig {
    storage_timeout: Duration,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OutboxTaskCoordinatorConfigError {
    #[error("OUTBOX_STORAGE_TIMEOUT_MS must be an unsigned integer")]
    InvalidEnvironmentValue,
    #[error("storage timeout must be positive and representable")]
    InvalidStorageTimeout,
}
impl Default for OutboxTaskCoordinatorConfig {
    fn default() -> Self {
        Self::new(Duration::from_millis(DEFAULT_STORAGE_TIMEOUT_MS)).expect("valid default storage timeout")
    }
}
impl OutboxTaskCoordinatorConfig {
    pub fn new(storage_timeout: Duration) -> Result<Self, OutboxTaskCoordinatorConfigError> {
        if storage_timeout.is_zero() || storage_timeout.as_millis() > i64::MAX as u128 {
            return Err(OutboxTaskCoordinatorConfigError::InvalidStorageTimeout);
        }
        Ok(Self { storage_timeout })
    }
    pub fn storage_timeout(&self) -> Duration {
        self.storage_timeout
    }
    pub fn from_env() -> Result<Self, OutboxTaskCoordinatorConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, OutboxTaskCoordinatorConfigError> {
        let millis = crate::env::read_u64(&mut lookup, "OUTBOX_STORAGE_TIMEOUT_MS", DEFAULT_STORAGE_TIMEOUT_MS)
            .map_err(|_| OutboxTaskCoordinatorConfigError::InvalidEnvironmentValue)?;
        Self::new(Duration::from_millis(millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_timeout_config_has_defaults_overrides_and_rejects_invalid_budgets() {
        assert_eq!(
            OutboxTaskCoordinatorConfig::from_lookup(|_| None).unwrap().storage_timeout(),
            Duration::from_secs(6)
        );
        assert_eq!(
            OutboxTaskCoordinatorConfig::from_lookup(|_| Some("750".into()))
                .unwrap()
                .storage_timeout(),
            Duration::from_millis(750)
        );
        for value in ["0", "invalid", "-1", "9223372036854775808", "18446744073709551616"] {
            assert!(OutboxTaskCoordinatorConfig::from_lookup(|_| Some(value.into())).is_err());
        }
        for storage_timeout in [Duration::ZERO, Duration::MAX] {
            assert!(OutboxTaskCoordinatorConfig::new(storage_timeout).is_err());
        }
    }
}
