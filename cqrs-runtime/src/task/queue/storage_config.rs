use std::time::Duration;
use thiserror::Error;
const DEFAULT_STORAGE_TIMEOUT_MS: u64 = 6_000;
/// Validated end-to-end storage budget. Defaults do not read the environment.
#[derive(Clone, Debug)]
pub struct TaskStorageConfig {
    storage_timeout: Duration,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskStorageConfigError {
    #[error("OUTBOX_STORAGE_TIMEOUT_MS must be an unsigned integer")]
    InvalidEnvironmentValue,
    #[error("storage timeout must be positive and representable")]
    InvalidStorageTimeout,
}
impl Default for TaskStorageConfig {
    fn default() -> Self {
        Self::new(Duration::from_millis(DEFAULT_STORAGE_TIMEOUT_MS)).expect("valid default storage timeout")
    }
}
impl TaskStorageConfig {
    pub fn new(storage_timeout: Duration) -> Result<Self, TaskStorageConfigError> {
        if storage_timeout.is_zero() || storage_timeout.as_millis() > i64::MAX as u128 {
            return Err(TaskStorageConfigError::InvalidStorageTimeout);
        }
        Ok(Self { storage_timeout })
    }
    pub fn storage_timeout(&self) -> Duration {
        self.storage_timeout
    }
    pub fn from_env() -> Result<Self, TaskStorageConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, TaskStorageConfigError> {
        let millis = crate::env::read_u64(&mut lookup, "OUTBOX_STORAGE_TIMEOUT_MS", DEFAULT_STORAGE_TIMEOUT_MS)
            .map_err(|_| TaskStorageConfigError::InvalidEnvironmentValue)?;
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
                "defaults" | "non_unicode" => assert_eq!(
                    TaskStorageConfig::from_env().unwrap().storage_timeout(),
                    Duration::from_secs(6)
                ),
                "override" => assert_eq!(
                    TaskStorageConfig::from_env().unwrap().storage_timeout(),
                    Duration::from_millis(750)
                ),
                "invalid" => assert_eq!(
                    TaskStorageConfig::from_env().unwrap_err(),
                    TaskStorageConfigError::InvalidEnvironmentValue
                ),
                "zero" => assert_eq!(
                    TaskStorageConfig::from_env().unwrap_err(),
                    TaskStorageConfigError::InvalidStorageTimeout
                ),
                _ => panic!("unknown test scenario"),
            }
            return;
        }
        for (scenario, value) in [
            ("defaults", None),
            ("override", Some("750")),
            ("invalid", Some("invalid")),
            ("zero", Some("0")),
        ] {
            test_support::isolated(
                concat!(module_path!(), "::from_env_validates_settings_in_an_isolated_process"),
                scenario,
                |child| {
                    if let Some(value) = value {
                        child.env("OUTBOX_STORAGE_TIMEOUT_MS", value);
                    }
                },
            );
        }
        #[cfg(unix)]
        test_support::isolated(
            concat!(module_path!(), "::from_env_validates_settings_in_an_isolated_process"),
            "non_unicode",
            |child| {
                use std::os::unix::ffi::OsStringExt;
                child.env("OUTBOX_STORAGE_TIMEOUT_MS", std::ffi::OsString::from_vec(vec![0xff]));
            },
        );
    }

    #[test]
    fn storage_timeout_config_has_defaults_overrides_and_rejects_invalid_budgets() {
        assert_eq!(
            TaskStorageConfig::from_lookup(|_| None).unwrap().storage_timeout(),
            Duration::from_secs(6)
        );
        assert_eq!(
            TaskStorageConfig::from_lookup(|_| Some("750".into()))
                .unwrap()
                .storage_timeout(),
            Duration::from_millis(750)
        );
        for value in ["0", "invalid", "-1", "9223372036854775808", "18446744073709551616"] {
            assert!(TaskStorageConfig::from_lookup(|_| Some(value.into())).is_err());
        }
        for storage_timeout in [Duration::ZERO, Duration::MAX] {
            assert!(TaskStorageConfig::new(storage_timeout).is_err());
        }
    }
}
