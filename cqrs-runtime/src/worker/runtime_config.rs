use super::{OutboxWorkerPoolConfig, OutboxWorkerPoolConfigError};
use crate::outbox::{OutboxTaskCoordinatorConfig, OutboxTaskCoordinatorConfigError};
use thiserror::Error;

/// Checked coordinator and worker budgets, independent of the queue technology.
/// Defaults do not read env; PostgreSQL statement timeout remains adapter config.
#[derive(Clone, Debug)]
pub struct OutboxRuntimeConfig {
    coordinator: OutboxTaskCoordinatorConfig,
    worker: OutboxWorkerPoolConfig,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OutboxRuntimeConfigError {
    #[error("{0}")]
    Coordinator(#[from] OutboxTaskCoordinatorConfigError),
    #[error("{0}")]
    Worker(#[from] OutboxWorkerPoolConfigError),
}

impl OutboxRuntimeConfig {
    pub fn new(
        coordinator: OutboxTaskCoordinatorConfig,
        worker: OutboxWorkerPoolConfig,
    ) -> Result<Self, OutboxRuntimeConfigError> {
        worker.validate_for_storage_timeout(coordinator.storage_timeout())?;
        Ok(Self { coordinator, worker })
    }

    pub fn coordinator(&self) -> &OutboxTaskCoordinatorConfig {
        &self.coordinator
    }

    pub fn worker(&self) -> &OutboxWorkerPoolConfig {
        &self.worker
    }

    pub(super) fn into_parts(self) -> (OutboxTaskCoordinatorConfig, OutboxWorkerPoolConfig) {
        (self.coordinator, self.worker)
    }

    pub fn from_env() -> Result<Self, OutboxRuntimeConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, OutboxRuntimeConfigError> {
        Self::new(
            OutboxTaskCoordinatorConfig::from_lookup(&mut lookup)?,
            OutboxWorkerPoolConfig::from_lookup(&mut lookup)?,
        )
    }
}

impl Default for OutboxRuntimeConfig {
    fn default() -> Self {
        Self::new(OutboxTaskCoordinatorConfig::default(), OutboxWorkerPoolConfig::default())
            .expect("valid default outbox runtime configuration")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn lookup(settings: &[(&str, &str)]) -> Result<OutboxRuntimeConfig, OutboxRuntimeConfigError> {
        OutboxRuntimeConfig::from_lookup(|name| {
            settings
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        })
    }

    #[test]
    fn combined_defaults_and_independent_overrides_use_existing_settings_once() {
        let default = OutboxRuntimeConfig::default();
        assert_eq!(default.worker().concurrency(), 128);
        assert_eq!(default.worker().queue_capacity(), 128);
        assert_eq!(default.worker().poll_interval(), Duration::from_secs(1));
        assert_eq!(default.worker().handler_timeout(), Duration::from_secs(15));
        assert_eq!(default.worker().lease_duration(), Duration::from_secs(60));
        assert_eq!(default.coordinator().storage_timeout(), Duration::from_secs(6));
        let mut reads = Vec::new();
        OutboxRuntimeConfig::from_lookup(|name| {
            reads.push(name.to_owned());
            None
        })
        .unwrap();
        assert_eq!(reads.len(), 6);
        reads.sort_unstable();
        reads.dedup();
        assert_eq!(reads.len(), 6);
        let c = lookup(&[("OUTBOX_WORKER_CONCURRENCY", "256")]).unwrap();
        assert_eq!(c.worker().queue_capacity(), 128);
        assert_eq!(c.worker().concurrency(), 256);
        let q = lookup(&[("OUTBOX_QUEUE_CAPACITY", "256"), ("OUTBOX_LEASE_SECS", "75")]).unwrap();
        assert_eq!(q.worker().concurrency(), 128);
        assert_eq!(q.worker().queue_capacity(), 256);
        let storage = lookup(&[("OUTBOX_STORAGE_TIMEOUT_MS", "5000")]).unwrap();
        assert_eq!(storage.coordinator().storage_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn parsing_errors_preserve_the_configuration_boundary_and_source() {
        let error = lookup(&[("OUTBOX_STORAGE_TIMEOUT_MS", "invalid")]).unwrap_err();
        assert_eq!(
            error,
            OutboxRuntimeConfigError::Coordinator(OutboxTaskCoordinatorConfigError::InvalidEnvironmentValue)
        );
        assert!(std::error::Error::source(&error).is_some());
        let error = lookup(&[("OUTBOX_WORKER_CONCURRENCY", "invalid")]).unwrap_err();
        assert_eq!(
            error,
            OutboxRuntimeConfigError::Worker(OutboxWorkerPoolConfigError::InvalidEnvironmentValue(
                "OUTBOX_WORKER_CONCURRENCY"
            ))
        );
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(
            lookup(&[("OUTBOX_STORAGE_TIMEOUT_MS", "0")]).unwrap_err(),
            OutboxRuntimeConfigError::Coordinator(OutboxTaskCoordinatorConfigError::InvalidStorageTimeout)
        );
    }

    #[test]
    fn full_budget_is_checked_at_load_time_with_strict_wave_boundaries() {
        for settings in [vec![("OUTBOX_LEASE_SECS", "53")], vec![("OUTBOX_QUEUE_CAPACITY", "129")]] {
            assert_eq!(
                lookup(&settings).unwrap_err(),
                OutboxRuntimeConfigError::Worker(OutboxWorkerPoolConfigError::LeaseTooShort)
            );
        }
        lookup(&[("OUTBOX_LEASE_SECS", "54")]).unwrap();
        lookup(&[("OUTBOX_QUEUE_CAPACITY", "129"), ("OUTBOX_LEASE_SECS", "75")]).unwrap();
        #[cfg(target_pointer_width = "64")]
        {
            let capacity = (usize::MAX >> 3).to_string();
            assert_eq!(
                lookup(&[("OUTBOX_WORKER_CONCURRENCY", "1"), ("OUTBOX_QUEUE_CAPACITY", &capacity)]).unwrap_err(),
                OutboxRuntimeConfigError::Worker(OutboxWorkerPoolConfigError::LeaseBudgetOverflow)
            );
        }
    }
}
