use crate::task::queue::lease::LEASE_SAFETY_MARGIN;
use std::time::Duration;
use thiserror::Error;

// Preserve the existing configuration range independently of Tokio internals.
const MAX_QUEUE_CAPACITY: usize = usize::MAX >> 3;
const DEFAULT_CONCURRENCY: u64 = 128;
const DEFAULT_QUEUE_CAPACITY: u64 = 128;
const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_HANDLER_TIMEOUT_SECS: u64 = 15;
const DEFAULT_LEASE_SECS: u64 = 60;

/// Independent process-wide execution and waiting limits, with validated local values.
/// Defaults: C=128, Q=128, poll=1s, handler=15s, lease=60s.
/// Validate the combined queued-wave lease against the actual executor.
#[derive(Clone, Debug)]
pub struct TaskWorkerPoolConfig {
    concurrency: usize,
    queue_capacity: usize,
    poll_interval: Duration,
    handler_timeout: Duration,
    lease_duration: Duration,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskWorkerPoolConfigError {
    #[error("invalid integer setting: {0}")]
    InvalidEnvironmentValue(&'static str),
    #[error("worker count must be positive")]
    InvalidConcurrency,
    #[error("queue capacity must be positive and supported by the bounded channel")]
    InvalidQueueCapacity,
    #[error("poll interval must be positive and representable")]
    InvalidPollInterval,
    #[error("handler timeout must be positive and shorter than the lease")]
    InvalidHandlerTimeout,
    #[error("lease duration must be positive and representable")]
    InvalidLeaseDuration,
    #[error("storage timeout must be positive")]
    InvalidStorageTimeout,
    #[error("lease budget overflows")]
    LeaseBudgetOverflow,
    #[error("lease must exceed claim + queued waves + handler + settlement + 5-second safety margin")]
    LeaseTooShort,
}
impl Default for TaskWorkerPoolConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_CONCURRENCY as usize,
            DEFAULT_QUEUE_CAPACITY as usize,
            Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
            Duration::from_secs(DEFAULT_HANDLER_TIMEOUT_SECS),
            Duration::from_secs(DEFAULT_LEASE_SECS),
        )
        .expect("valid default worker pool limits")
    }
}
impl TaskWorkerPoolConfig {
    /// Construct explicit limits. Both concurrency and queue capacity must be positive.
    /// Validate the full lease separately with [`Self::validate_for_storage_timeout`].
    pub fn new(
        concurrency: usize,
        queue_capacity: usize,
        poll_interval: Duration,
        handler_timeout: Duration,
        lease_duration: Duration,
    ) -> Result<Self, TaskWorkerPoolConfigError> {
        if concurrency == 0 {
            return Err(TaskWorkerPoolConfigError::InvalidConcurrency);
        }
        if queue_capacity == 0 || queue_capacity > MAX_QUEUE_CAPACITY {
            return Err(TaskWorkerPoolConfigError::InvalidQueueCapacity);
        }
        if poll_interval.is_zero() || poll_interval.as_millis() > i64::MAX as u128 {
            return Err(TaskWorkerPoolConfigError::InvalidPollInterval);
        }
        if lease_duration.is_zero() || lease_duration.as_millis() > i64::MAX as u128 {
            return Err(TaskWorkerPoolConfigError::InvalidLeaseDuration);
        }
        if handler_timeout.is_zero() || handler_timeout >= lease_duration {
            return Err(TaskWorkerPoolConfigError::InvalidHandlerTimeout);
        }
        Ok(Self {
            concurrency,
            queue_capacity,
            poll_interval,
            handler_timeout,
            lease_duration,
        })
    }
    /// Permanent Tokio workers; each executes one delivery including settlement.
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }
    /// Claimed deliveries waiting locally; independent of running worker count.
    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
    pub fn handler_timeout(&self) -> Duration {
        self.handler_timeout
    }
    pub fn lease_duration(&self) -> Duration {
        self.lease_duration
    }
    pub fn from_env() -> Result<Self, TaskWorkerPoolConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, TaskWorkerPoolConfigError> {
        let mut read_u64 = |name, default| {
            crate::env::read_u64(&mut lookup, name, default).map_err(|_| TaskWorkerPoolConfigError::InvalidEnvironmentValue(name))
        };
        Self::new(
            usize::try_from(read_u64("OUTBOX_WORKER_CONCURRENCY", DEFAULT_CONCURRENCY)?)
                .map_err(|_| TaskWorkerPoolConfigError::InvalidConcurrency)?,
            usize::try_from(read_u64("OUTBOX_QUEUE_CAPACITY", DEFAULT_QUEUE_CAPACITY)?)
                .map_err(|_| TaskWorkerPoolConfigError::InvalidQueueCapacity)?,
            Duration::from_millis(read_u64("OUTBOX_POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL_MS)?),
            Duration::from_secs(read_u64("OUTBOX_HANDLER_TIMEOUT_SECS", DEFAULT_HANDLER_TIMEOUT_SECS)?),
            Duration::from_secs(read_u64("OUTBOX_LEASE_SECS", DEFAULT_LEASE_SECS)?),
        )
    }
    /// Conservative healthy-pool budget: lease > 2*S + H + margin + ceil(Q/C)*(H+S).
    /// Queue time consumes the original lease; per-delivery deadline checks remain mandatory.
    pub fn validate_for_storage_timeout(&self, storage_timeout: Duration) -> Result<(), TaskWorkerPoolConfigError> {
        if storage_timeout.is_zero() {
            return Err(TaskWorkerPoolConfigError::InvalidStorageTimeout);
        }
        let waves = self.queue_capacity.div_ceil(self.concurrency);
        let invocation = self
            .handler_timeout
            .checked_add(storage_timeout)
            .ok_or(TaskWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued_nanos = invocation
            .as_nanos()
            .checked_mul(waves as u128)
            .ok_or(TaskWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued_seconds =
            u64::try_from(queued_nanos / 1_000_000_000).map_err(|_| TaskWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued = Duration::new(queued_seconds, (queued_nanos % 1_000_000_000) as u32);
        let required = storage_timeout
            .checked_mul(2)
            .and_then(|budget| budget.checked_add(self.handler_timeout))
            .and_then(|budget| budget.checked_add(LEASE_SAFETY_MARGIN))
            .and_then(|budget| budget.checked_add(queued))
            .ok_or(TaskWorkerPoolConfigError::LeaseBudgetOverflow)?;
        if self.lease_duration <= required {
            return Err(TaskWorkerPoolConfigError::LeaseTooShort);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_validates_independent_worker_limits_in_an_isolated_process() {
        use crate::env::test_support;
        if let Some(scenario) = test_support::scenario() {
            match scenario.as_str() {
                "defaults" => {
                    let config = TaskWorkerPoolConfig::from_env().unwrap();
                    assert_eq!(config.concurrency(), 128);
                    assert_eq!(config.queue_capacity(), 128);
                }
                "override" => {
                    let config = TaskWorkerPoolConfig::from_env().unwrap();
                    assert_eq!(config.concurrency(), 2);
                    assert_eq!(config.queue_capacity(), 128);
                }
                "invalid" => assert_eq!(
                    TaskWorkerPoolConfig::from_env().unwrap_err(),
                    TaskWorkerPoolConfigError::InvalidEnvironmentValue("OUTBOX_WORKER_CONCURRENCY")
                ),
                "zero" => assert_eq!(
                    TaskWorkerPoolConfig::from_env().unwrap_err(),
                    TaskWorkerPoolConfigError::InvalidConcurrency
                ),
                _ => panic!("unknown test scenario"),
            }
            return;
        }
        for (scenario, value) in [
            ("defaults", None),
            ("override", Some("2")),
            ("invalid", Some("invalid")),
            ("zero", Some("0")),
        ] {
            test_support::isolated(
                concat!(
                    module_path!(),
                    "::from_env_validates_independent_worker_limits_in_an_isolated_process"
                ),
                scenario,
                |child| {
                    if let Some(value) = value {
                        child.env("OUTBOX_WORKER_CONCURRENCY", value);
                    }
                },
            );
        }
    }

    #[test]
    fn defaults_and_independent_environment_overrides() {
        let default = TaskWorkerPoolConfig::from_lookup(|_| None).unwrap();
        assert_eq!(default.concurrency(), 128);
        assert_eq!(default.queue_capacity(), 128);
        assert_eq!(default.lease_duration(), Duration::from_secs(60));
        assert_eq!(default.handler_timeout(), Duration::from_secs(15));
        default.validate_for_storage_timeout(Duration::from_secs(6)).unwrap();
        let custom = TaskWorkerPoolConfig::from_lookup(|name| {
            Some(
                match name {
                    "OUTBOX_WORKER_CONCURRENCY" => "2",
                    "OUTBOX_QUEUE_CAPACITY" => "3",
                    "OUTBOX_POLL_INTERVAL_MS" => "50",
                    "OUTBOX_HANDLER_TIMEOUT_SECS" => "10",
                    "OUTBOX_LEASE_SECS" => "120",
                    _ => unreachable!(),
                }
                .into(),
            )
        })
        .unwrap();
        assert_eq!(custom.concurrency(), 2);
        assert_eq!(custom.queue_capacity(), 3);
        assert_eq!(custom.poll_interval(), Duration::from_millis(50));
        assert_eq!(custom.handler_timeout(), Duration::from_secs(10));
        let only_c =
            TaskWorkerPoolConfig::from_lookup(|name| (name == "OUTBOX_WORKER_CONCURRENCY").then(|| "1000".into())).unwrap();
        assert_eq!(only_c.queue_capacity(), 128);
        let only_q = TaskWorkerPoolConfig::from_lookup(|name| (name == "OUTBOX_QUEUE_CAPACITY").then(|| "256".into())).unwrap();
        assert_eq!(only_q.concurrency(), 128);
    }

    #[test]
    fn invalid_environment_and_structural_values_are_typed() {
        for (key, value) in [
            ("OUTBOX_WORKER_CONCURRENCY", "garbage"),
            ("OUTBOX_WORKER_CONCURRENCY", "0"),
            ("OUTBOX_QUEUE_CAPACITY", "0"),
            ("OUTBOX_QUEUE_CAPACITY", "18446744073709551615"),
            ("OUTBOX_POLL_INTERVAL_MS", "0"),
            ("OUTBOX_HANDLER_TIMEOUT_SECS", "0"),
            ("OUTBOX_HANDLER_TIMEOUT_SECS", "120"),
            ("OUTBOX_LEASE_SECS", "0"),
            ("OUTBOX_LEASE_SECS", "18446744073709551615"),
            ("OUTBOX_POLL_INTERVAL_MS", "18446744073709551615"),
        ] {
            assert!(
                TaskWorkerPoolConfig::from_lookup(|name| (name == key).then(|| value.into())).is_err(),
                "{key}={value}"
            );
        }
        let config = |c, q, poll, handler, lease| TaskWorkerPoolConfig::new(c, q, poll, handler, lease);
        assert_eq!(
            config(
                1,
                0,
                Duration::from_secs(1),
                Duration::from_secs(30),
                Duration::from_secs(120)
            )
            .unwrap_err(),
            TaskWorkerPoolConfigError::InvalidQueueCapacity
        );
        assert!(config(1, 1, Duration::from_secs(1), Duration::from_secs(30), Duration::MAX).is_err());
        assert!(config(1, 1, Duration::MAX, Duration::from_secs(30), Duration::from_secs(120)).is_err());
    }

    #[test]
    fn queue_waves_budget_boundaries_and_storage_overflow() {
        let build = |c, q, lease| {
            TaskWorkerPoolConfig::new(
                c,
                q,
                Duration::from_secs(1),
                Duration::from_secs(30),
                Duration::from_secs(lease),
            )
            .unwrap()
        };
        for (q, required) in [(128, 80), (256, 115), (257, 150)] {
            assert_eq!(
                build(128, q, required).validate_for_storage_timeout(Duration::from_secs(5)),
                Err(TaskWorkerPoolConfigError::LeaseTooShort)
            );
            assert!(
                build(128, q, required + 1)
                    .validate_for_storage_timeout(Duration::from_secs(5))
                    .is_ok()
            );
        }
        assert!(build(2, 3, 120).validate_for_storage_timeout(Duration::from_secs(5)).is_ok());
        assert!(
            build(1000, 1000, 120)
                .validate_for_storage_timeout(Duration::from_secs(5))
                .is_ok()
        );
        assert!(
            build(128, 256, 120)
                .validate_for_storage_timeout(Duration::from_secs(10))
                .is_err()
        );
        assert_eq!(
            build(1, 1, 120).validate_for_storage_timeout(Duration::ZERO),
            Err(TaskWorkerPoolConfigError::InvalidStorageTimeout)
        );
        assert_eq!(
            build(1, 1, 120).validate_for_storage_timeout(Duration::MAX),
            Err(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
        );
        assert_eq!(
            build(1, MAX_QUEUE_CAPACITY, 120).validate_for_storage_timeout(Duration::from_secs(5)),
            Err(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
        );
        // Individually representable durations can overflow multiplication by
        // queue waves, or overflow the final sum even with one queued wave.
        assert_eq!(
            build(1, MAX_QUEUE_CAPACITY, 120).validate_for_storage_timeout(Duration::from_millis(i64::MAX as u64)),
            Err(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
        );
        assert_eq!(
            build(1, 1, 120).validate_for_storage_timeout(Duration::from_secs(u64::MAX / 2)),
            Err(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
        );
    }
    #[test]
    fn default_lease_budget_has_the_expected_boundary_and_rejects_an_extra_queue_wave() {
        let build = |queue, lease| {
            TaskWorkerPoolConfig::new(
                128,
                queue,
                Duration::from_secs(1),
                Duration::from_secs(15),
                Duration::from_secs(lease),
            )
            .unwrap()
        };
        assert_eq!(
            build(128, 53).validate_for_storage_timeout(Duration::from_secs(6)),
            Err(TaskWorkerPoolConfigError::LeaseTooShort)
        );
        build(128, 54).validate_for_storage_timeout(Duration::from_secs(6)).unwrap();
        build(128, 60).validate_for_storage_timeout(Duration::from_secs(6)).unwrap();
        assert_eq!(
            build(256, 60).validate_for_storage_timeout(Duration::from_secs(6)),
            Err(TaskWorkerPoolConfigError::LeaseTooShort)
        );
        build(256, 75).validate_for_storage_timeout(Duration::from_secs(6)).unwrap();
    }
}
