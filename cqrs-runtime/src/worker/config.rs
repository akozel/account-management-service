use crate::outbox::LEASE_SAFETY_MARGIN;
use std::time::Duration;
use thiserror::Error;

// Preserve the existing configuration range independently of Tokio internals.
const MAX_QUEUE_CAPACITY: usize = usize::MAX >> 3;

/// Independent process-wide execution and waiting limits, with validated local values.
/// Defaults: C=128, Q=128, poll=1s, handler=30s, lease=120s.
/// Validate the combined queued-wave lease against the actual coordinator.
#[derive(Clone, Debug)]
pub struct OutboxWorkerPoolConfig {
    concurrency: usize,
    queue_capacity: usize,
    poll_interval: Duration,
    handler_timeout: Duration,
    lease_duration: Duration,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OutboxWorkerPoolConfigError {
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
impl Default for OutboxWorkerPoolConfig {
    fn default() -> Self {
        Self {
            concurrency: 128,
            queue_capacity: 128,
            poll_interval: Duration::from_secs(1),
            handler_timeout: Duration::from_secs(30),
            lease_duration: Duration::from_secs(120),
        }
    }
}
impl OutboxWorkerPoolConfig {
    /// Construct explicit limits. Both concurrency and queue capacity must be positive.
    /// Validate the full lease separately with [`Self::validate_for_storage_timeout`].
    pub fn new(
        concurrency: usize,
        queue_capacity: usize,
        poll_interval: Duration,
        handler_timeout: Duration,
        lease_duration: Duration,
    ) -> Result<Self, OutboxWorkerPoolConfigError> {
        if concurrency == 0 {
            return Err(OutboxWorkerPoolConfigError::InvalidConcurrency);
        }
        if queue_capacity == 0 || queue_capacity > MAX_QUEUE_CAPACITY {
            return Err(OutboxWorkerPoolConfigError::InvalidQueueCapacity);
        }
        if poll_interval.is_zero() || poll_interval.as_millis() > i64::MAX as u128 {
            return Err(OutboxWorkerPoolConfigError::InvalidPollInterval);
        }
        if lease_duration.is_zero() || lease_duration.as_millis() > i64::MAX as u128 {
            return Err(OutboxWorkerPoolConfigError::InvalidLeaseDuration);
        }
        if handler_timeout.is_zero() || handler_timeout >= lease_duration {
            return Err(OutboxWorkerPoolConfigError::InvalidHandlerTimeout);
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
    pub fn from_env() -> Result<Self, OutboxWorkerPoolConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, OutboxWorkerPoolConfigError> {
        let mut read_u64 = |name, default| {
            lookup(name)
                .map(|value| value.parse::<u64>())
                .transpose()
                .map_err(|_| OutboxWorkerPoolConfigError::InvalidEnvironmentValue(name))
                .map(|value| value.unwrap_or(default))
        };
        Self::new(
            usize::try_from(read_u64("OUTBOX_WORKER_CONCURRENCY", 128)?)
                .map_err(|_| OutboxWorkerPoolConfigError::InvalidConcurrency)?,
            usize::try_from(read_u64("OUTBOX_QUEUE_CAPACITY", 128)?)
                .map_err(|_| OutboxWorkerPoolConfigError::InvalidQueueCapacity)?,
            Duration::from_millis(read_u64("OUTBOX_POLL_INTERVAL_MS", 1000)?),
            Duration::from_secs(read_u64("OUTBOX_HANDLER_TIMEOUT_SECS", 30)?),
            Duration::from_secs(read_u64("OUTBOX_LEASE_SECS", 120)?),
        )
    }
    /// Conservative healthy-pool budget: lease > 2*S + H + margin + ceil(Q/C)*(H+S).
    /// Queue time consumes the original lease; per-delivery deadline checks remain mandatory.
    pub fn validate_for_storage_timeout(&self, storage_timeout: Duration) -> Result<(), OutboxWorkerPoolConfigError> {
        if storage_timeout.is_zero() {
            return Err(OutboxWorkerPoolConfigError::InvalidStorageTimeout);
        }
        let waves = self.queue_capacity.div_ceil(self.concurrency);
        let invocation = self
            .handler_timeout
            .checked_add(storage_timeout)
            .ok_or(OutboxWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued_nanos = invocation
            .as_nanos()
            .checked_mul(waves as u128)
            .ok_or(OutboxWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued_seconds =
            u64::try_from(queued_nanos / 1_000_000_000).map_err(|_| OutboxWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued = Duration::new(queued_seconds, (queued_nanos % 1_000_000_000) as u32);
        let required = storage_timeout
            .checked_mul(2)
            .and_then(|budget| budget.checked_add(self.handler_timeout))
            .and_then(|budget| budget.checked_add(LEASE_SAFETY_MARGIN))
            .and_then(|budget| budget.checked_add(queued))
            .ok_or(OutboxWorkerPoolConfigError::LeaseBudgetOverflow)?;
        if self.lease_duration <= required {
            return Err(OutboxWorkerPoolConfigError::LeaseTooShort);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_independent_environment_overrides() {
        let default = OutboxWorkerPoolConfig::from_lookup(|_| None).unwrap();
        assert_eq!(default.concurrency(), 128);
        assert_eq!(default.queue_capacity(), 128);
        assert_eq!(default.lease_duration(), Duration::from_secs(120));
        default.validate_for_storage_timeout(Duration::from_secs(5)).unwrap();
        let custom = OutboxWorkerPoolConfig::from_lookup(|name| {
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
            OutboxWorkerPoolConfig::from_lookup(|name| (name == "OUTBOX_WORKER_CONCURRENCY").then(|| "1000".into())).unwrap();
        assert_eq!(only_c.queue_capacity(), 128);
        let only_q = OutboxWorkerPoolConfig::from_lookup(|name| (name == "OUTBOX_QUEUE_CAPACITY").then(|| "256".into())).unwrap();
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
                OutboxWorkerPoolConfig::from_lookup(|name| (name == key).then(|| value.into())).is_err(),
                "{key}={value}"
            );
        }
        let config = |c, q, poll, handler, lease| OutboxWorkerPoolConfig::new(c, q, poll, handler, lease);
        assert_eq!(
            config(
                1,
                0,
                Duration::from_secs(1),
                Duration::from_secs(30),
                Duration::from_secs(120)
            )
            .unwrap_err(),
            OutboxWorkerPoolConfigError::InvalidQueueCapacity
        );
        assert!(config(1, 1, Duration::from_secs(1), Duration::from_secs(30), Duration::MAX).is_err());
        assert!(config(1, 1, Duration::MAX, Duration::from_secs(30), Duration::from_secs(120)).is_err());
    }

    #[test]
    fn queue_waves_budget_boundaries_and_storage_overflow() {
        let build = |c, q, lease| {
            OutboxWorkerPoolConfig::new(
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
                Err(OutboxWorkerPoolConfigError::LeaseTooShort)
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
            Err(OutboxWorkerPoolConfigError::InvalidStorageTimeout)
        );
        assert_eq!(
            build(1, 1, 120).validate_for_storage_timeout(Duration::MAX),
            Err(OutboxWorkerPoolConfigError::LeaseBudgetOverflow)
        );
        assert_eq!(
            build(1, MAX_QUEUE_CAPACITY, 120).validate_for_storage_timeout(Duration::from_secs(5)),
            Err(OutboxWorkerPoolConfigError::LeaseBudgetOverflow)
        );
    }
}
