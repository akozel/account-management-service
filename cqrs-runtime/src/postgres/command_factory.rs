use std::{num::NonZeroUsize, sync::Arc};

use cqrs_es::{Aggregate, CqrsFramework, persist::PersistedEventStore};
use sqlx::PgPool;
use thiserror::Error;

use crate::command::{CommandExecutor, executor::CqrsCommandExecutor, retry::AggregateConflictRetryPolicy};
use crate::postgres::event_repository_with_outbox::repository::PostgresEventRepositoryWithOutbox;

/// Checked snapshot interval and optimistic-conflict policy. Does not read env.
#[derive(Clone, Debug, PartialEq)]
pub struct PostgresCommandConfig {
    snapshot_interval: NonZeroUsize,
    conflict_retry: AggregateConflictRetryPolicy,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PostgresCommandConfigError {
    #[error("snapshot interval must be positive")]
    InvalidSnapshotInterval,
}

impl PostgresCommandConfig {
    pub fn new(
        snapshot_interval: usize,
        conflict_retry: AggregateConflictRetryPolicy,
    ) -> Result<Self, PostgresCommandConfigError> {
        Ok(Self {
            snapshot_interval: NonZeroUsize::new(snapshot_interval).ok_or(PostgresCommandConfigError::InvalidSnapshotInterval)?,
            conflict_retry,
        })
    }

    pub fn snapshot_interval(&self) -> usize {
        self.snapshot_interval.get()
    }

    pub fn conflict_retry(&self) -> AggregateConflictRetryPolicy {
        self.conflict_retry
    }
}

impl Default for PostgresCommandConfig {
    fn default() -> Self {
        Self::new(25, AggregateConflictRetryPolicy::default()).expect("valid default PostgreSQL command configuration")
    }
}

/// Assemble a snapshot-backed command executor with unit services and no queries.
///
/// Construction performs no I/O. Every command binds its immutable task list to
/// a fresh repository/framework; conflict retries reuse that framework and reload
/// state. The pool is shared, never a mutable task buffer. Accepted event, optional
/// snapshot and explicit tasks commit atomically; at most one event is supported
/// per command. Connection/timeout failures may have unknown commit outcomes and
/// are not automatically retried.
///
/// The current cqrs-es snapshot path reapplies accepted events to state already
/// changed by EventSink. Aggregate event application must tolerate that repeat
/// (for example, assign the accepted state rather than increment or append).
///
/// For custom stores, services or queries, use
/// [`CqrsCommandExecutor::new`] with a framework factory instead.
pub fn command_executor<A>(pool: PgPool, config: PostgresCommandConfig) -> Arc<dyn CommandExecutor<A>>
where
    A: Aggregate<Services = ()> + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
{
    let snapshot_interval = config.snapshot_interval();
    Arc::new(CqrsCommandExecutor::<A, _>::new(
        move |tasks| {
            let repository = PostgresEventRepositoryWithOutbox::new(pool.clone()).with_outbox(tasks);
            let store = PersistedEventStore::new_snapshot_store(repository, snapshot_interval);
            CqrsFramework::new(store, Vec::new(), ())
        },
        config.conflict_retry(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn config_defaults_overrides_and_zero_interval_are_checked() {
        let defaults = PostgresCommandConfig::default();
        assert_eq!(defaults.snapshot_interval(), 25);
        assert_eq!(defaults.conflict_retry(), AggregateConflictRetryPolicy::default());
        let retry = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
        let custom = PostgresCommandConfig::new(2, retry).unwrap();
        assert_eq!(custom.snapshot_interval(), 2);
        assert_eq!(custom.conflict_retry(), retry);
        assert_eq!(
            PostgresCommandConfig::new(0, retry),
            Err(PostgresCommandConfigError::InvalidSnapshotInterval)
        );
    }
}
