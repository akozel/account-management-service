mod command_factory;
mod event_repository_with_outbox;
mod outbox_queue;

pub use command_factory::{PostgresCommandConfig, PostgresCommandConfigError, command_executor};
pub use event_repository_with_outbox::PostgresEventRepositoryWithOutbox;
pub use outbox_queue::{PostgresOutboxQueue, PostgresOutboxQueueConfig, PostgresOutboxQueueConfigError};

/// Canonical PostgreSQL event-store schema used by this runtime.
pub const EVENT_STORE_SCHEMA: &str = include_str!("../sql/001-event-store.sql");

/// Canonical PostgreSQL transactional-outbox schema used by this runtime.
pub const TRANSACTIONAL_OUTBOX_SCHEMA: &str = include_str!("../sql/002-transactional-outbox.sql");

/// Operational statement for explicitly requeuing one failed task.
pub const REQUEUE_FAILED_OUTBOX_TASK: &str = include_str!("../sql/requeue-failed-outbox.sql");
