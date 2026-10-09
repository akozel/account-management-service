#![cfg(feature = "postgres")]

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{
    Aggregate, DomainEvent,
    event_sink::EventSink,
    persist::{PersistedEventRepository, PersistenceError, SerializedEvent},
};
#[cfg(feature = "worker")]
use cqrs_runtime::{
    outbox::TaskHandlerRegistry,
    worker::{self as outbox_worker_pool, OutboxWorkerPoolConfig, TaskHandlerFactory},
};
use cqrs_runtime::{
    outbox::{
        ClaimedTask, NewOutboxTask, OutboxError, OutboxQueue, Schedule, TaskExecutionError, TaskFormat, TaskHandler, TaskOutcome,
        TaskPayload, outbox_task_coordinator,
    },
    postgres::{
        EVENT_STORE_SCHEMA, PostgresEventRepositoryWithOutbox, PostgresOutboxQueue, REQUEUE_FAILED_OUTBOX_TASK,
        TRANSACTIONAL_OUTBOX_SCHEMA,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
#[cfg(feature = "worker")]
use tokio::sync::watch;
use uuid::Uuid;

#[path = "postgres/persistence.rs"]
mod persistence;
#[path = "postgres/processing.rs"]
mod processing;
#[path = "postgres/queue.rs"]
mod queue;
#[path = "postgres/support.rs"]
mod support;
#[cfg(feature = "worker")]
#[path = "postgres/worker.rs"]
mod worker;
use support::*;
