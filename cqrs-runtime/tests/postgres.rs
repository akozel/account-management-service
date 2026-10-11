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
use cqrs_runtime::runtime::{self, dispatcher_factory::TaskDispatcherFactory, worker_pool_config::TaskWorkerPoolConfig};
use cqrs_runtime::{
    postgres::{
        EVENT_STORE_SCHEMA, REQUEUE_FAILED_OUTBOX_TASK, TRANSACTIONAL_OUTBOX_SCHEMA,
        event_repository_with_outbox::repository::PostgresEventRepositoryWithOutbox, task_queue::queue::PostgresTaskQueue,
    },
    task::{
        data::{
            format::{TaskFormat, TaskPayload},
            new_task::{NewTask, TaskSchedule},
        },
        executor::default_executor::task_executor,
        handling::{adapter::TaskAdapter, dispatcher::TaskDispatcher, handler::TaskHandlerError},
        queue::{error::TaskProcessingError, lease::LeasedTask, port::TaskQueue, settlement::SettlementRequest},
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
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[path = "postgres/command_factory.rs"]
mod command_factory;
#[path = "postgres/failures.rs"]
mod failures;
#[path = "postgres/persistence.rs"]
mod persistence;
#[path = "postgres/pool.rs"]
mod pool;
#[path = "postgres/processing.rs"]
mod processing;
#[path = "postgres/queue.rs"]
mod queue;
#[path = "postgres/schema.rs"]
mod schema;
#[path = "postgres/support.rs"]
mod support;
#[path = "postgres/timeouts.rs"]
mod timeouts;
#[cfg(feature = "worker")]
#[path = "postgres/worker.rs"]
mod worker;
use support::*;
