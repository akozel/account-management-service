use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use account_management_service::{
    application::{
        email_reservation::tasks::send_verification_code::{send_verification_code_task_handler, v1::SendVerificationCodeTaskV1},
        outbox::{
            ClaimedTask, NewOutboxTask, OutboxError, OutboxQueue, Schedule, TaskExecutionError, TaskFormat, TaskHandler,
            TaskHandlerRegistry, TaskOutcome, outbox_task_coordinator,
        },
    },
    domain::{Email, EmailReservation},
    infrastructure::{
        console_verification_code_sender::ConsoleVerificationCodeSender,
        postgres::{event_repository_with_outbox::PostgresEventRepositoryWithOutbox, outbox_queue::PostgresOutboxQueue},
    },
    presentation::outbox_worker_pool::{self, OutboxWorkerPoolConfig, TaskHandlerFactory},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{
    Aggregate, DomainEvent,
    event_sink::EventSink,
    persist::{PersistedEventRepository, PersistenceError, SerializedEvent},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use tokio::sync::watch;
use uuid::Uuid;

#[path = "outbox/persistence.rs"]
mod persistence;
#[path = "outbox/processing.rs"]
mod processing;
#[path = "outbox/queue.rs"]
mod queue;
#[path = "outbox/support.rs"]
mod support;
#[path = "outbox/worker.rs"]
mod worker;
use support::*;
