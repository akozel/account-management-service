#![cfg(feature = "worker")]
//! Task runtime behavior through the public queue, executor and runtime modules.
use async_trait::async_trait;
use cqrs_runtime::{
    runtime::{
        dispatcher_factory::{TaskDispatcherFactory, WorkerSetupError},
        supervisor::{TaskRuntimeError, run},
        worker_pool_config::TaskWorkerPoolConfig,
    },
    task::{
        data::{attempt::TaskAttempt, format::TaskFormat},
        executor::default_executor::{task_executor, task_executor_with_config},
        handling::{
            adapter::TaskAdapter,
            dispatcher::{TaskDispatcher, TaskDispatcherError},
            handler::TaskHandlerError,
        },
        queue::{
            error::TaskProcessingError,
            lease::LeasedTask,
            port::TaskQueue,
            settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
            storage_config::TaskStorageConfig,
        },
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::task::Id;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[path = "runtime/admission.rs"]
mod admission;
#[path = "runtime/preparation.rs"]
mod preparation;
#[path = "runtime/shutdown.rs"]
mod shutdown;
#[path = "runtime/supervision.rs"]
mod supervision;
#[path = "runtime/support.rs"]
mod support;
use support::*;
