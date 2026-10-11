use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::task::{
    data::{
        attempt::TaskAttempt,
        format::{TaskFormat, TaskPayload},
    },
    handling::{adapter::TaskAdapter, dispatcher::TaskDispatcher, handler::TaskHandlerError},
    queue::{
        error::TaskProcessingError,
        lease::{LeasedTask, TaskLease},
        port::TaskQueue,
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult, TaskFailure, TaskFailureCause},
    },
};

#[derive(Serialize)]
pub(crate) struct DummyTask {
    pub(crate) value: &'static str,
}

impl TaskPayload for DummyTask {
    const TASK_TYPE: &'static str = "dummy_task";
    const TASK_VERSION: i32 = 1;
}

pub(crate) fn format() -> TaskFormat {
    TaskFormat::of::<DummyTask>().unwrap()
}

pub(crate) fn claimed() -> LeasedTask {
    LeasedTask::new(
        TaskAttempt::new(
            Uuid::new_v4(),
            format(),
            json!({"value": "runtime"}),
            json!({"correlation_id": "test"}),
            2,
        )
        .unwrap(),
        TaskLease::new(Uuid::new_v4(), tokio::time::Instant::now() + Duration::from_secs(60)),
    )
}

#[derive(Default)]
pub(crate) struct Queue {
    pub(crate) incoming: Mutex<VecDeque<LeasedTask>>,
    pub(crate) outcomes: Mutex<Vec<(Uuid, SettlementRequest)>>,
    pub(crate) requested: Mutex<Vec<TaskFormat>>,
    pub(crate) claim_limits: Mutex<Vec<usize>>,
    pub(crate) recovery_limits: Mutex<Vec<usize>>,
    pub(crate) claim_error: AtomicBool,
    pub(crate) recovery_error: AtomicBool,
    pub(crate) settle_error: AtomicBool,
    pub(crate) lost_owner: AtomicBool,
    pub(crate) recovery_calls: AtomicUsize,
    pub(crate) hang_claim: AtomicBool,
    pub(crate) hang_recovery: AtomicBool,
    pub(crate) hang_settle: AtomicBool,
    pub(crate) failing_format: Mutex<Option<TaskFormat>>,
    pub(crate) claim_delay: Mutex<Duration>,
}

#[async_trait]
impl TaskQueue for Queue {
    async fn claim(
        &self,
        format: &TaskFormat,
        _: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        let lease_deadline = tokio::time::Instant::now() + lease;
        self.requested.lock().unwrap().push(format.clone());
        self.claim_limits.lock().unwrap().push(limit);
        let delay = *self.claim_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if self.hang_claim.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.claim_error.load(Ordering::SeqCst) || self.failing_format.lock().unwrap().as_ref() == Some(format) {
            return Err(TaskProcessingError::Unavailable);
        }
        let mut incoming = self.incoming.lock().unwrap();
        let mut result = Vec::new();
        while result.len() < limit && incoming.front().is_some_and(|message| message.attempt().format() == format) {
            let mut task = incoming.pop_front().unwrap();
            task = with_deadline(task, lease_deadline);
            result.push(task);
        }
        Ok(result)
    }

    async fn settle(&self, task: &LeasedTask, outcome: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        let confirmed = match &outcome {
            SettlementRequest::Completed => SettlementResult::Completed,
            SettlementRequest::Retry { .. } => SettlementResult::RetryScheduled,
            SettlementRequest::Failed { .. } => SettlementResult::Failed(TaskFailure {
                id: task.attempt().id(),
                format: task.attempt().format().clone(),
                attempt: task.attempt().number(),
                cause: TaskFailureCause::PermanentRejection,
            }),
        };
        self.outcomes.lock().unwrap().push((task.attempt().id(), outcome));
        if self.hang_settle.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.settle_error.load(Ordering::SeqCst) {
            return Err(TaskProcessingError::Unavailable);
        }
        if self.lost_owner.load(Ordering::SeqCst) {
            return Ok(SettlementResult::OwnershipLost);
        }
        Ok(confirmed)
    }

    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        self.recovery_limits.lock().unwrap().push(limit);
        self.recovery_calls.fetch_add(1, Ordering::SeqCst);
        if self.hang_recovery.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.recovery_error.load(Ordering::SeqCst) {
            return Err(TaskProcessingError::Unavailable);
        }
        Ok(LeaseRecoveryReport::default())
    }
}

pub(crate) struct Adapter {
    pub(crate) result: Result<(), TaskHandlerError>,
    pub(crate) hang: bool,
}

#[async_trait]
impl TaskAdapter for Adapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
        if self.hang {
            std::future::pending::<()>().await;
        }
        self.result.clone()
    }
}

pub(crate) fn adapter() -> Adapter {
    Adapter {
        result: Ok(()),
        hang: false,
    }
}

#[cfg(feature = "worker")]
pub(crate) struct CountingAdapter(pub(crate) std::sync::Arc<AtomicUsize>);

#[cfg(feature = "worker")]
#[async_trait]
impl TaskAdapter for CountingAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub(crate) fn with_deadline(task: LeasedTask, deadline: tokio::time::Instant) -> LeasedTask {
    let token = task.lease().lock_token();
    LeasedTask::new(task.attempt().clone(), TaskLease::new(token, deadline))
}
#[cfg(feature = "worker")]
pub(crate) fn with_format(task: LeasedTask, format: TaskFormat) -> LeasedTask {
    let d = task.attempt();
    LeasedTask::new(
        TaskAttempt::new(d.id(), format, d.payload().clone(), d.metadata().clone(), d.number()).unwrap(),
        task.lease().clone(),
    )
}

/// Route a test adapter through the same concrete dispatcher used in production.
pub(crate) fn dispatcher(adapter: impl TaskAdapter + 'static) -> TaskDispatcher {
    TaskDispatcher::try_new(vec![Box::new(adapter)]).unwrap()
}
