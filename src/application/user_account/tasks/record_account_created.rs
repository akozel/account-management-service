use std::sync::Arc;

use async_trait::async_trait;

use super::{RecordAccountCreatedTaskV1, delivery_error};
use crate::application::{
    outbox::{ClaimedTask, TaskExecutionError, TaskFormat, TaskHandler},
    user_account::service::RegistrationService,
};

pub mod v1;

pub fn record_account_created_task_handler(service: Arc<dyn RegistrationService>) -> Box<dyn TaskHandler> {
    Box::new(RecordAccountCreatedTaskHandler { service })
}

struct RecordAccountCreatedTaskHandler {
    service: Arc<dyn RegistrationService>,
}

#[async_trait]
impl TaskHandler for RecordAccountCreatedTaskHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![TaskFormat::of::<RecordAccountCreatedTaskV1>()]
    }

    async fn handle(&mut self, task: &ClaimedTask) -> Result<(), TaskExecutionError> {
        if task.format != TaskFormat::of::<RecordAccountCreatedTaskV1>() {
            return Err(TaskExecutionError::Permanent {
                reason: "unsupported record_account_created format".into(),
            });
        }
        let payload = serde_json::from_value::<RecordAccountCreatedTaskV1>(task.payload.clone()).map_err(|_| {
            TaskExecutionError::Permanent {
                reason: "invalid record_account_created payload".into(),
            }
        })?;
        self.service.record_account_created(payload).await.map_err(delivery_error)
    }
}
