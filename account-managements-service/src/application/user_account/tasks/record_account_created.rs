use std::sync::Arc;

use async_trait::async_trait;

use super::{RecordAccountCreatedTaskV1, delivery_error};
use crate::application::user_account::service::RegistrationService;
use cqrs_runtime::outbox::{TaskDelivery, TaskFormat, TaskFormatError, TaskHandler, TaskHandlerError};

pub mod v1;

pub fn record_account_created_task_handler(
    service: Arc<dyn RegistrationService>,
) -> Result<Box<dyn TaskHandler>, TaskFormatError> {
    Ok(Box::new(RecordAccountCreatedTaskHandler {
        service,
        format: TaskFormat::of::<RecordAccountCreatedTaskV1>()?,
    }))
}

struct RecordAccountCreatedTaskHandler {
    format: TaskFormat,
    service: Arc<dyn RegistrationService>,
}

#[async_trait]
impl TaskHandler for RecordAccountCreatedTaskHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![self.format.clone()]
    }

    async fn handle(&mut self, task: &TaskDelivery) -> Result<(), TaskHandlerError> {
        if task.format() != &self.format {
            return Err(TaskHandlerError::Permanent {
                reason: "unsupported record_account_created format".into(),
            });
        }
        let payload = serde_json::from_value::<RecordAccountCreatedTaskV1>(task.payload().clone()).map_err(|_| {
            TaskHandlerError::Permanent {
                reason: "invalid record_account_created payload".into(),
            }
        })?;
        self.service.record_account_created(payload).await.map_err(delivery_error)
    }
}
