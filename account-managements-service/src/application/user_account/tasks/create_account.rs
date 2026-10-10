use std::sync::Arc;

use async_trait::async_trait;

use super::{CreateAccountTaskV1, delivery_error};
use crate::application::user_account::service::RegistrationService;
use cqrs_runtime::outbox::{TaskDelivery, TaskFormat, TaskFormatError, TaskHandler, TaskHandlerError};

pub mod v1;

pub fn create_account_task_handler(service: Arc<dyn RegistrationService>) -> Result<Box<dyn TaskHandler>, TaskFormatError> {
    Ok(Box::new(CreateAccountTaskHandler {
        service,
        format: TaskFormat::of::<CreateAccountTaskV1>()?,
    }))
}

struct CreateAccountTaskHandler {
    format: TaskFormat,
    service: Arc<dyn RegistrationService>,
}

#[async_trait]
impl TaskHandler for CreateAccountTaskHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![self.format.clone()]
    }

    async fn handle(&mut self, task: &TaskDelivery) -> Result<(), TaskHandlerError> {
        if task.format() != &self.format {
            return Err(TaskHandlerError::Permanent {
                reason: "unsupported create_account format".into(),
            });
        }
        let payload =
            serde_json::from_value::<CreateAccountTaskV1>(task.payload().clone()).map_err(|_| TaskHandlerError::Permanent {
                reason: "invalid create_account payload".into(),
            })?;
        self.service.create_account(payload).await.map_err(delivery_error)
    }
}
