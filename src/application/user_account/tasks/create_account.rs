use std::sync::Arc;

use async_trait::async_trait;

use super::{CreateAccountTaskV1, delivery_error};
use crate::application::{
    outbox::{ClaimedTask, TaskExecutionError, TaskFormat, TaskHandler},
    user_account::service::RegistrationService,
};

pub mod v1;

pub fn create_account_task_handler(service: Arc<dyn RegistrationService>) -> Box<dyn TaskHandler> {
    Box::new(CreateAccountTaskHandler { service })
}

struct CreateAccountTaskHandler {
    service: Arc<dyn RegistrationService>,
}

#[async_trait]
impl TaskHandler for CreateAccountTaskHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![TaskFormat::of::<CreateAccountTaskV1>()]
    }

    async fn handle(&mut self, task: &ClaimedTask) -> Result<(), TaskExecutionError> {
        if task.format != TaskFormat::of::<CreateAccountTaskV1>() {
            return Err(TaskExecutionError::Permanent {
                reason: "unsupported create_account format".into(),
            });
        }
        let payload =
            serde_json::from_value::<CreateAccountTaskV1>(task.payload.clone()).map_err(|_| TaskExecutionError::Permanent {
                reason: "invalid create_account payload".into(),
            })?;
        self.service.create_account(payload).await.map_err(delivery_error)
    }
}
