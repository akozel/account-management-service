use std::sync::Arc;

use async_trait::async_trait;

use super::{CreateAccountTaskV1, delivery_error};
use crate::application::user_account::service::RegistrationService;
use cqrs_runtime::task::{
    data::{attempt::TaskAttempt, format::TaskFormatError},
    handling::{
        adapter::TaskAdapter,
        handler::{TaskHandler, TaskHandlerError},
        json_adapter::task_adapter,
    },
};

pub mod v1;

pub fn create_account_task_adapter(service: Arc<dyn RegistrationService>) -> Result<Box<dyn TaskAdapter>, TaskFormatError> {
    task_adapter::<CreateAccountTaskV1, _>(CreateAccountTaskHandler { service })
}

struct CreateAccountTaskHandler {
    service: Arc<dyn RegistrationService>,
}

#[async_trait]
impl TaskHandler<CreateAccountTaskV1> for CreateAccountTaskHandler {
    async fn handle(&mut self, payload: CreateAccountTaskV1, _delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.service.create_account(payload).await.map_err(delivery_error)
    }
}
