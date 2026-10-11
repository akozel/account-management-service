use std::sync::Arc;

use async_trait::async_trait;

use super::{CreateAccountTaskV1, delivery_error};
use crate::application::user_account::service::RegistrationService;
use cqrs_runtime::outbox::{TaskDelivery, TaskFormatError, TaskHandler, TaskHandlerError, TypedTaskHandler, typed_task_handler};

pub mod v1;

pub fn create_account_task_handler(service: Arc<dyn RegistrationService>) -> Result<Box<dyn TaskHandler>, TaskFormatError> {
    typed_task_handler::<CreateAccountTaskV1, _>(CreateAccountTaskHandler { service })
}

struct CreateAccountTaskHandler {
    service: Arc<dyn RegistrationService>,
}

#[async_trait]
impl TypedTaskHandler<CreateAccountTaskV1> for CreateAccountTaskHandler {
    async fn handle(&mut self, payload: CreateAccountTaskV1, _delivery: &TaskDelivery) -> Result<(), TaskHandlerError> {
        self.service.create_account(payload).await.map_err(delivery_error)
    }
}
