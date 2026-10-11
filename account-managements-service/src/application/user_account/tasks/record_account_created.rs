use std::sync::Arc;

use async_trait::async_trait;

use super::{RecordAccountCreatedTaskV1, delivery_error};
use crate::application::user_account::service::RegistrationService;
use cqrs_runtime::outbox::{TaskDelivery, TaskFormatError, TaskHandler, TaskHandlerError, TypedTaskHandler, typed_task_handler};

pub mod v1;

pub fn record_account_created_task_handler(
    service: Arc<dyn RegistrationService>,
) -> Result<Box<dyn TaskHandler>, TaskFormatError> {
    typed_task_handler::<RecordAccountCreatedTaskV1, _>(RecordAccountCreatedTaskHandler { service })
}

struct RecordAccountCreatedTaskHandler {
    service: Arc<dyn RegistrationService>,
}

#[async_trait]
impl TypedTaskHandler<RecordAccountCreatedTaskV1> for RecordAccountCreatedTaskHandler {
    async fn handle(&mut self, payload: RecordAccountCreatedTaskV1, _delivery: &TaskDelivery) -> Result<(), TaskHandlerError> {
        self.service.record_account_created(payload).await.map_err(delivery_error)
    }
}
