use std::sync::Arc;

use async_trait::async_trait;

use super::{RecordAccountCreatedTaskV1, delivery_error};
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

pub fn record_account_created_task_adapter(
    service: Arc<dyn RegistrationService>,
) -> Result<Box<dyn TaskAdapter>, TaskFormatError> {
    task_adapter::<RecordAccountCreatedTaskV1, _>(RecordAccountCreatedTaskHandler { service })
}

struct RecordAccountCreatedTaskHandler {
    service: Arc<dyn RegistrationService>,
}

#[async_trait]
impl TaskHandler<RecordAccountCreatedTaskV1> for RecordAccountCreatedTaskHandler {
    async fn handle(&mut self, payload: RecordAccountCreatedTaskV1, _delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.service.record_account_created(payload).await.map_err(delivery_error)
    }
}
