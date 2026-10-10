use crate::domain::Email;
use crate::domain::UserAccountId;
use async_trait::async_trait;
use cqrs_runtime::outbox::{TaskDelivery, TaskFormat, TaskFormatError, TaskHandler, TaskHandlerError};
use uuid::Uuid;
pub mod v1;
use v1::SendVerificationCodeTaskV1;

/// Current handler input; evolve this independently of the persisted v1 task DTO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationCodeDelivery {
    pub email: Email,
    pub account_id: UserAccountId,
    pub verification_code: u32,
}

impl From<SendVerificationCodeTaskV1> for VerificationCodeDelivery {
    fn from(task: SendVerificationCodeTaskV1) -> Self {
        Self {
            email: task.email,
            account_id: task.account_id,
            verification_code: task.verification_code,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeliveryContext {
    pub task_id: Uuid,
    pub attempt: i32,
    pub metadata: serde_json::Value,
}

#[async_trait]
pub trait VerificationCodeSender: Send {
    async fn send(&mut self, message: VerificationCodeDelivery, context: DeliveryContext) -> Result<(), TaskHandlerError>;
}

pub fn send_verification_code_task_handler(
    sender: Box<dyn VerificationCodeSender>,
) -> Result<Box<dyn TaskHandler>, TaskFormatError> {
    Ok(Box::new(SendVerificationCodeTaskHandler {
        sender,
        format: TaskFormat::of::<SendVerificationCodeTaskV1>()?,
    }))
}

struct SendVerificationCodeTaskHandler {
    format: TaskFormat,
    sender: Box<dyn VerificationCodeSender>,
}

#[async_trait]
impl TaskHandler for SendVerificationCodeTaskHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![self.format.clone()]
    }

    async fn handle(&mut self, task: &TaskDelivery) -> Result<(), TaskHandlerError> {
        if task.format() != &self.format {
            return Err(TaskHandlerError::Permanent {
                reason: "unsupported task format".to_owned(),
            });
        }
        let payload = serde_json::from_value::<SendVerificationCodeTaskV1>(task.payload().clone()).map_err(|_| {
            TaskHandlerError::Permanent {
                reason: "invalid send_verification_code v1 task payload".to_owned(),
            }
        })?;
        if !(1_000_000..=9_999_999).contains(&payload.verification_code) {
            return Err(TaskHandlerError::Permanent {
                reason: "invalid verification code".to_owned(),
            });
        }
        self.sender
            .send(
                payload.into(),
                DeliveryContext {
                    task_id: task.id(),
                    attempt: task.attempt(),
                    metadata: task.metadata().clone(),
                },
            )
            .await
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use cqrs_runtime::outbox::{TaskDelivery, TaskFormat, TaskHandlerError};
    use serde_json::json;
    use uuid::Uuid;

    use super::{DeliveryContext, SendVerificationCodeTaskV1, VerificationCodeDelivery, VerificationCodeSender};

    pub(crate) fn format() -> TaskFormat {
        TaskFormat::of::<SendVerificationCodeTaskV1>().unwrap()
    }

    pub(crate) fn claimed() -> TaskDelivery {
        TaskDelivery::new(Uuid::new_v4(), format(), json!({"email": "alice@example.com", "account_id": "26e91668-f9fc-4be4-bd9d-321fba42e18b", "verification_code": 1_234_567}), json!({"correlation_id":"test"}), 2).unwrap()
    }

    #[derive(Clone)]
    pub(crate) struct Sender {
        pub(crate) records: Arc<Mutex<Vec<(VerificationCodeDelivery, DeliveryContext)>>>,
    }

    #[async_trait]
    impl VerificationCodeSender for Sender {
        async fn send(&mut self, message: VerificationCodeDelivery, context: DeliveryContext) -> Result<(), TaskHandlerError> {
            self.records.lock().unwrap().push((message, context));
            Ok(())
        }
    }

    pub(crate) fn sender() -> Sender {
        Sender { records: Arc::default() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_runtime::outbox::TaskHandlerError;
    use serde_json::json;

    use super::test_support::*;

    #[tokio::test]
    async fn version_one_task_is_readable_with_extra_fields_and_passes_delivery_context() {
        let sender = sender();
        let records = sender.records.clone();
        let mut handler = send_verification_code_task_handler(Box::new(sender)).unwrap();
        let mut task = claimed();
        let mut payload = task.payload().clone();
        payload["future_optional_field"] = json!({"locale":"en"});
        task = TaskDelivery::new(
            task.id(),
            task.format().clone(),
            payload,
            task.metadata().clone(),
            task.attempt(),
        )
        .unwrap();
        handler.handle(&task).await.unwrap();
        assert_eq!(handler.formats(), vec![format()]);
        {
            let saved = records.lock().unwrap();
            assert_eq!(saved[0].0.email.to_string(), "alice@example.com");
            assert_eq!(saved[0].0.account_id.to_string(), "26e91668-f9fc-4be4-bd9d-321fba42e18b");
            assert_eq!(saved[0].0.verification_code, 1_234_567);
            assert_eq!(saved[0].1.task_id, task.id());
            assert_eq!(saved[0].1.attempt, 2);
            assert_eq!(&saved[0].1.metadata, task.metadata());
        }
        for payload in [
            json!({}),
            json!({"email":"invalid", "account_id":"26e91668-f9fc-4be4-bd9d-321fba42e18b", "verification_code":1234567}),
            json!({"email":"alice@example.com", "account_id":"26e91668-f9fc-4be4-bd9d-321fba42e18b", "verification_code":0}),
        ] {
            task = TaskDelivery::new(
                task.id(),
                task.format().clone(),
                payload,
                task.metadata().clone(),
                task.attempt(),
            )
            .unwrap();
            assert!(matches!(handler.handle(&task).await, Err(TaskHandlerError::Permanent { .. })));
        }
        task = TaskDelivery::new(
            task.id(),
            TaskFormat::new(task.format().task_type(), 3).unwrap(),
            task.payload().clone(),
            task.metadata().clone(),
            task.attempt(),
        )
        .unwrap();
        assert!(matches!(handler.handle(&task).await, Err(TaskHandlerError::Permanent { .. })));
        assert_eq!(records.lock().unwrap().len(), 1);
    }
}
