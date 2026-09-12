use crate::application::outbox::{ClaimedTask, TaskExecutionError, TaskFormat, TaskHandler};
use crate::domain::Email;
use crate::domain::UserAccountId;
use async_trait::async_trait;
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
    async fn send(&mut self, message: VerificationCodeDelivery, context: DeliveryContext) -> Result<(), TaskExecutionError>;
}

pub fn send_verification_code_task_handler(sender: Box<dyn VerificationCodeSender>) -> Box<dyn TaskHandler> {
    Box::new(SendVerificationCodeTaskHandler { sender })
}

struct SendVerificationCodeTaskHandler {
    sender: Box<dyn VerificationCodeSender>,
}

#[async_trait]
impl TaskHandler for SendVerificationCodeTaskHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![TaskFormat::of::<SendVerificationCodeTaskV1>()]
    }

    async fn handle(&mut self, task: &ClaimedTask) -> Result<(), TaskExecutionError> {
        if task.format != TaskFormat::of::<SendVerificationCodeTaskV1>() {
            return Err(TaskExecutionError::Permanent {
                reason: "unsupported task format".to_owned(),
            });
        }
        let payload = serde_json::from_value::<SendVerificationCodeTaskV1>(task.payload.clone()).map_err(|_| {
            TaskExecutionError::Permanent {
                reason: "invalid send_verification_code v1 task payload".to_owned(),
            }
        })?;
        if !(1_000_000..=9_999_999).contains(&payload.verification_code) {
            return Err(TaskExecutionError::Permanent {
                reason: "invalid verification code".to_owned(),
            });
        }
        self.sender
            .send(
                payload.into(),
                DeliveryContext {
                    task_id: task.id,
                    attempt: task.attempt,
                    metadata: task.metadata.clone(),
                },
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::outbox::{test_support::*, *};
    use serde_json::json;

    #[tokio::test]
    async fn version_one_task_is_readable_with_extra_fields_and_passes_delivery_context() {
        let sender = sender();
        let records = sender.records.clone();
        let mut handler = send_verification_code_task_handler(Box::new(sender));
        let mut task = claimed();
        task.payload["future_optional_field"] = json!({"locale":"en"});
        handler.handle(&task).await.unwrap();
        assert_eq!(handler.formats(), vec![format()]);
        {
            let saved = records.lock().unwrap();
            assert_eq!(saved[0].0.email.to_string(), "alice@example.com");
            assert_eq!(saved[0].0.account_id.to_string(), "26e91668-f9fc-4be4-bd9d-321fba42e18b");
            assert_eq!(saved[0].0.verification_code, 1_234_567);
            assert_eq!(saved[0].1.task_id, task.id);
            assert_eq!(saved[0].1.attempt, 2);
            assert_eq!(saved[0].1.metadata, task.metadata);
        }
        for payload in [
            json!({}),
            json!({"email":"invalid", "account_id":"26e91668-f9fc-4be4-bd9d-321fba42e18b", "verification_code":1234567}),
            json!({"email":"alice@example.com", "account_id":"26e91668-f9fc-4be4-bd9d-321fba42e18b", "verification_code":0}),
        ] {
            task.payload = payload;
            assert!(matches!(
                handler.handle(&task).await,
                Err(TaskExecutionError::Permanent { .. })
            ));
        }
        task.format.task_version = 3;
        assert!(matches!(
            handler.handle(&task).await,
            Err(TaskExecutionError::Permanent { .. })
        ));
        assert_eq!(records.lock().unwrap().len(), 1);
    }
}
