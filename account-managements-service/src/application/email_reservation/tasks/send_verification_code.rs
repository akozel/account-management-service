use crate::domain::Email;
use crate::domain::UserAccountId;
use async_trait::async_trait;
use cqrs_runtime::task::{
    data::{attempt::TaskAttempt, format::TaskFormatError},
    handling::{
        adapter::TaskAdapter,
        handler::{TaskHandler, TaskHandlerError},
        json_adapter::task_adapter,
    },
};
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

/// Sender for one external effect, borrowing the original immutable delivery.
/// ID, attempt and metadata are available throughout the async invocation.
#[async_trait]
pub trait VerificationCodeSender: Send {
    async fn send(&mut self, message: VerificationCodeDelivery, delivery: &TaskAttempt) -> Result<(), TaskHandlerError>;
}

pub fn send_verification_code_task_adapter(
    sender: Box<dyn VerificationCodeSender>,
) -> Result<Box<dyn TaskAdapter>, TaskFormatError> {
    task_adapter::<SendVerificationCodeTaskV1, _>(SendVerificationCodeTaskHandler { sender })
}

struct SendVerificationCodeTaskHandler {
    sender: Box<dyn VerificationCodeSender>,
}

#[async_trait]
impl TaskHandler<SendVerificationCodeTaskV1> for SendVerificationCodeTaskHandler {
    async fn handle(&mut self, payload: SendVerificationCodeTaskV1, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
        if !(1_000_000..=9_999_999).contains(&payload.verification_code) {
            return Err(TaskHandlerError::Permanent {
                reason: "invalid verification code".to_owned(),
            });
        }
        self.sender.send(payload.into(), delivery).await
    }
}
