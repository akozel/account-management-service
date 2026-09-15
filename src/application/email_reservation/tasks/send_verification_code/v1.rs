use crate::{
    application::outbox::TaskPayload,
    domain::{Email, UserAccountId},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendVerificationCodeTaskV1 {
    pub email: Email,
    pub account_id: UserAccountId,
    pub verification_code: u32,
}

impl TaskPayload for SendVerificationCodeTaskV1 {
    const TASK_TYPE: &'static str = "send_verification_code";
    const TASK_VERSION: i32 = 1;
}
