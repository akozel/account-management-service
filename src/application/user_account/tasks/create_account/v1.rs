use serde::{Deserialize, Serialize};

use crate::{
    application::outbox::TaskPayload,
    domain::{Email, RegistrationProfile, UserAccountId},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateAccountTaskV1 {
    pub email: Email,
    pub account_id: UserAccountId,
    pub profile: RegistrationProfile,
}

impl TaskPayload for CreateAccountTaskV1 {
    const TASK_TYPE: &'static str = "create_account";
    const TASK_VERSION: i32 = 1;
}
