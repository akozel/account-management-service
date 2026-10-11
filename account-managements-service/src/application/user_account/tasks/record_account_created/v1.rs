use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::{Email, RegistrationProfile, UserAccountId};
use cqrs_runtime::task::data::format::TaskPayload;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordAccountCreatedTaskV1 {
    pub email: Email,
    pub account_id: UserAccountId,
    pub profile: RegistrationProfile,
    pub created_at: DateTime<Utc>,
}

impl TaskPayload for RecordAccountCreatedTaskV1 {
    const TASK_TYPE: &'static str = "record_account_created";
    const TASK_VERSION: i32 = 1;
}
