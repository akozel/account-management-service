use chrono::{DateTime, NaiveDate, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{Email, UserAccountId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum UserAccountEvent {
    AccountCreated {
        account_id: UserAccountId,
        email: Email,
        first_name: String,
        last_name: String,
        date_of_birth: NaiveDate,
        validation_date: NaiveDate,
        created_at: DateTime<Utc>,
    },
}

impl DomainEvent for UserAccountEvent {
    fn event_type(&self) -> String {
        "account_created".to_owned()
    }

    fn event_version(&self) -> String {
        "1".to_owned()
    }
}
