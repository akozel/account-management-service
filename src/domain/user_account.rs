use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

mod aggregate;
mod commands;
mod errors;
mod events;
mod id;
#[cfg(test)]
mod tests;

pub use super::email_reservation::Email;
pub use commands::UserAccountCommand;
pub use errors::UserAccountError;
pub use events::UserAccountEvent;
pub use id::UserAccountId;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserAccount {
    created: Option<CreatedAccount>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreatedAccount {
    account_id: UserAccountId,
    email: Email,
    first_name: String,
    last_name: String,
    date_of_birth: NaiveDate,
    validation_date: NaiveDate,
    created_at: DateTime<Utc>,
}

impl UserAccount {
    pub const fn created(&self) -> Option<&CreatedAccount> {
        self.created.as_ref()
    }
}

impl CreatedAccount {
    pub const fn account_id(&self) -> UserAccountId {
        self.account_id
    }

    pub const fn email(&self) -> &Email {
        &self.email
    }

    pub fn first_name(&self) -> &str {
        &self.first_name
    }

    pub fn last_name(&self) -> &str {
        &self.last_name
    }

    pub const fn date_of_birth(&self) -> NaiveDate {
        self.date_of_birth
    }

    pub const fn validation_date(&self) -> NaiveDate {
        self.validation_date
    }

    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
