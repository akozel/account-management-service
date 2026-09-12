use chrono::{DateTime, NaiveDate, Utc};
use cqrs_es::event_sink::EventSink;

use super::{Email, UserAccount, UserAccountError, UserAccountEvent, UserAccountId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UserAccountCommand {
    CreateAccount {
        account_id: UserAccountId,
        email: Email,
        first_name: String,
        last_name: String,
        date_of_birth: NaiveDate,
        validation_date: NaiveDate,
        created_at: DateTime<Utc>,
    },
}

impl UserAccount {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_create_account(
        &mut self,
        account_id: UserAccountId,
        email: Email,
        first_name: String,
        last_name: String,
        date_of_birth: NaiveDate,
        validation_date: NaiveDate,
        created_at: DateTime<Utc>,
        sink: &EventSink<Self>,
    ) -> Result<(), UserAccountError> {
        let first_name = first_name.trim();
        let last_name = last_name.trim();
        if let Some(existing) = self.created() {
            return if existing.account_id == account_id
                && existing.email == email
                && existing.first_name == first_name
                && existing.last_name == last_name
                && existing.date_of_birth == date_of_birth
                && existing.validation_date == validation_date
            {
                Err(UserAccountError::AlreadyCreated)
            } else {
                Err(UserAccountError::AlreadyExistsWithDifferentData)
            };
        }
        if first_name.is_empty() {
            return Err(UserAccountError::EmptyFirstName);
        }
        if last_name.is_empty() {
            return Err(UserAccountError::EmptyLastName);
        }
        if date_of_birth > validation_date {
            return Err(UserAccountError::FutureDateOfBirth {
                date_of_birth,
                validation_date,
            });
        }
        sink.write(
            UserAccountEvent::AccountCreated {
                account_id,
                email,
                first_name: first_name.to_owned(),
                last_name: last_name.to_owned(),
                date_of_birth,
                validation_date,
                created_at,
            },
            self,
        )
        .await;
        Ok(())
    }
}
