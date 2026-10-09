use serde::{Deserialize, Serialize};

mod aggregate;
mod commands;
mod errors;
mod events;
mod value_objects;

pub use commands::EmailReservationCommand;
pub use errors::EmailReservationError;
pub use events::EmailReservationEvent;
pub use value_objects::{Email, InvalidEmail};

use super::UserAccountId;
use chrono::{DateTime, NaiveDate, Utc};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegistrationProfile {
    pub first_name: String,
    pub last_name: String,
    pub date_of_birth: NaiveDate,
    pub validation_date: NaiveDate,
}

impl RegistrationProfile {
    pub fn new(first_name: String, last_name: String, date_of_birth: NaiveDate, validation_date: NaiveDate) -> Self {
        Self {
            first_name: first_name.trim().to_owned(),
            last_name: last_name.trim().to_owned(),
            date_of_birth,
            validation_date,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReservationPhase {
    #[default]
    Absent,
    AwaitingCode {
        account_id: UserAccountId,
        verification_code: u32,
    },
    AwaitingProfile {
        account_id: UserAccountId,
        token_digest: String,
    },
    CreatingAccount {
        account_id: UserAccountId,
        consumed_token_digest: String,
        accepted_profile: RegistrationProfile,
    },
    AccountCreated {
        account_id: UserAccountId,
        consumed_token_digest: String,
        accepted_profile: RegistrationProfile,
        created_at: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EmailReservation {
    phase: ReservationPhase,
}

impl EmailReservation {
    pub const fn phase(&self) -> &ReservationPhase {
        &self.phase
    }

    pub const fn account_id(&self) -> Option<UserAccountId> {
        match &self.phase {
            ReservationPhase::Absent => None,
            ReservationPhase::AwaitingCode { account_id, .. }
            | ReservationPhase::AwaitingProfile { account_id, .. }
            | ReservationPhase::CreatingAccount { account_id, .. }
            | ReservationPhase::AccountCreated { account_id, .. } => Some(*account_id),
        }
    }

    pub fn is_reserved(&self) -> bool {
        !matches!(self.phase, ReservationPhase::Absent)
    }
}
