use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::Email;
use crate::domain::RegistrationProfile;
use crate::domain::UserAccountId;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EmailReservationEvent {
    EmailReserved {
        email: Email,
        account_id: UserAccountId,
        verification_code: u32,
    },
    VerificationCodeRequested {
        email: Email,
        account_id: UserAccountId,
        verification_code: u32,
    },
    VerificationRestarted {
        email: Email,
        account_id: UserAccountId,
        verification_code: u32,
    },
    EmailVerified {
        email: Email,
        account_id: UserAccountId,
        token_digest: String,
    },
    RegistrationProfileAccepted {
        email: Email,
        account_id: UserAccountId,
        consumed_token_digest: String,
        profile: RegistrationProfile,
    },
    AccountCreationCompleted {
        email: Email,
        account_id: UserAccountId,
        created_at: DateTime<Utc>,
    },
}

impl DomainEvent for EmailReservationEvent {
    fn event_type(&self) -> String {
        match self {
            Self::EmailReserved { .. } => "email_reserved",
            Self::VerificationCodeRequested { .. } => "verification_code_requested",
            Self::VerificationRestarted { .. } => "verification_restarted",
            Self::EmailVerified { .. } => "email_verified",
            Self::RegistrationProfileAccepted { .. } => "registration_profile_accepted",
            Self::AccountCreationCompleted { .. } => "account_creation_completed",
        }
        .to_owned()
    }

    fn event_version(&self) -> String {
        match self {
            Self::EmailReserved { .. } => "1",
            Self::VerificationCodeRequested { .. } | Self::EmailVerified { .. } => "1",
            Self::VerificationRestarted { .. } => "1",
            Self::RegistrationProfileAccepted { .. } | Self::AccountCreationCompleted { .. } => "1",
        }
        .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn metadata_matches_new_event_shapes() {
        let email = Email::parse("alice@example.com").unwrap();
        let account_id = UserAccountId::new();
        for (event, kind, version) in [
            (
                EmailReservationEvent::EmailReserved {
                    email: email.clone(),
                    account_id,
                    verification_code: 1_234_567,
                },
                "email_reserved",
                "1",
            ),
            (
                EmailReservationEvent::VerificationCodeRequested {
                    email: email.clone(),
                    account_id,
                    verification_code: 1_234_567,
                },
                "verification_code_requested",
                "1",
            ),
            (
                EmailReservationEvent::VerificationRestarted {
                    email: email.clone(),
                    account_id,
                    verification_code: 1_234_567,
                },
                "verification_restarted",
                "1",
            ),
            (
                EmailReservationEvent::EmailVerified {
                    email,
                    account_id,
                    token_digest: "digest".into(),
                },
                "email_verified",
                "1",
            ),
            (
                EmailReservationEvent::RegistrationProfileAccepted {
                    email: Email::parse("alice@example.com").unwrap(),
                    account_id,
                    consumed_token_digest: "digest".into(),
                    profile: RegistrationProfile::new(
                        "Alice".into(),
                        "Smith".into(),
                        NaiveDate::from_ymd_opt(1990, 5, 12).unwrap(),
                        NaiveDate::from_ymd_opt(2026, 9, 14).unwrap(),
                    ),
                },
                "registration_profile_accepted",
                "1",
            ),
            (
                EmailReservationEvent::AccountCreationCompleted {
                    email: Email::parse("alice@example.com").unwrap(),
                    account_id,
                    created_at: DateTime::parse_from_rfc3339("2026-09-14T12:00:00Z")
                        .unwrap()
                        .with_timezone(&Utc),
                },
                "account_creation_completed",
                "1",
            ),
        ] {
            assert_eq!(event.event_type(), kind);
            assert_eq!(event.event_version(), version);
            assert_eq!(
                serde_json::from_value::<EmailReservationEvent>(serde_json::to_value(&event).unwrap()).unwrap(),
                event
            );
        }
    }
}
