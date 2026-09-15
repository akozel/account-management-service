use cqrs_es::event_sink::EventSink;

use super::RegistrationProfile;
use super::{Email, EmailReservation, EmailReservationError, EmailReservationEvent, ReservationPhase};
use crate::domain::UserAccountId;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EmailReservationCommand {
    ReserveEmail {
        email: Email,
        account_id: UserAccountId,
        verification_code: u32,
    },
    RequestVerificationCode {
        email: Email,
        account_id: UserAccountId,
        verification_code: u32,
    },
    VerifyEmail {
        email: Email,
        account_id: UserAccountId,
        code: u32,
        token_digest: String,
    },
    SubmitRegistrationProfile {
        email: Email,
        account_id: UserAccountId,
        token_digest: String,
        profile: RegistrationProfile,
    },
    RecordAccountCreated {
        email: Email,
        account_id: UserAccountId,
        profile: RegistrationProfile,
        created_at: DateTime<Utc>,
    },
}

impl EmailReservation {
    pub(super) async fn handle_reserve(
        &mut self,
        email: Email,
        account_id: UserAccountId,
        verification_code: u32,
        sink: &EventSink<Self>,
    ) -> Result<(), EmailReservationError> {
        if self.is_reserved() {
            return Err(EmailReservationError::AlreadyReserved);
        }
        ensure_code_format(verification_code)?;
        sink.write(
            EmailReservationEvent::EmailReserved {
                email,
                account_id,
                verification_code,
            },
            self,
        )
        .await;
        Ok(())
    }

    pub(super) async fn handle_request_verification_code(
        &mut self,
        email: Email,
        account_id: UserAccountId,
        verification_code: u32,
        sink: &EventSink<Self>,
    ) -> Result<(), EmailReservationError> {
        let current = self.account_id().ok_or(EmailReservationError::NotReserved)?;
        if current == account_id {
            return Err(EmailReservationError::AccountIdUnchanged);
        }
        ensure_code_format(verification_code)?;
        let event = match self.phase() {
            ReservationPhase::AwaitingCode { .. } => EmailReservationEvent::VerificationCodeRequested {
                email,
                account_id,
                verification_code,
            },
            ReservationPhase::AwaitingProfile { .. } => EmailReservationEvent::VerificationRestarted {
                email,
                account_id,
                verification_code,
            },
            ReservationPhase::CreatingAccount { .. } | ReservationPhase::AccountCreated { .. } => {
                return Err(EmailReservationError::AccountCreationStarted);
            }
            ReservationPhase::Absent => unreachable!("a current account ID exists"),
        };
        sink.write(event, self).await;
        Ok(())
    }

    pub(super) async fn handle_verify(
        &mut self,
        email: Email,
        account_id: UserAccountId,
        code: u32,
        token_digest: String,
        sink: &EventSink<Self>,
    ) -> Result<(), EmailReservationError> {
        match self.phase() {
            ReservationPhase::Absent => return Err(EmailReservationError::NotReserved),
            ReservationPhase::AwaitingCode {
                account_id: current,
                verification_code,
            } => {
                if *current != account_id {
                    return Err(EmailReservationError::NotCurrentAccountId);
                }
                if *verification_code != code {
                    return Err(EmailReservationError::InvalidVerificationCode);
                }
            }
            ReservationPhase::AwaitingProfile { account_id: current, .. } => {
                return Err(if *current == account_id {
                    EmailReservationError::AlreadyVerified
                } else {
                    EmailReservationError::NotCurrentAccountId
                });
            }
            ReservationPhase::CreatingAccount { .. } | ReservationPhase::AccountCreated { .. } => {
                return Err(EmailReservationError::AccountCreationStarted);
            }
        }
        sink.write(
            EmailReservationEvent::EmailVerified {
                email,
                account_id,
                token_digest,
            },
            self,
        )
        .await;
        Ok(())
    }

    pub(super) async fn handle_submit_profile(
        &mut self,
        email: Email,
        account_id: UserAccountId,
        token_digest: String,
        profile: RegistrationProfile,
        sink: &EventSink<Self>,
    ) -> Result<(), EmailReservationError> {
        let profile = RegistrationProfile::new(
            profile.first_name,
            profile.last_name,
            profile.date_of_birth,
            profile.validation_date,
        );
        match self.phase() {
            ReservationPhase::Absent => return Err(EmailReservationError::NotReserved),
            ReservationPhase::AwaitingCode { account_id: current, .. } => {
                return Err(if *current == account_id {
                    EmailReservationError::NotAwaitingProfile
                } else {
                    EmailReservationError::NotCurrentAccountId
                });
            }
            ReservationPhase::AwaitingProfile {
                account_id: current,
                token_digest: expected,
            } => {
                if *current != account_id {
                    return Err(EmailReservationError::NotCurrentAccountId);
                }
                if *expected != token_digest {
                    return Err(EmailReservationError::InvalidAccountCreationToken);
                }
            }
            ReservationPhase::CreatingAccount {
                account_id: current,
                consumed_token_digest,
                ..
            }
            | ReservationPhase::AccountCreated {
                account_id: current,
                consumed_token_digest,
                ..
            } => {
                if *current != account_id {
                    return Err(EmailReservationError::NotCurrentAccountId);
                }
                if *consumed_token_digest != token_digest {
                    return Err(EmailReservationError::InvalidAccountCreationToken);
                }
                return Err(EmailReservationError::ProfileAlreadySubmitted);
            }
        }
        if profile.first_name.is_empty() {
            return Err(EmailReservationError::EmptyFirstName);
        }
        if profile.last_name.is_empty() {
            return Err(EmailReservationError::EmptyLastName);
        }
        if profile.date_of_birth > profile.validation_date {
            return Err(EmailReservationError::FutureDateOfBirth);
        }
        sink.write(
            EmailReservationEvent::RegistrationProfileAccepted {
                email,
                account_id,
                consumed_token_digest: token_digest,
                profile,
            },
            self,
        )
        .await;
        Ok(())
    }

    pub(super) async fn handle_record_account_created(
        &mut self,
        email: Email,
        account_id: UserAccountId,
        profile: RegistrationProfile,
        created_at: DateTime<Utc>,
        sink: &EventSink<Self>,
    ) -> Result<(), EmailReservationError> {
        match self.phase() {
            ReservationPhase::CreatingAccount {
                account_id: current,
                accepted_profile,
                ..
            } if *current == account_id && *accepted_profile == profile => {}
            ReservationPhase::AccountCreated {
                account_id: current,
                accepted_profile,
                created_at: original,
                ..
            } if *current == account_id && *accepted_profile == profile && *original == created_at => {
                return Err(EmailReservationError::AccountCreationAlreadyCompleted);
            }
            ReservationPhase::Absent => return Err(EmailReservationError::NotReserved),
            ReservationPhase::AwaitingCode { .. } | ReservationPhase::AwaitingProfile { .. } => {
                return Err(EmailReservationError::NotAwaitingProfile);
            }
            _ => return Err(EmailReservationError::AccountCreationMismatch),
        }
        sink.write(
            EmailReservationEvent::AccountCreationCompleted {
                email,
                account_id,
                created_at,
            },
            self,
        )
        .await;
        Ok(())
    }
}

fn ensure_code_format(code: u32) -> Result<(), EmailReservationError> {
    if (1_000_000..=9_999_999).contains(&code) {
        Ok(())
    } else {
        Err(EmailReservationError::InvalidVerificationCodeFormat)
    }
}
