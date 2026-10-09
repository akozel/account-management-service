use cqrs_es::{Aggregate, event_sink::EventSink};

use super::{EmailReservation, EmailReservationCommand, EmailReservationError, EmailReservationEvent, ReservationPhase};

impl Aggregate for EmailReservation {
    const TYPE: &'static str = "email_reservation";
    type Command = EmailReservationCommand;
    type Event = EmailReservationEvent;
    type Error = EmailReservationError;
    type Services = ();

    async fn handle(
        &mut self,
        command: Self::Command,
        _service: &Self::Services,
        sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
        match command {
            EmailReservationCommand::ReserveEmail {
                email,
                account_id,
                verification_code,
            } => self.handle_reserve(email, account_id, verification_code, sink).await,
            EmailReservationCommand::RequestVerificationCode {
                email,
                account_id,
                verification_code,
            } => {
                self.handle_request_verification_code(email, account_id, verification_code, sink)
                    .await
            }
            EmailReservationCommand::VerifyEmail {
                email,
                account_id,
                code,
                token_digest,
            } => self.handle_verify(email, account_id, code, token_digest, sink).await,
            EmailReservationCommand::SubmitRegistrationProfile {
                email,
                account_id,
                token_digest,
                profile,
            } => {
                self.handle_submit_profile(email, account_id, token_digest, profile, sink)
                    .await
            }
            EmailReservationCommand::RecordAccountCreated {
                email,
                account_id,
                profile,
                created_at,
            } => {
                self.handle_record_account_created(email, account_id, profile, created_at, sink)
                    .await
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        self.phase = match event {
            EmailReservationEvent::EmailReserved {
                account_id,
                verification_code,
                ..
            }
            | EmailReservationEvent::VerificationCodeRequested {
                account_id,
                verification_code,
                ..
            }
            | EmailReservationEvent::VerificationRestarted {
                account_id,
                verification_code,
                ..
            } => ReservationPhase::AwaitingCode {
                account_id,
                verification_code,
            },
            EmailReservationEvent::EmailVerified {
                account_id,
                token_digest,
                ..
            } => ReservationPhase::AwaitingProfile {
                account_id,
                token_digest,
            },
            EmailReservationEvent::RegistrationProfileAccepted {
                account_id,
                consumed_token_digest,
                profile,
                ..
            } => ReservationPhase::CreatingAccount {
                account_id,
                consumed_token_digest,
                accepted_profile: profile,
            },
            EmailReservationEvent::AccountCreationCompleted {
                account_id, created_at, ..
            } => {
                let ReservationPhase::CreatingAccount {
                    consumed_token_digest,
                    accepted_profile,
                    ..
                } = &self.phase
                else {
                    panic!("completion requires an accepted profile")
                };
                ReservationPhase::AccountCreated {
                    account_id,
                    consumed_token_digest: consumed_token_digest.clone(),
                    accepted_profile: accepted_profile.clone(),
                    created_at,
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, Utc};
    use cqrs_es::{Aggregate, test::TestFramework};

    use super::*;
    use crate::domain::{Email, RegistrationProfile, UserAccountId};

    fn email() -> Email {
        Email::parse("alice@example.com").unwrap()
    }
    fn id(value: u128) -> UserAccountId {
        UserAccountId::from_uuid(uuid::Uuid::from_u128(value))
    }
    fn reserved() -> EmailReservationEvent {
        EmailReservationEvent::EmailReserved {
            email: email(),
            account_id: id(1),
            verification_code: 1_234_567,
        }
    }
    fn verified() -> EmailReservationEvent {
        EmailReservationEvent::EmailVerified {
            email: email(),
            account_id: id(1),
            token_digest: "digest".into(),
        }
    }

    fn profile() -> RegistrationProfile {
        RegistrationProfile::new(
            " Alice ".into(),
            " Smith ".into(),
            NaiveDate::from_ymd_opt(1990, 5, 12).unwrap(),
            NaiveDate::from_ymd_opt(2026, 9, 14).unwrap(),
        )
    }

    fn created_at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-14T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn accepted() -> EmailReservationEvent {
        EmailReservationEvent::RegistrationProfileAccepted {
            email: email(),
            account_id: id(1),
            consumed_token_digest: "digest".into(),
            profile: profile(),
        }
    }

    fn completed() -> EmailReservationEvent {
        EmailReservationEvent::AccountCreationCompleted {
            email: email(),
            account_id: id(1),
            created_at: created_at(),
        }
    }

    #[test]
    fn reserve_requires_absent_email_and_valid_code() {
        TestFramework::<EmailReservation>::with(())
            .given_no_previous_events()
            .when(EmailReservationCommand::ReserveEmail {
                email: email(),
                account_id: id(1),
                verification_code: 1_234_567,
            })
            .then_expect_events(vec![reserved()]);
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved()])
            .when(EmailReservationCommand::ReserveEmail {
                email: email(),
                account_id: id(2),
                verification_code: 7_654_321,
            })
            .then_expect_error(EmailReservationError::AlreadyReserved);
        TestFramework::<EmailReservation>::with(())
            .given_no_previous_events()
            .when(EmailReservationCommand::ReserveEmail {
                email: email(),
                account_id: id(1),
                verification_code: 12,
            })
            .then_expect_error(EmailReservationError::InvalidVerificationCodeFormat);
    }

    #[test]
    fn reissue_changes_id_even_when_numeric_code_repeats() {
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved()])
            .when(EmailReservationCommand::RequestVerificationCode {
                email: email(),
                account_id: id(2),
                verification_code: 1_234_567,
            })
            .then_expect_events(vec![EmailReservationEvent::VerificationCodeRequested {
                email: email(),
                account_id: id(2),
                verification_code: 1_234_567,
            }]);
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved(), verified()])
            .when(EmailReservationCommand::RequestVerificationCode {
                email: email(),
                account_id: id(2),
                verification_code: 7_654_321,
            })
            .then_expect_events(vec![EmailReservationEvent::VerificationRestarted {
                email: email(),
                account_id: id(2),
                verification_code: 7_654_321,
            }]);
        for (events, next_id, code, error) in [
            (vec![], id(2), 7_654_321, EmailReservationError::NotReserved),
            (vec![reserved()], id(1), 7_654_321, EmailReservationError::AccountIdUnchanged),
            (
                vec![reserved()],
                id(2),
                12,
                EmailReservationError::InvalidVerificationCodeFormat,
            ),
        ] {
            TestFramework::<EmailReservation>::with(())
                .given(events)
                .when(EmailReservationCommand::RequestVerificationCode {
                    email: email(),
                    account_id: next_id,
                    verification_code: code,
                })
                .then_expect_error(error);
        }
    }

    #[test]
    fn verification_uses_current_pair_and_issues_one_digest() {
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved()])
            .when(EmailReservationCommand::VerifyEmail {
                email: email(),
                account_id: id(1),
                code: 1_234_567,
                token_digest: "digest".into(),
            })
            .then_expect_events(vec![verified()]);
        for (events, account_id, code, error) in [
            (vec![], id(1), 1_234_567, EmailReservationError::NotReserved),
            (vec![reserved()], id(2), 1_234_567, EmailReservationError::NotCurrentAccountId),
            (
                vec![reserved()],
                id(1),
                7_654_321,
                EmailReservationError::InvalidVerificationCode,
            ),
            (
                vec![reserved(), verified()],
                id(1),
                1_234_567,
                EmailReservationError::AlreadyVerified,
            ),
            (
                vec![reserved(), verified()],
                id(2),
                1_234_567,
                EmailReservationError::NotCurrentAccountId,
            ),
        ] {
            TestFramework::<EmailReservation>::with(())
                .given(events)
                .when(EmailReservationCommand::VerifyEmail {
                    email: email(),
                    account_id,
                    code,
                    token_digest: "other".into(),
                })
                .then_expect_error(error);
        }
    }

    #[test]
    fn replay_and_snapshot_keep_only_active_proof() {
        let mut aggregate = EmailReservation::default();
        assert_eq!(aggregate.phase(), &ReservationPhase::Absent);
        assert!(!aggregate.is_reserved());
        assert_eq!(aggregate.account_id(), None);
        aggregate.apply(reserved());
        assert_eq!(aggregate.account_id(), Some(id(1)));
        aggregate.apply(verified());
        assert_eq!(
            aggregate.phase(),
            &ReservationPhase::AwaitingProfile {
                account_id: id(1),
                token_digest: "digest".into()
            }
        );
        aggregate.apply(EmailReservationEvent::VerificationRestarted {
            email: email(),
            account_id: id(2),
            verification_code: 7_654_321,
        });
        assert_eq!(
            aggregate.phase(),
            &ReservationPhase::AwaitingCode {
                account_id: id(2),
                verification_code: 7_654_321
            }
        );
        assert_eq!(
            serde_json::from_value::<EmailReservation>(serde_json::to_value(&aggregate).unwrap()).unwrap(),
            aggregate
        );
    }

    #[test]
    fn profile_submission_requires_current_token_and_valid_profile() {
        let command = |account_id, token_digest: &str, profile| EmailReservationCommand::SubmitRegistrationProfile {
            email: email(),
            account_id,
            token_digest: token_digest.into(),
            profile,
        };
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved(), verified()])
            .when(command(
                id(1),
                "digest",
                RegistrationProfile {
                    first_name: " Alice ".into(),
                    last_name: " Smith ".into(),
                    ..profile()
                },
            ))
            .then_expect_events(vec![accepted()]);
        for (events, id_value, token, error) in [
            (vec![], id(1), "digest", EmailReservationError::NotReserved),
            (vec![reserved()], id(1), "digest", EmailReservationError::NotAwaitingProfile),
            (
                vec![reserved(), verified()],
                id(2),
                "digest",
                EmailReservationError::NotCurrentAccountId,
            ),
            (
                vec![reserved(), verified()],
                id(1),
                "wrong",
                EmailReservationError::InvalidAccountCreationToken,
            ),
        ] {
            TestFramework::<EmailReservation>::with(())
                .given(events)
                .when(command(id_value, token, profile()))
                .then_expect_error(error);
        }
        let mut invalid = profile();
        invalid.first_name = "  ".into();
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved(), verified()])
            .when(command(id(1), "digest", invalid))
            .then_expect_error(EmailReservationError::EmptyFirstName);
        let mut invalid = profile();
        invalid.last_name = "  ".into();
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved(), verified()])
            .when(command(id(1), "digest", invalid))
            .then_expect_error(EmailReservationError::EmptyLastName);
        let mut invalid = profile();
        invalid.date_of_birth = NaiveDate::from_ymd_opt(2026, 9, 15).unwrap();
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved(), verified()])
            .when(command(id(1), "digest", invalid))
            .then_expect_error(EmailReservationError::FutureDateOfBirth);
    }

    #[test]
    fn a_consumed_token_cannot_submit_another_profile() {
        let command = |account_id, token_digest: &str, profile| EmailReservationCommand::SubmitRegistrationProfile {
            email: email(),
            account_id,
            token_digest: token_digest.into(),
            profile,
        };
        for history in [
            vec![reserved(), verified(), accepted()],
            vec![reserved(), verified(), accepted(), completed()],
        ] {
            let mut later = profile();
            later.validation_date = NaiveDate::from_ymd_opt(1980, 1, 1).unwrap();
            TestFramework::<EmailReservation>::with(())
                .given(history.clone())
                .when(command(id(1), "digest", later))
                .then_expect_error(EmailReservationError::ProfileAlreadySubmitted);
            let mut changed = profile();
            changed.last_name = "Jones".into();
            TestFramework::<EmailReservation>::with(())
                .given(history.clone())
                .when(command(id(1), "digest", changed))
                .then_expect_error(EmailReservationError::ProfileAlreadySubmitted);
            TestFramework::<EmailReservation>::with(())
                .given(history.clone())
                .when(command(id(1), "wrong", profile()))
                .then_expect_error(EmailReservationError::InvalidAccountCreationToken);
            TestFramework::<EmailReservation>::with(())
                .given(history)
                .when(command(id(2), "digest", profile()))
                .then_expect_error(EmailReservationError::NotCurrentAccountId);
        }
    }

    #[test]
    fn completion_requires_matching_account_fact_and_is_idempotent() {
        let command = |account_id, profile, created_at| EmailReservationCommand::RecordAccountCreated {
            email: email(),
            account_id,
            profile,
            created_at,
        };
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved(), verified(), accepted()])
            .when(command(id(1), profile(), created_at()))
            .then_expect_events(vec![completed()]);
        TestFramework::<EmailReservation>::with(())
            .given(vec![reserved(), verified(), accepted(), completed()])
            .when(command(id(1), profile(), created_at()))
            .then_expect_error(EmailReservationError::AccountCreationAlreadyCompleted);
        let mut changed = profile();
        changed.first_name = "Bob".into();
        for (history, account_id, profile, created_at, error) in [
            (vec![], id(1), profile(), created_at(), EmailReservationError::NotReserved),
            (
                vec![reserved()],
                id(1),
                profile(),
                created_at(),
                EmailReservationError::NotAwaitingProfile,
            ),
            (
                vec![reserved(), verified()],
                id(1),
                profile(),
                created_at(),
                EmailReservationError::NotAwaitingProfile,
            ),
            (
                vec![reserved(), verified(), accepted()],
                id(2),
                profile(),
                created_at(),
                EmailReservationError::AccountCreationMismatch,
            ),
            (
                vec![reserved(), verified(), accepted()],
                id(1),
                changed,
                created_at(),
                EmailReservationError::AccountCreationMismatch,
            ),
            (
                vec![reserved(), verified(), accepted(), completed()],
                id(1),
                profile(),
                created_at() + chrono::Duration::seconds(1),
                EmailReservationError::AccountCreationMismatch,
            ),
        ] {
            TestFramework::<EmailReservation>::with(())
                .given(history)
                .when(command(account_id, profile, created_at))
                .then_expect_error(error);
        }
    }

    #[test]
    fn creation_phases_block_code_reissue_and_keep_accepted_profile_on_replay() {
        for history in [
            vec![reserved(), verified(), accepted()],
            vec![reserved(), verified(), accepted(), completed()],
        ] {
            TestFramework::<EmailReservation>::with(())
                .given(history.clone())
                .when(EmailReservationCommand::RequestVerificationCode {
                    email: email(),
                    account_id: id(2),
                    verification_code: 7_654_321,
                })
                .then_expect_error(EmailReservationError::AccountCreationStarted);
            TestFramework::<EmailReservation>::with(())
                .given(history.clone())
                .when(EmailReservationCommand::ReserveEmail {
                    email: email(),
                    account_id: id(2),
                    verification_code: 7_654_321,
                })
                .then_expect_error(EmailReservationError::AlreadyReserved);
            let mut aggregate = EmailReservation::default();
            for event in history {
                aggregate.apply(event);
            }
            assert_eq!(aggregate.account_id(), Some(id(1)));
            assert!(aggregate.is_reserved());
            assert_eq!(
                serde_json::from_value::<EmailReservation>(serde_json::to_value(&aggregate).unwrap()).unwrap(),
                aggregate
            );
        }
    }
}
