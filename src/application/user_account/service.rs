use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    application::{
        CommandGatewayError,
        email_reservation::gateway::EmailReservationGateway,
        outbox::NewOutboxTask,
        user_account::{
            gateway::UserAccountGateway,
            tasks::{CreateAccountTaskV1, RecordAccountCreatedTaskV1},
        },
    },
    domain::{
        Email, EmailReservationCommand, EmailReservationError, RegistrationProfile, UserAccountCommand, UserAccountError,
        UserAccountId,
    },
};

pub trait RegistrationClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Clone)]
pub struct ProfileSubmission {
    pub email: Email,
    pub account_id: UserAccountId,
    pub account_creation_token: String,
    pub first_name: String,
    pub last_name: String,
    pub date_of_birth: NaiveDate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedSubmission {
    pub email: Email,
    pub account_id: UserAccountId,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SubmissionError {
    #[error("registration rejected: {0}")]
    Domain(EmailReservationError),
    #[error("registration changed concurrently")]
    Conflict,
    #[error("registration is temporarily unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ContinuationError {
    #[error("temporary registration continuation failure")]
    Retry,
    #[error("registration continuation data does not match")]
    Permanent,
}

#[async_trait]
pub trait RegistrationService: Send + Sync {
    async fn submit_profile(&self, submission: ProfileSubmission) -> Result<AcceptedSubmission, SubmissionError>;
    async fn create_account(&self, task: CreateAccountTaskV1) -> Result<(), ContinuationError>;
    async fn record_account_created(&self, task: RecordAccountCreatedTaskV1) -> Result<(), ContinuationError>;
}

pub fn registration_service(
    email_gateway: Arc<dyn EmailReservationGateway>,
    account_gateway: Arc<dyn UserAccountGateway>,
    clock: Arc<dyn RegistrationClock>,
) -> Arc<dyn RegistrationService> {
    Arc::new(DefaultRegistrationService {
        email_gateway,
        account_gateway,
        clock,
    })
}

struct DefaultRegistrationService {
    email_gateway: Arc<dyn EmailReservationGateway>,
    account_gateway: Arc<dyn UserAccountGateway>,
    clock: Arc<dyn RegistrationClock>,
}

fn digest(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

#[async_trait]
impl RegistrationService for DefaultRegistrationService {
    async fn submit_profile(&self, submission: ProfileSubmission) -> Result<AcceptedSubmission, SubmissionError> {
        let profile = RegistrationProfile::new(
            submission.first_name,
            submission.last_name,
            submission.date_of_birth,
            self.clock.now().date_naive(),
        );
        let command = EmailReservationCommand::SubmitRegistrationProfile {
            email: submission.email.clone(),
            account_id: submission.account_id,
            token_digest: digest(&submission.account_creation_token),
            profile: profile.clone(),
        };
        let task = NewOutboxTask::new(CreateAccountTaskV1 {
            email: submission.email.clone(),
            account_id: submission.account_id,
            profile,
        })
        .map_err(|_| SubmissionError::Unavailable)?;
        match self.email_gateway.execute_with_outbox(command, vec![task]).await {
            Ok(()) => Ok(AcceptedSubmission {
                email: submission.email,
                account_id: submission.account_id,
            }),
            Err(CommandGatewayError::Conflict) => Err(SubmissionError::Conflict),
            Err(CommandGatewayError::Unavailable) => Err(SubmissionError::Unavailable),
            Err(CommandGatewayError::Domain(error)) => Err(SubmissionError::Domain(error)),
        }
    }

    async fn create_account(&self, task: CreateAccountTaskV1) -> Result<(), ContinuationError> {
        // Select the creation time once; both the command and follow-up Task use it.
        let created_at = self.clock.now();
        let completion = NewOutboxTask::new(RecordAccountCreatedTaskV1 {
            email: task.email.clone(),
            account_id: task.account_id,
            profile: task.profile.clone(),
            created_at,
        })
        .map_err(|_| ContinuationError::Permanent)?;
        let command = UserAccountCommand::CreateAccount {
            account_id: task.account_id,
            email: task.email,
            first_name: task.profile.first_name,
            last_name: task.profile.last_name,
            date_of_birth: task.profile.date_of_birth,
            validation_date: task.profile.validation_date,
            created_at,
        };
        match self.account_gateway.execute_with_outbox(command, vec![completion]).await {
            Ok(()) | Err(CommandGatewayError::Domain(UserAccountError::AlreadyCreated)) => Ok(()),
            Err(CommandGatewayError::Conflict | CommandGatewayError::Unavailable) => Err(ContinuationError::Retry),
            Err(CommandGatewayError::Domain(_)) => Err(ContinuationError::Permanent),
        }
    }

    async fn record_account_created(&self, task: RecordAccountCreatedTaskV1) -> Result<(), ContinuationError> {
        let command = EmailReservationCommand::RecordAccountCreated {
            email: task.email,
            account_id: task.account_id,
            profile: task.profile,
            created_at: task.created_at,
        };
        match self.email_gateway.execute(command).await {
            Ok(()) | Err(CommandGatewayError::Domain(EmailReservationError::AccountCreationAlreadyCompleted)) => Ok(()),
            Err(CommandGatewayError::Conflict | CommandGatewayError::Unavailable) => Err(ContinuationError::Retry),
            Err(CommandGatewayError::Domain(_)) => Err(ContinuationError::Permanent),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct ConflictEmailGateway {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl EmailReservationGateway for ConflictEmailGateway {
        async fn execute(&self, _: EmailReservationCommand) -> Result<(), CommandGatewayError<EmailReservationError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(CommandGatewayError::Conflict)
        }

        async fn execute_with_outbox(
            &self,
            _: EmailReservationCommand,
            tasks: Vec<NewOutboxTask>,
        ) -> Result<(), CommandGatewayError<EmailReservationError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(tasks.len(), 1);
            Err(CommandGatewayError::Conflict)
        }
    }

    struct ConflictAccountGateway;

    #[async_trait]
    impl UserAccountGateway for ConflictAccountGateway {
        async fn execute_with_outbox(
            &self,
            _: UserAccountCommand,
            _: Vec<NewOutboxTask>,
        ) -> Result<(), CommandGatewayError<UserAccountError>> {
            Err(CommandGatewayError::Conflict)
        }
    }

    struct FixedClock;

    impl RegistrationClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            "2026-09-20T12:00:00Z".parse().unwrap()
        }
    }

    #[tokio::test]
    async fn profile_submission_delegates_conflict_retry_to_the_gateway() {
        let email_gateway = Arc::new(ConflictEmailGateway {
            calls: AtomicUsize::new(0),
        });
        let service = registration_service(email_gateway.clone(), Arc::new(ConflictAccountGateway), Arc::new(FixedClock));

        let result = service
            .submit_profile(ProfileSubmission {
                email: Email::parse("alice@example.com").unwrap(),
                account_id: "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse().unwrap(),
                account_creation_token: "token".into(),
                first_name: "Alice".into(),
                last_name: "Smith".into(),
                date_of_birth: NaiveDate::from_ymd_opt(1990, 5, 12).unwrap(),
            })
            .await;

        assert_eq!(result, Err(SubmissionError::Conflict));
        assert_eq!(email_gateway.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn internal_continuations_keep_exhausted_conflicts_retryable() {
        let email_gateway = Arc::new(ConflictEmailGateway {
            calls: AtomicUsize::new(0),
        });
        let service = registration_service(email_gateway.clone(), Arc::new(ConflictAccountGateway), Arc::new(FixedClock));
        let email = Email::parse("alice@example.com").unwrap();
        let account_id = "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse().unwrap();
        let profile = RegistrationProfile::new(
            "Alice".into(),
            "Smith".into(),
            NaiveDate::from_ymd_opt(1990, 5, 12).unwrap(),
            NaiveDate::from_ymd_opt(2026, 9, 20).unwrap(),
        );

        assert_eq!(
            service
                .create_account(CreateAccountTaskV1 {
                    email: email.clone(),
                    account_id,
                    profile: profile.clone(),
                })
                .await,
            Err(ContinuationError::Retry)
        );
        assert_eq!(
            service
                .record_account_created(RecordAccountCreatedTaskV1 {
                    email,
                    account_id,
                    profile,
                    created_at: FixedClock.now(),
                })
                .await,
            Err(ContinuationError::Retry)
        );
        assert_eq!(email_gateway.calls.load(Ordering::SeqCst), 1);
    }
}
