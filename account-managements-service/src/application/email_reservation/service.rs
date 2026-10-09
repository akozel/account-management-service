use super::{gateway::EmailReservationGateway, use_cases};
use crate::domain::{Email, EmailReservationError, UserAccountId};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EmailReservationServiceError {
    #[error("email is already reserved")]
    AlreadyReserved,
    #[error("email is not reserved")]
    NotReserved,
    #[error("account ID is not current")]
    NotCurrentAccountId,
    #[error("email is already verified")]
    AlreadyVerified,
    #[error("account creation has already started")]
    AccountCreationStarted,
    #[error("verification code does not match")]
    InvalidVerificationCode,
    #[error("email reservation changed concurrently")]
    Conflict,
    #[error("email reservation is temporarily unavailable")]
    Unavailable,
    #[error("email reservation encountered an unexpected domain rejection: {0}")]
    UnexpectedDomainRejection(EmailReservationError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReservedEmail {
    pub email: Email,
    pub account_id: UserAccountId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedEmail {
    pub email: Email,
    pub account_id: UserAccountId,
    pub account_creation_token: String,
}

pub trait RegistrationMaterialGenerator: Send + Sync {
    fn account_id(&self) -> UserAccountId;
    fn verification_code(&self) -> u32;
    fn account_creation_token(&self) -> String;
}

#[async_trait]
pub trait EmailReservationService: Send + Sync {
    async fn reserve(&self, email: Email) -> Result<ReservedEmail, EmailReservationServiceError>;
    async fn request_new_verification_code(&self, email: Email) -> Result<(), EmailReservationServiceError>;
    async fn confirm_email(
        &self,
        email: Email,
        account_id: UserAccountId,
        code: u32,
    ) -> Result<VerifiedEmail, EmailReservationServiceError>;
}

pub fn email_reservation_service(
    gateway: Arc<dyn EmailReservationGateway>,
    generator: Arc<dyn RegistrationMaterialGenerator>,
) -> Arc<dyn EmailReservationService> {
    Arc::new(DefaultEmailReservationService { gateway, generator })
}

struct DefaultEmailReservationService {
    gateway: Arc<dyn EmailReservationGateway>,
    generator: Arc<dyn RegistrationMaterialGenerator>,
}

#[async_trait]
impl EmailReservationService for DefaultEmailReservationService {
    async fn reserve(&self, email: Email) -> Result<ReservedEmail, EmailReservationServiceError> {
        use_cases::reserve_email::execute(self.gateway.as_ref(), self.generator.as_ref(), email).await
    }

    async fn request_new_verification_code(&self, email: Email) -> Result<(), EmailReservationServiceError> {
        use_cases::request_new_verification_code::execute(self.gateway.as_ref(), self.generator.as_ref(), email).await
    }

    async fn confirm_email(
        &self,
        email: Email,
        account_id: UserAccountId,
        code: u32,
    ) -> Result<VerifiedEmail, EmailReservationServiceError> {
        use_cases::confirm_email::execute(self.gateway.as_ref(), self.generator.as_ref(), email, account_id, code).await
    }
}
