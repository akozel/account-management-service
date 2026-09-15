use sha2::{Digest, Sha256};

use crate::{
    application::{
        CommandGatewayError,
        email_reservation::{
            gateway::EmailReservationGateway,
            service::{EmailReservationServiceError, RegistrationMaterialGenerator, VerifiedEmail},
        },
    },
    domain::{Email, EmailReservationCommand, EmailReservationError, UserAccountId},
};

pub(crate) async fn execute(
    gateway: &dyn EmailReservationGateway,
    generator: &dyn RegistrationMaterialGenerator,
    email: Email,
    account_id: UserAccountId,
    code: u32,
) -> Result<VerifiedEmail, EmailReservationServiceError> {
    if !(1_000_000..=9_999_999).contains(&code) {
        return Err(EmailReservationServiceError::InvalidVerificationCode);
    }
    let account_creation_token = generator.account_creation_token();
    let token_digest = hex::encode(Sha256::digest(account_creation_token.as_bytes()));
    gateway
        .execute(EmailReservationCommand::VerifyEmail {
            email: email.clone(),
            account_id,
            code,
            token_digest,
        })
        .await
        .map_err(|error| match error {
            CommandGatewayError::Domain(EmailReservationError::NotReserved) => EmailReservationServiceError::NotReserved,
            CommandGatewayError::Domain(EmailReservationError::NotCurrentAccountId) => {
                EmailReservationServiceError::NotCurrentAccountId
            }
            CommandGatewayError::Domain(EmailReservationError::AlreadyVerified) => EmailReservationServiceError::AlreadyVerified,
            CommandGatewayError::Domain(EmailReservationError::AccountCreationStarted) => {
                EmailReservationServiceError::AccountCreationStarted
            }
            CommandGatewayError::Domain(EmailReservationError::InvalidVerificationCode) => {
                EmailReservationServiceError::InvalidVerificationCode
            }
            CommandGatewayError::Domain(error) => EmailReservationServiceError::UnexpectedDomainRejection(error),
            CommandGatewayError::Conflict => EmailReservationServiceError::Conflict,
            CommandGatewayError::Unavailable => EmailReservationServiceError::Unavailable,
        })?;
    Ok(VerifiedEmail {
        email,
        account_id,
        account_creation_token,
    })
}
