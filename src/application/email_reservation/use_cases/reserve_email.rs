use crate::{
    application::{
        CommandGatewayError,
        email_reservation::{
            gateway::EmailReservationGateway,
            service::{EmailReservationServiceError, RegistrationMaterialGenerator, ReservedEmail},
            tasks::send_verification_code::v1::SendVerificationCodeTaskV1,
        },
        outbox::NewOutboxTask,
    },
    domain::{Email, EmailReservationCommand, EmailReservationError},
};

pub(crate) async fn execute(
    gateway: &dyn EmailReservationGateway,
    generator: &dyn RegistrationMaterialGenerator,
    email: Email,
) -> Result<ReservedEmail, EmailReservationServiceError> {
    let account_id = generator.account_id();
    let verification_code = generator.verification_code();
    let task = NewOutboxTask::new(SendVerificationCodeTaskV1 {
        email: email.clone(),
        account_id,
        verification_code,
    })
    .map_err(|_| EmailReservationServiceError::Unavailable)?;
    gateway
        .execute_with_outbox(
            EmailReservationCommand::ReserveEmail {
                email: email.clone(),
                account_id,
                verification_code,
            },
            vec![task],
        )
        .await
        .map_err(|error| match error {
            CommandGatewayError::Domain(EmailReservationError::AlreadyReserved) | CommandGatewayError::Conflict => {
                EmailReservationServiceError::AlreadyReserved
            }
            CommandGatewayError::Domain(error) => EmailReservationServiceError::UnexpectedDomainRejection(error),
            CommandGatewayError::Unavailable => EmailReservationServiceError::Unavailable,
        })?;
    Ok(ReservedEmail { email, account_id })
}
