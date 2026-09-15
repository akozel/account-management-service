use crate::{
    application::{
        CommandGatewayError,
        email_reservation::{
            gateway::EmailReservationGateway,
            service::{EmailReservationServiceError, RegistrationMaterialGenerator},
            tasks::send_verification_code::v1::SendVerificationCodeTaskV1,
            use_cases::CONFLICT_RETRIES,
        },
        outbox::NewOutboxTask,
    },
    domain::{Email, EmailReservationCommand, EmailReservationError},
};

pub(crate) async fn execute(
    gateway: &dyn EmailReservationGateway,
    generator: &dyn RegistrationMaterialGenerator,
    email: Email,
) -> Result<(), EmailReservationServiceError> {
    for _ in 0..CONFLICT_RETRIES {
        let account_id = generator.account_id();
        let verification_code = generator.verification_code();
        let task = NewOutboxTask::new(SendVerificationCodeTaskV1 {
            email: email.clone(),
            account_id,
            verification_code,
        })
        .map_err(|_| EmailReservationServiceError::Unavailable)?;
        match gateway
            .execute_with_outbox(
                EmailReservationCommand::RequestVerificationCode {
                    email: email.clone(),
                    account_id,
                    verification_code,
                },
                vec![task],
            )
            .await
        {
            Ok(()) => return Ok(()),
            Err(CommandGatewayError::Conflict | CommandGatewayError::Domain(EmailReservationError::AccountIdUnchanged)) => {
                continue;
            }
            Err(CommandGatewayError::Domain(EmailReservationError::NotReserved)) => {
                return Err(EmailReservationServiceError::NotReserved);
            }
            Err(CommandGatewayError::Domain(EmailReservationError::AccountCreationStarted)) => {
                return Err(EmailReservationServiceError::AccountCreationStarted);
            }
            Err(CommandGatewayError::Domain(error)) => {
                return Err(EmailReservationServiceError::UnexpectedDomainRejection(error));
            }
            Err(CommandGatewayError::Unavailable) => return Err(EmailReservationServiceError::Unavailable),
        }
    }
    Err(EmailReservationServiceError::Conflict)
}
