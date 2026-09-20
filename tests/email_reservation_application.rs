use std::sync::{Arc, Mutex};

use account_management_service::{
    application::{
        CommandGatewayError,
        email_reservation::{
            gateway::EmailReservationGateway,
            service::{EmailReservationServiceError, RegistrationMaterialGenerator, email_reservation_service},
        },
        outbox::NewOutboxTask,
    },
    domain::{Email, EmailReservationCommand, EmailReservationError, UserAccountId},
};
use async_trait::async_trait;

struct FixedGenerator;

impl RegistrationMaterialGenerator for FixedGenerator {
    fn account_id(&self) -> UserAccountId {
        "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse().unwrap()
    }

    fn verification_code(&self) -> u32 {
        1_234_567
    }

    fn account_creation_token(&self) -> String {
        unreachable!("these scenarios do not confirm an email")
    }
}

struct FakeGateway {
    result: Mutex<Option<Result<(), CommandGatewayError<EmailReservationError>>>>,
    calls: Mutex<Vec<(EmailReservationCommand, Vec<NewOutboxTask>)>>,
}

impl FakeGateway {
    fn returning(result: Result<(), CommandGatewayError<EmailReservationError>>) -> Self {
        Self {
            result: Mutex::new(Some(result)),
            calls: Mutex::default(),
        }
    }

    fn calls(&self) -> Vec<(EmailReservationCommand, Vec<NewOutboxTask>)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl EmailReservationGateway for FakeGateway {
    async fn execute(&self, command: EmailReservationCommand) -> Result<(), CommandGatewayError<EmailReservationError>> {
        self.execute_with_outbox(command, Vec::new()).await
    }

    async fn execute_with_outbox(
        &self,
        command: EmailReservationCommand,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<EmailReservationError>> {
        self.calls.lock().unwrap().push((command, tasks));
        self.result
            .lock()
            .unwrap()
            .take()
            .expect("application must call the gateway once")
    }
}

#[tokio::test]
async fn reserve_keeps_technical_conflict_distinct_from_already_reserved() {
    for (gateway_error, expected) in [
        (CommandGatewayError::Conflict, EmailReservationServiceError::Conflict),
        (
            CommandGatewayError::Domain(EmailReservationError::AlreadyReserved),
            EmailReservationServiceError::AlreadyReserved,
        ),
    ] {
        let gateway = Arc::new(FakeGateway::returning(Err(gateway_error)));
        let service = email_reservation_service(gateway.clone(), Arc::new(FixedGenerator));

        assert_eq!(
            service.reserve(Email::parse("alice@example.com").unwrap()).await,
            Err(expected)
        );
        let calls = gateway.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1.len(), 1);
        assert!(matches!(calls[0].0, EmailReservationCommand::ReserveEmail { .. }));
    }
}

#[tokio::test]
async fn request_code_does_not_retry_application_conflicts() {
    for gateway_error in [
        CommandGatewayError::Domain(EmailReservationError::AccountIdUnchanged),
        CommandGatewayError::Conflict,
    ] {
        let gateway = Arc::new(FakeGateway::returning(Err(gateway_error)));
        let service = email_reservation_service(gateway.clone(), Arc::new(FixedGenerator));

        assert_eq!(
            service
                .request_new_verification_code(Email::parse("alice@example.com").unwrap())
                .await,
            Err(EmailReservationServiceError::Conflict)
        );
        let calls = gateway.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1.len(), 1);
        assert!(matches!(calls[0].0, EmailReservationCommand::RequestVerificationCode { .. }));
    }
}
