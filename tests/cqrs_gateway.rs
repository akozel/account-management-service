use account_management_service::{
    application::{CommandGatewayError, email_reservation::gateway::EmailReservationGateway, outbox::NewOutboxTask},
    domain::{Email, EmailReservation, EmailReservationCommand, EmailReservationError, UserAccountId},
    infrastructure::cqrs::email_reservation_gateway::CqrsEmailReservationGateway,
};
use cqrs_es::{CqrsFramework, mem_store::MemStore};

#[tokio::test]
async fn executes_command_and_preserves_domain_rejection() {
    let store = MemStore::<EmailReservation>::default();
    let gateway = CqrsEmailReservationGateway::new(move |tasks: Vec<NewOutboxTask>| {
        assert!(tasks.is_empty());
        CqrsFramework::new(store.clone(), Vec::new(), ())
    });
    let email = Email::parse("alice@example.com").expect("test email must be valid");
    let account_id: UserAccountId = "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse().unwrap();

    assert_eq!(
        gateway
            .execute(EmailReservationCommand::ReserveEmail {
                email: email.clone(),
                account_id,
                verification_code: 1_234_567
            })
            .await,
        Ok(())
    );
    assert_eq!(
        gateway
            .execute(EmailReservationCommand::ReserveEmail {
                email,
                account_id: UserAccountId::new(),
                verification_code: 7_654_321
            })
            .await,
        Err(CommandGatewayError::Domain(EmailReservationError::AlreadyReserved))
    );
}
