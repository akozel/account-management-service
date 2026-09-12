use crate::application::{CommandGatewayError, outbox::NewOutboxTask};
use crate::domain::{EmailReservationCommand, EmailReservationError};
use async_trait::async_trait;

#[async_trait]
pub trait EmailReservationGateway: Send + Sync {
    async fn execute(&self, command: EmailReservationCommand) -> Result<(), CommandGatewayError<EmailReservationError>>;
    async fn execute_with_outbox(
        &self,
        command: EmailReservationCommand,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<EmailReservationError>>;
}
