use std::sync::Arc;

use async_trait::async_trait;
use cqrs_runtime::{
    command::{CommandExecutor, CommandGatewayError},
    task::data::new_task::NewTask,
};

use crate::{
    application::email_reservation::gateway::EmailReservationGateway,
    domain::{EmailReservation, EmailReservationCommand, EmailReservationError},
};

pub struct CqrsEmailReservationGateway {
    executor: Arc<dyn CommandExecutor<EmailReservation>>,
}

impl CqrsEmailReservationGateway {
    pub fn new(executor: Arc<dyn CommandExecutor<EmailReservation>>) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl EmailReservationGateway for CqrsEmailReservationGateway {
    async fn execute(&self, command: EmailReservationCommand) -> Result<(), CommandGatewayError<EmailReservationError>> {
        self.execute_with_outbox(command, Vec::new()).await
    }

    async fn execute_with_outbox(
        &self,
        command: EmailReservationCommand,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<EmailReservationError>> {
        let aggregate_id = aggregate_id(&command);
        self.executor.execute(&aggregate_id, command, tasks).await
    }
}

fn aggregate_id(command: &EmailReservationCommand) -> String {
    match command {
        EmailReservationCommand::ReserveEmail { email, .. }
        | EmailReservationCommand::RequestVerificationCode { email, .. }
        | EmailReservationCommand::VerifyEmail { email, .. }
        | EmailReservationCommand::SubmitRegistrationProfile { email, .. }
        | EmailReservationCommand::RecordAccountCreated { email, .. } => email.to_string(),
    }
}
