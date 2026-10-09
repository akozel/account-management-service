use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use cqrs_runtime::{
    command::{AggregateConflictRetryPolicy, CommandGatewayError, CqrsCommandExecutor},
    outbox::NewOutboxTask,
};

use crate::{
    application::email_reservation::gateway::EmailReservationGateway,
    domain::{EmailReservation, EmailReservationCommand, EmailReservationError},
};

pub struct CqrsEmailReservationGateway<BuildFramework> {
    executor: CqrsCommandExecutor<EmailReservation, BuildFramework>,
}

impl<BuildFramework> CqrsEmailReservationGateway<BuildFramework> {
    pub fn new(build_framework: BuildFramework, retry_policy: AggregateConflictRetryPolicy) -> Self {
        Self {
            executor: CqrsCommandExecutor::new(build_framework, retry_policy),
        }
    }
}

#[async_trait]
impl<BuildFramework, ES> EmailReservationGateway for CqrsEmailReservationGateway<BuildFramework>
where
    BuildFramework: Fn(Vec<NewOutboxTask>) -> CqrsFramework<EmailReservation, ES> + Send + Sync,
    ES: EventStore<EmailReservation> + 'static,
    ES::AC: Send,
{
    async fn execute(&self, command: EmailReservationCommand) -> Result<(), CommandGatewayError<EmailReservationError>> {
        self.execute_with_outbox(command, Vec::new()).await
    }

    async fn execute_with_outbox(
        &self,
        command: EmailReservationCommand,
        tasks: Vec<NewOutboxTask>,
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
