use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};

use super::{AggregateConflictRetryPolicy, map_aggregate_error, retry_on_aggregate_conflict};

use crate::{
    application::{CommandGatewayError, email_reservation::gateway::EmailReservationGateway, outbox::NewOutboxTask},
    domain::{EmailReservation, EmailReservationCommand, EmailReservationError},
};

pub struct CqrsEmailReservationGateway<BuildFramework> {
    build_framework: BuildFramework,
    retry_policy: AggregateConflictRetryPolicy,
}

impl<BuildFramework> CqrsEmailReservationGateway<BuildFramework> {
    pub fn new(build_framework: BuildFramework, retry_policy: AggregateConflictRetryPolicy) -> Self {
        Self {
            build_framework,
            retry_policy,
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
        let framework = (self.build_framework)(tasks);
        retry_on_aggregate_conflict(self.retry_policy, || framework.execute(&aggregate_id, command.clone()))
            .await
            .map_err(map_aggregate_error)
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
