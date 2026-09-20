use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};

use super::{AggregateConflictRetryPolicy, map_aggregate_error, retry_on_aggregate_conflict};

use crate::{
    application::{CommandGatewayError, outbox::NewOutboxTask, user_account::gateway::UserAccountGateway},
    domain::{UserAccount, UserAccountCommand, UserAccountError},
};

pub struct CqrsUserAccountGateway<BuildFramework> {
    build_framework: BuildFramework,
    retry_policy: AggregateConflictRetryPolicy,
}

impl<BuildFramework> CqrsUserAccountGateway<BuildFramework> {
    pub fn new(build_framework: BuildFramework, retry_policy: AggregateConflictRetryPolicy) -> Self {
        Self {
            build_framework,
            retry_policy,
        }
    }
}

#[async_trait]
impl<BuildFramework, ES> UserAccountGateway for CqrsUserAccountGateway<BuildFramework>
where
    BuildFramework: Fn(Vec<NewOutboxTask>) -> CqrsFramework<UserAccount, ES> + Send + Sync,
    ES: EventStore<UserAccount> + 'static,
    ES::AC: Send,
{
    async fn execute_with_outbox(
        &self,
        command: UserAccountCommand,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<UserAccountError>> {
        let UserAccountCommand::CreateAccount { account_id, .. } = &command;
        let aggregate_id = account_id.to_string();
        let framework = (self.build_framework)(tasks);
        retry_on_aggregate_conflict(self.retry_policy, || framework.execute(&aggregate_id, command.clone()))
            .await
            .map_err(map_aggregate_error)
    }
}
