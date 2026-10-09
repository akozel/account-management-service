use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use cqrs_runtime::{
    command::{AggregateConflictRetryPolicy, CommandGatewayError, CqrsCommandExecutor},
    outbox::NewOutboxTask,
};

use crate::{
    application::user_account::gateway::UserAccountGateway,
    domain::{UserAccount, UserAccountCommand, UserAccountError},
};

pub struct CqrsUserAccountGateway<BuildFramework> {
    executor: CqrsCommandExecutor<UserAccount, BuildFramework>,
}

impl<BuildFramework> CqrsUserAccountGateway<BuildFramework> {
    pub fn new(build_framework: BuildFramework, retry_policy: AggregateConflictRetryPolicy) -> Self {
        Self {
            executor: CqrsCommandExecutor::new(build_framework, retry_policy),
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
        self.executor.execute(&aggregate_id, command, tasks).await
    }
}
