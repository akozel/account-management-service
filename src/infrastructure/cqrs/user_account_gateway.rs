use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};

use super::map_aggregate_error;

use crate::{
    application::{CommandGatewayError, outbox::NewOutboxTask, user_account::gateway::UserAccountGateway},
    domain::{UserAccount, UserAccountCommand, UserAccountError},
};

pub struct CqrsUserAccountGateway<BuildFramework> {
    build_framework: BuildFramework,
}

impl<BuildFramework> CqrsUserAccountGateway<BuildFramework> {
    pub fn new(build_framework: BuildFramework) -> Self {
        Self { build_framework }
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
        (self.build_framework)(tasks)
            .execute(&account_id.to_string(), command)
            .await
            .map_err(map_aggregate_error)
    }
}
