use std::sync::Arc;

use async_trait::async_trait;
use cqrs_runtime::{
    command::{CommandExecutor, CommandGatewayError},
    task::data::new_task::NewTask,
};

use crate::{
    application::user_account::gateway::UserAccountGateway,
    domain::{UserAccount, UserAccountCommand, UserAccountError},
};

pub struct CqrsUserAccountGateway {
    executor: Arc<dyn CommandExecutor<UserAccount>>,
}

impl CqrsUserAccountGateway {
    pub fn new(executor: Arc<dyn CommandExecutor<UserAccount>>) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl UserAccountGateway for CqrsUserAccountGateway {
    async fn execute_with_outbox(
        &self,
        command: UserAccountCommand,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<UserAccountError>> {
        let UserAccountCommand::CreateAccount { account_id, .. } = &command;
        let aggregate_id = account_id.to_string();
        self.executor.execute(&aggregate_id, command, tasks).await
    }
}
