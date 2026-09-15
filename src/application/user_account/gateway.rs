use async_trait::async_trait;

use crate::{
    application::{CommandGatewayError, outbox::NewOutboxTask},
    domain::{UserAccountCommand, UserAccountError},
};

#[async_trait]
pub trait UserAccountGateway: Send + Sync {
    async fn execute_with_outbox(
        &self,
        command: UserAccountCommand,
        tasks: Vec<NewOutboxTask>,
    ) -> Result<(), CommandGatewayError<UserAccountError>>;
}
