//! Wire-format adaptation before invoking one business operation.
use crate::task::{
    data::{attempt::TaskAttempt, format::TaskFormat},
    handling::handler::TaskHandlerError,
};
use async_trait::async_trait;

/// An adapter advertises supported formats and invokes their business handler.
/// JSON adapters decode one DTO version; custom adapters can convert several
/// versions to the same business input. They preserve the attempt context and
/// add no retry, settlement or workflow sequencing.
#[async_trait]
pub trait TaskAdapter: Send {
    fn formats(&self) -> Vec<TaskFormat>;
    async fn invoke(&mut self, attempt: &TaskAttempt) -> Result<(), TaskHandlerError>;
}
