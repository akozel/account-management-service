use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OutboxError {
    #[error("invalid outbox task")]
    InvalidTask,
    #[error("outbox storage is unavailable")]
    Unavailable,
    #[error("outbox storage operation timed out; its result is unknown")]
    TimedOut,
    #[error("outbox lease budget exhausted; task requires durable recovery")]
    LeaseBudgetExhausted,
}
