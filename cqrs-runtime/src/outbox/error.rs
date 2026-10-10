use thiserror::Error;
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueueRequestError {
    #[error("queue batch limit is not representable")]
    InvalidLimit,
    #[error("lease duration must be positive and representable")]
    InvalidLeaseDuration,
    #[error("retry delay is not representable")]
    InvalidRetryDelay,
    #[error("execution budget must be positive and representable")]
    InvalidExecutionBudget,
    #[error("queue returned structurally invalid delivery data")]
    InvalidDelivery,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OutboxError {
    #[error(transparent)]
    InvalidRequest(#[from] QueueRequestError),
    #[error("outbox storage is unavailable")]
    Unavailable,
    #[error("outbox storage operation timed out; its result is unknown")]
    TimedOut,
    #[error("outbox lease budget exhausted; task requires durable recovery")]
    LeaseBudgetExhausted,
}
