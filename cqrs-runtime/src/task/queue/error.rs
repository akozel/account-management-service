use thiserror::Error;
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueueRequestError {
    #[error("queue batch limit is not representable")]
    InvalidLimit,
    #[error("lease duration must be positive and representable")]
    InvalidLeaseDuration,
    #[error("retry delay is not representable")]
    InvalidRetryDelay,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskProcessingError {
    #[error(transparent)]
    InvalidRequest(#[from] QueueRequestError),
    #[error("queue returned structurally invalid delivery data")]
    InvalidDelivery,
    #[error("execution budget must be positive and representable")]
    InvalidExecutionBudget,
    #[error("task queue storage is unavailable")]
    Unavailable,
    #[error("task queue storage operation timed out; its result is unknown")]
    TimedOut,
    #[error("task lease budget exhausted; task requires durable recovery")]
    LeaseBudgetExhausted,
}
