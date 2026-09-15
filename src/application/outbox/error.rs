use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OutboxError {
    #[error("invalid outbox task")]
    InvalidTask,
    #[error("outbox storage is unavailable")]
    Unavailable,
}
