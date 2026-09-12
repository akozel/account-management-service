//! Application use cases and ports.

use thiserror::Error;

pub mod email_reservation;
pub mod outbox;
pub mod user_account;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CommandGatewayError<D> {
    #[error("domain command failed a domain rule: {0}")]
    Domain(D),
    #[error("domain command conflicted with a concurrent change")]
    Conflict,
    #[error("command gateway is temporarily unavailable")]
    Unavailable,
}
