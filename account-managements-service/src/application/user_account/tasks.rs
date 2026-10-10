//! Versioned registration continuation tasks.
//!
//! Aggregate phases establish the registration order: profile acceptance
//! publishes account creation, and the committed account event publishes the
//! completion task. Workers may run concurrently; completion does not wait for
//! the creation task's separate acknowledgement. Target aggregates validate
//! transitions and exact repeats, while handlers classify delivery outcomes.
pub mod create_account;
pub mod record_account_created;

pub use create_account::v1::CreateAccountTaskV1;
pub use record_account_created::v1::RecordAccountCreatedTaskV1;

use crate::application::user_account::service::ContinuationError;
use cqrs_runtime::outbox::TaskHandlerError;

fn delivery_error(error: ContinuationError) -> TaskHandlerError {
    match error {
        ContinuationError::Retry => TaskHandlerError::Retry {
            reason: "registration continuation temporarily unavailable".into(),
            retry_after: None,
        },
        ContinuationError::Permanent => TaskHandlerError::Permanent {
            reason: "registration continuation data does not match".into(),
        },
    }
}
