//! Versioned registration continuation tasks.
pub mod create_account;
pub mod record_account_created;

pub use create_account::v1::CreateAccountTaskV1;
pub use record_account_created::v1::RecordAccountCreatedTaskV1;

use crate::application::user_account::service::ContinuationError;
use cqrs_runtime::outbox::TaskExecutionError;

fn delivery_error(error: ContinuationError) -> TaskExecutionError {
    match error {
        ContinuationError::Retry => TaskExecutionError::Retry {
            reason: "registration continuation temporarily unavailable".into(),
            retry_after: None,
        },
        ContinuationError::Permanent => TaskExecutionError::Permanent {
            reason: "registration continuation data does not match".into(),
        },
    }
}
