//! Shared structural validation for published tasks and attempts.
use crate::task::data::format::TaskFormatError;
use serde_json::Value;
use thiserror::Error;
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskValidationError {
    #[error(transparent)]
    Format(#[from] TaskFormatError),
    #[error("task payload serialization failed")]
    Serialization,
    #[error("task payload must be a JSON object")]
    PayloadNotObject,
    #[error("task metadata must be a JSON object")]
    MetadataNotObject,
    #[error("task attempt limit must be positive")]
    InvalidAttemptLimit,
    #[error("delivery attempt must be positive")]
    InvalidAttempt,
    #[error("task schedule delay is not representable")]
    InvalidSchedule,
}
pub(super) fn validate_payload(payload: &Value) -> Result<(), TaskValidationError> {
    if payload.is_object() {
        Ok(())
    } else {
        Err(TaskValidationError::PayloadNotObject)
    }
}
pub(super) fn validate_metadata(metadata: &Value) -> Result<(), TaskValidationError> {
    if metadata.is_object() {
        Ok(())
    } else {
        Err(TaskValidationError::MetadataNotObject)
    }
}
