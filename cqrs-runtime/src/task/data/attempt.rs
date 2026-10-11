//! Immutable handler-visible input without queue ownership credentials.
use serde_json::Value;
use uuid::Uuid;
use {
    super::validation::{validate_metadata, validate_payload},
    crate::task::data::{format::TaskFormat, validation::TaskValidationError},
};
/// One delivery number's immutable business input. No token or lease clock.
#[derive(Clone, Debug)]
pub struct TaskAttempt {
    id: Uuid,
    format: TaskFormat,
    payload: Value,
    metadata: Value,
    number: i32,
}
impl TaskAttempt {
    pub fn new(id: Uuid, format: TaskFormat, payload: Value, metadata: Value, number: i32) -> Result<Self, TaskValidationError> {
        validate_payload(&payload)?;
        validate_metadata(&metadata)?;
        if number <= 0 {
            return Err(TaskValidationError::InvalidAttempt);
        }
        Ok(Self {
            id,
            format,
            payload,
            metadata,
            number,
        })
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn format(&self) -> &TaskFormat {
        &self.format
    }
    pub fn payload(&self) -> &Value {
        &self.payload
    }
    pub fn metadata(&self) -> &Value {
        &self.metadata
    }
    pub fn number(&self) -> i32 {
        self.number
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn invalid_attempt_data_is_rejected() {
        let format = TaskFormat::new("test", 1).unwrap();
        let build = |payload, metadata, attempt| TaskAttempt::new(Uuid::new_v4(), format.clone(), payload, metadata, attempt);
        assert!(build(json!([]), json!({}), 1).is_err());
        assert!(build(json!({}), json!([]), 1).is_err());
        assert!(build(json!({}), json!({}), 0).is_err());
    }
}
