//! Checked immutable task publication and its initial schedule.
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;
use {
    super::validation::{validate_metadata, validate_payload},
    crate::task::data::{
        format::{TaskFormat, TaskPayload},
        validation::TaskValidationError,
    },
};
#[derive(Clone, Debug, Default, PartialEq)]
pub enum TaskSchedule {
    #[default]
    Now,
    After(Duration),
    At(DateTime<Utc>),
}
impl TaskSchedule {
    fn validate(&self) -> Result<(), TaskValidationError> {
        match self {
            Self::After(delay) if delay.as_millis() > i64::MAX as u128 => Err(TaskValidationError::InvalidSchedule),
            _ => Ok(()),
        }
    }
}

/// Immutable command-bound work; preserve its generated ID across retries.
#[derive(Clone, Debug, PartialEq)]
pub struct NewTask {
    id: Uuid,
    format: TaskFormat,
    payload: Value,
    metadata: Value,
    schedule: TaskSchedule,
    max_attempts: i32,
}
impl NewTask {
    pub fn new<T: TaskPayload>(payload: T) -> Result<Self, TaskValidationError> {
        Self::from_payload(
            TaskFormat::of::<T>()?,
            serde_json::to_value(payload).map_err(|_| TaskValidationError::Serialization)?,
        )
    }
    /// Dynamic producers must still supply an object and a validated format.
    pub fn from_payload(format: TaskFormat, payload: Value) -> Result<Self, TaskValidationError> {
        validate_payload(&payload)?;
        Ok(Self {
            id: Uuid::new_v4(),
            format,
            payload,
            metadata: json!({}),
            schedule: TaskSchedule::Now,
            max_attempts: 10,
        })
    }
    pub fn scheduled(mut self, schedule: TaskSchedule) -> Result<Self, TaskValidationError> {
        schedule.validate()?;
        self.schedule = schedule;
        Ok(self)
    }
    pub fn with_metadata(mut self, metadata: Value) -> Result<Self, TaskValidationError> {
        validate_metadata(&metadata)?;
        self.metadata = metadata;
        Ok(self)
    }
    /// Total claims, including the initial claim and skipped invocations.
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Result<Self, TaskValidationError> {
        if max_attempts <= 0 {
            return Err(TaskValidationError::InvalidAttemptLimit);
        }
        self.max_attempts = max_attempts;
        Ok(self)
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
    pub fn schedule(&self) -> &TaskSchedule {
        &self.schedule
    }
    pub fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::data::format::TaskFormatError;
    use crate::task::test_support::DummyTask;
    use serde::{Serialize, Serializer};
    #[test]
    fn construction_validates_format_objects_attempts_and_schedule() {
        let task = NewTask::new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(task.payload(), &json!({"value": "runtime"}));
        assert_eq!(task.format().task_version(), 1);
        assert_eq!(task.format().to_string(), "dummy_task v1");
        assert_eq!(task.max_attempts(), 10);
        assert_eq!(task.metadata(), &json!({}));
        assert_eq!(task.schedule(), &TaskSchedule::Now);
        let id = task.id();
        let task = task
            .with_metadata(json!({"trace": "test"}))
            .unwrap()
            .with_max_attempts(2)
            .unwrap()
            .scheduled(TaskSchedule::After(Duration::ZERO))
            .unwrap();
        assert_eq!(task.id(), id);
        assert_eq!(task.max_attempts(), 2);
        assert!(task.clone().with_metadata(json!([])).is_err());
        assert!(task.clone().with_max_attempts(0).is_err());
        assert!(task.clone().scheduled(TaskSchedule::After(Duration::MAX)).is_err());
        assert!(task.scheduled(TaskSchedule::At(Utc::now())).is_ok());
        assert_eq!(TaskFormat::new("", 1), Err(TaskFormatError::EmptyType));
        assert_eq!(TaskFormat::new("valid", 0), Err(TaskFormatError::InvalidVersion));
        assert!(NewTask::from_payload(TaskFormat::new("valid", 1).unwrap(), json!([])).is_err());
        struct Broken;
        impl Serialize for Broken {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("broken"))
            }
        }
        impl TaskPayload for Broken {
            const TASK_TYPE: &'static str = "broken";
            const TASK_VERSION: i32 = 1;
        }
        assert_eq!(NewTask::new(Broken), Err(TaskValidationError::Serialization));
    }

    #[test]
    fn a_payload_with_an_invalid_declared_format_cannot_become_a_task() {
        #[derive(Serialize)]
        struct InvalidFormat;
        impl TaskPayload for InvalidFormat {
            const TASK_TYPE: &'static str = "invalid_format";
            const TASK_VERSION: i32 = 0;
        }
        assert_eq!(
            NewTask::new(InvalidFormat),
            Err(TaskValidationError::Format(TaskFormatError::InvalidVersion))
        );
    }
}
