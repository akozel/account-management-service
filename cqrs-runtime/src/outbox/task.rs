//! Validated, versioned durable work. Event sequence is provenance, not FIFO.
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use std::{fmt, time::Duration};
use thiserror::Error;
use uuid::Uuid;

/// Versioned wire DTO. Retain supported versions while queued work exists.
pub trait TaskPayload: Serialize {
    const TASK_TYPE: &'static str;
    const TASK_VERSION: i32;
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskFormatError {
    #[error("task type must not be empty")]
    EmptyType,
    #[error("task version must be positive")]
    InvalidVersion,
}

/// One durable type/version pair; also the registry's routing key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskFormat {
    task_type: String,
    task_version: i32,
}
impl TaskFormat {
    pub fn new(task_type: impl Into<String>, task_version: i32) -> Result<Self, TaskFormatError> {
        let task_type = task_type.into();
        if task_type.is_empty() {
            return Err(TaskFormatError::EmptyType);
        }
        if task_version <= 0 {
            return Err(TaskFormatError::InvalidVersion);
        }
        Ok(Self { task_type, task_version })
    }
    pub fn of<T: TaskPayload>() -> Result<Self, TaskFormatError> {
        Self::new(T::TASK_TYPE, T::TASK_VERSION)
    }
    pub fn task_type(&self) -> &str {
        &self.task_type
    }
    pub fn task_version(&self) -> i32 {
        self.task_version
    }
}
impl fmt::Display for TaskFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} v{}", self.task_type, self.task_version)
    }
}

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
pub struct NewOutboxTask {
    id: Uuid,
    format: TaskFormat,
    payload: Value,
    metadata: Value,
    schedule: TaskSchedule,
    max_attempts: i32,
}
impl NewOutboxTask {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::test_support::DummyTask;
    use serde::Serializer;
    #[test]
    fn construction_validates_format_objects_attempts_and_schedule() {
        let task = NewOutboxTask::new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(task.payload(), &json!({"value": "runtime"}));
        assert_eq!(task.format().task_version(), 1);
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
        assert!(NewOutboxTask::from_payload(TaskFormat::new("valid", 1).unwrap(), json!([])).is_err());
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
        assert_eq!(NewOutboxTask::new(Broken), Err(TaskValidationError::Serialization));
    }
}
