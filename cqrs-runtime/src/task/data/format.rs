//! Versioned wire DTO contract and checked routing key.
use serde::Serialize;
use std::fmt;
use thiserror::Error;
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
