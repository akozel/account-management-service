//! Versioned durable task data for operations permitted by accepted events.
//!
//! Several tasks may originate from the same event and execute concurrently.
//! Business prerequisites belong to aggregate transitions, not task scheduling.
//! An external operation's stable idempotency key can be persisted in its
//! versioned payload or metadata. The task ID identifies repeats of one task;
//! different tasks for one logical effect need a shared operation identity.

use super::error::OutboxError;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

/// Versioned application wire DTO, independently versioned from domain events.
///
/// PostgreSQL requires an object payload, a nonempty type, and a positive version.
/// Keep older DTOs readable while their durable tasks can still be retried.
pub trait TaskPayload: Serialize {
    const TASK_TYPE: &'static str;
    const TASK_VERSION: i32;
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum Schedule {
    #[default]
    Now,
    After(Duration),
    At(DateTime<Utc>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewOutboxTask {
    /// Generated once for durable work; preserve on command retries and requeue.
    pub id: Uuid,
    pub task_type: String,
    pub task_version: i32,
    pub payload: Value,
    pub metadata: Value,
    pub schedule: Schedule,
    /// Total claims, including the initial attempt; skipped execution also counts.
    pub max_attempts: i32,
}

impl NewOutboxTask {
    pub fn new<T: TaskPayload>(payload: T) -> Result<Self, OutboxError> {
        Ok(Self {
            id: Uuid::new_v4(),
            task_type: T::TASK_TYPE.to_owned(),
            task_version: T::TASK_VERSION,
            payload: serde_json::to_value(payload).map_err(|_| OutboxError::InvalidTask)?,
            metadata: serde_json::json!({}),
            schedule: Schedule::Now,
            max_attempts: 10,
        })
    }

    pub fn scheduled(mut self, schedule: Schedule) -> Self {
        self.schedule = schedule;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskFormat {
    pub task_type: String,
    pub task_version: i32,
}

impl TaskFormat {
    pub fn of<T: TaskPayload>() -> Self {
        Self {
            task_type: T::TASK_TYPE.to_owned(),
            task_version: T::TASK_VERSION,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::{retry_delay, test_support::DummyTask};
    use serde::{Serialize, Serializer};
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn payload_builder_and_retry_bounds_are_stable() {
        let task = NewOutboxTask::new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(task.payload, json!({"value": "runtime"}));
        assert_eq!(task.task_version, 1);
        assert_eq!(task.max_attempts, 10);
        assert_eq!(task.metadata, json!({}));
        assert_eq!(task.schedule, Schedule::default());
        let scheduled = task.scheduled(Schedule::After(Duration::from_secs(300)));
        assert_eq!(scheduled.schedule, Schedule::After(Duration::from_secs(300)));
        for attempt in [i32::MIN, 0, 1, 2, 6, 7, 10, i32::MAX] {
            for _ in 0..100 {
                let delay = retry_delay(attempt);
                assert!((Duration::from_secs(4)..=Duration::from_secs(300)).contains(&delay));
                if attempt >= 7 {
                    assert!(delay >= Duration::from_secs(240));
                }
            }
        }
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
        assert_eq!(NewOutboxTask::new(Broken), Err(OutboxError::InvalidTask));
    }
}
