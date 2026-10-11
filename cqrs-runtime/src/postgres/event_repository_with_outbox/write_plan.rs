//! Checked single-event and snapshot decisions before any SQL work.
use crate::task::data::new_task::NewTask;
use cqrs_es::persist::{PersistenceError, SerializedEvent};
use serde_json::Value;
use thiserror::Error;
pub(super) enum WritePlan<'a> {
    NoOp,
    OneEvent {
        event: &'a SerializedEvent,
        sequence: i64,
        snapshot: SnapshotWrite,
    },
}
pub(super) enum SnapshotWrite {
    None,
    Insert {
        aggregate_id: String,
        payload: Value,
        version: i64,
    },
    Update {
        aggregate_id: String,
        payload: Value,
        version: i64,
    },
}
impl SnapshotWrite {
    fn new(update: Option<(String, Value, usize)>) -> Result<Self, PersistenceError> {
        match update {
            None => Ok(Self::None),
            Some((aggregate_id, payload, version)) => {
                let version = sequence_as_i64(version)?;
                Ok(if version == 1 {
                    Self::Insert {
                        aggregate_id,
                        payload,
                        version,
                    }
                } else {
                    Self::Update {
                        aggregate_id,
                        payload,
                        version,
                    }
                })
            }
        }
    }
    fn bind_count(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Insert { .. } => 5,
            Self::Update { .. } => 6,
        }
    }
}
impl<'a> WritePlan<'a> {
    pub(super) fn new(
        events: &'a [SerializedEvent],
        snapshot: Option<(String, Value, usize)>,
        tasks: &[NewTask],
    ) -> Result<Self, PersistenceError> {
        match events {
            [] if snapshot.is_some() => Err(invalid_write(WriteValidationError::SnapshotWithoutEvent)),
            [] if !tasks.is_empty() => Err(invalid_write(WriteValidationError::TasksWithoutEvent)),
            [] => Ok(Self::NoOp),
            [event] => {
                let snapshot = SnapshotWrite::new(snapshot)?;
                validate_bind_count(tasks.len(), snapshot.bind_count())?;
                Ok(Self::OneEvent {
                    event,
                    sequence: sequence_as_i64(event.sequence)?,
                    snapshot,
                })
            }
            _ => Err(invalid_write(WriteValidationError::MultipleEvents)),
        }
    }
}
#[derive(Debug, Error)]
enum WriteValidationError {
    #[error("a command may commit at most one domain event")]
    MultipleEvents,
    #[error("a snapshot requires a committed domain event")]
    SnapshotWithoutEvent,
    #[error("outbox tasks require a committed domain event")]
    TasksWithoutEvent,
    #[error("event sequence does not fit PostgreSQL bigint")]
    SequenceOverflow,
    #[error("write batch exceeds PostgreSQL's bind parameter limit")]
    BindLimitExceeded,
}
fn invalid_write(reason: WriteValidationError) -> PersistenceError {
    PersistenceError::UnknownError(Box::new(reason))
}
fn validate_bind_count(tasks: usize, snapshot_binds: usize) -> Result<(), PersistenceError> {
    const EVENT_BINDS: usize = 7;
    const TASK_BINDS: usize = 11;
    let count = tasks
        .checked_mul(TASK_BINDS)
        .and_then(|count| count.checked_add(EVENT_BINDS))
        .and_then(|count| count.checked_add(snapshot_binds));
    if count.is_some_and(|count| count < u16::MAX as usize) {
        Ok(())
    } else {
        Err(invalid_write(WriteValidationError::BindLimitExceeded))
    }
}
fn sequence_as_i64(sequence: usize) -> Result<i64, PersistenceError> {
    i64::try_from(sequence).map_err(|_| invalid_write(WriteValidationError::SequenceOverflow))
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(sequence: usize) -> SerializedEvent {
        SerializedEvent::new(
            "aggregate".into(),
            sequence,
            "test".into(),
            "recorded".into(),
            "1".into(),
            json!({}),
            json!({}),
        )
    }

    #[test]
    fn a_write_plan_rejects_oversized_task_batches_including_snapshot_binds() {
        let task = NewTask::from_payload(crate::task::data::format::TaskFormat::new("test", 1).unwrap(), json!({})).unwrap();
        let events = [event(1)];
        for (count, snapshot) in [
            (5_958, None),
            (5_957, Some(("aggregate".into(), json!({}), 1))),
            (5_957, Some(("aggregate".into(), json!({}), 2))),
        ] {
            let tasks = vec![task.clone(); count];
            let Err(PersistenceError::UnknownError(source)) = WritePlan::new(&events, snapshot, &tasks) else {
                panic!("an oversized batch must be rejected before issuing SQL");
            };
            assert!(matches!(source.downcast_ref(), Some(WriteValidationError::BindLimitExceeded)));
        }
    }

    #[test]
    fn invalid_write_shapes_retain_their_validation_reason() {
        let tasks = [NewTask::from_payload(crate::task::data::format::TaskFormat::new("test", 1).unwrap(), json!({})).unwrap()];
        for (events, snapshot, tasks, expected) in [
            (
                vec![],
                Some(("aggregate".into(), json!({}), 1)),
                &[][..],
                "a snapshot requires a committed domain event",
            ),
            (vec![], None, &tasks[..], "outbox tasks require a committed domain event"),
            (
                vec![event(1), event(2)],
                None,
                &[][..],
                "a command may commit at most one domain event",
            ),
        ] {
            let Err(PersistenceError::UnknownError(source)) = WritePlan::new(&events, snapshot, tasks) else {
                panic!("invalid shape must be rejected before SQL");
            };
            assert_eq!(source.to_string(), expected);
        }
        assert!(matches!(WritePlan::new(&[], None, &[]), Ok(WritePlan::NoOp)));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn both_event_and_snapshot_sequence_overflows_are_rejected_before_sql() {
        let overflow = i64::MAX as usize + 1;
        for (event, snapshot) in [
            (event(overflow), None),
            (event(1), Some(("aggregate".into(), json!({}), overflow))),
        ] {
            let Err(PersistenceError::UnknownError(source)) = WritePlan::new(&[event], snapshot, &[]) else {
                panic!("overflow must fail during validation");
            };
            assert!(matches!(source.downcast_ref(), Some(WriteValidationError::SequenceOverflow)));
        }
    }
    #[test]
    fn converts_supported_sequence() {
        assert_eq!(sequence_as_i64(42).expect("sequence must fit"), 42);
    }

    #[test]
    fn rejects_batches_over_the_postgres_bind_limit() {
        assert!(validate_bind_count(1, 6).is_ok());
        assert!(validate_bind_count(5_957, 0).is_ok());
        assert!(matches!(
            validate_bind_count(5_958, 0),
            Err(PersistenceError::UnknownError(_))
        ));
        assert!(validate_bind_count(usize::MAX, 0).is_err());
        // Snapshot binds count toward the same statement limit as the tasks.
        for snapshot_binds in [5, 6] {
            assert!(validate_bind_count(5_956, snapshot_binds).is_ok());
            assert!(validate_bind_count(5_957, snapshot_binds).is_err());
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_overflowing_sequence() {
        let overflowing_sequence = usize::try_from(i64::MAX).expect("64-bit usize must fit i64::MAX") + 1;

        let result = sequence_as_i64(overflowing_sequence);

        assert!(matches!(result, Err(PersistenceError::UnknownError(_))));
    }
}
