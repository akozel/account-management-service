//! Checked per-worker dispatcher construction for preparation and restart.
use crate::task::{
    data::format::TaskFormat,
    handling::dispatcher::{TaskDispatcher, TaskDispatcherError},
};
use std::sync::Arc;
use thiserror::Error;
/// Fresh handler dispatcher per worker/restart; every instance must expose the
/// same ordered format list as the first retained dispatcher. Sender state
/// belongs to a worker.
pub type TaskDispatcherFactory = Arc<dyn Fn() -> Result<TaskDispatcher, TaskDispatcherError> + Send + Sync>;

/// Registry construction or format mismatch, shared by preparation and restart.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkerSetupError {
    #[error("worker {worker_index} dispatcher construction failed: {source}")]
    Build {
        worker_index: usize,
        #[source]
        source: TaskDispatcherError,
    },
    #[error("worker {worker_index} dispatcher and executor formats differ")]
    FormatsMismatch { worker_index: usize },
}

pub(super) fn build_checked_dispatcher(
    factory: &TaskDispatcherFactory,
    formats: &[TaskFormat],
    worker_index: usize,
) -> Result<TaskDispatcher, WorkerSetupError> {
    let dispatcher = factory().map_err(|source| WorkerSetupError::Build { worker_index, source })?;
    if dispatcher.formats() == formats {
        Ok(dispatcher)
    } else {
        Err(WorkerSetupError::FormatsMismatch { worker_index })
    }
}
