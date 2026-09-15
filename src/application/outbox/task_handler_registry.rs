use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;

use super::{ClaimedTask, TaskExecutionError, TaskFormat, TaskHandler};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskHandlerRegistryError {
    #[error("handler registry must contain at least one handler")]
    Empty,
    #[error("duplicate outbox task format: {0} v{1}")]
    DuplicateFormat(String, i32),
}

/// Routes each persisted task format to exactly one handler within a worker.
pub struct TaskHandlerRegistry {
    handlers: Vec<Box<dyn TaskHandler>>,
    formats: Vec<TaskFormat>,
    routes: HashMap<(String, i32), usize>,
}

impl TaskHandlerRegistry {
    pub fn try_new(handlers: Vec<Box<dyn TaskHandler>>) -> Result<Self, TaskHandlerRegistryError> {
        if handlers.is_empty() {
            return Err(TaskHandlerRegistryError::Empty);
        }
        let mut routes = HashMap::new();
        let mut formats = Vec::new();
        for (index, handler) in handlers.iter().enumerate() {
            for format in handler.formats() {
                if routes
                    .insert((format.task_type.clone(), format.task_version), index)
                    .is_some()
                {
                    return Err(TaskHandlerRegistryError::DuplicateFormat(
                        format.task_type,
                        format.task_version,
                    ));
                }
                formats.push(format);
            }
        }
        if formats.is_empty() {
            return Err(TaskHandlerRegistryError::Empty);
        }
        Ok(Self {
            handlers,
            formats,
            routes,
        })
    }

    pub fn formats(&self) -> Vec<TaskFormat> {
        self.formats.clone()
    }
}

#[async_trait]
impl TaskHandler for TaskHandlerRegistry {
    fn formats(&self) -> Vec<TaskFormat> {
        self.formats()
    }

    async fn handle(&mut self, task: &ClaimedTask) -> Result<(), TaskExecutionError> {
        match self.routes.get(&(task.format.task_type.clone(), task.format.task_version)) {
            Some(index) => self.handlers[*index].handle(task).await,
            None => Err(TaskExecutionError::Permanent {
                reason: "unsupported task format".to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::application::outbox::test_support::claimed;

    struct RecordingHandler {
        format: TaskFormat,
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl TaskHandler for RecordingHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![self.format.clone()]
        }

        async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
            self.calls.lock().unwrap().push(self.format.task_type.clone());
            Ok(())
        }
    }

    fn handler(format: TaskFormat, calls: &Arc<Mutex<Vec<String>>>) -> Box<dyn TaskHandler> {
        Box::new(RecordingHandler {
            format,
            calls: calls.clone(),
        })
    }

    #[tokio::test]
    async fn routes_two_formats_and_rejects_unknown_format() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = TaskFormat {
            task_type: "first".into(),
            task_version: 1,
        };
        let second = TaskFormat {
            task_type: "second".into(),
            task_version: 2,
        };
        let mut registry =
            TaskHandlerRegistry::try_new(vec![handler(first.clone(), &calls), handler(second.clone(), &calls)]).unwrap();
        assert_eq!(registry.formats(), vec![first.clone(), second.clone()]);
        for format in [second, first] {
            let mut task = claimed();
            task.format = format;
            registry.handle(&task).await.unwrap();
        }
        assert_eq!(*calls.lock().unwrap(), vec!["second", "first"]);
        let mut unknown = claimed();
        unknown.format.task_version = 77;
        assert!(matches!(
            registry.handle(&unknown).await,
            Err(TaskExecutionError::Permanent { .. })
        ));
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[test]
    fn rejects_empty_and_duplicate_formats() {
        assert!(matches!(
            TaskHandlerRegistry::try_new(vec![]),
            Err(TaskHandlerRegistryError::Empty)
        ));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let format = TaskFormat {
            task_type: "same".into(),
            task_version: 1,
        };
        assert!(matches!(
            TaskHandlerRegistry::try_new(vec![handler(format.clone(), &calls), handler(format, &calls)]),
            Err(TaskHandlerRegistryError::DuplicateFormat(name, 1)) if name == "same"
        ));
    }
}
