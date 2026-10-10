use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;

use super::{TaskDelivery, TaskFormat, TaskHandler, TaskHandlerError};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskHandlerRegistryError {
    #[error("handler registry must contain at least one handler")]
    Empty,
    #[error("duplicate outbox task format: {0}")]
    DuplicateFormat(TaskFormat),
    #[error(transparent)]
    InvalidFormat(#[from] super::TaskFormatError),
}

/// Routes each persisted task format to exactly one handler within a worker.
pub struct TaskHandlerRegistry {
    handlers: Vec<Box<dyn TaskHandler>>,
    formats: Vec<TaskFormat>,
    routes: HashMap<TaskFormat, usize>,
}

impl TaskHandlerRegistry {
    pub fn try_new(handlers: Vec<Box<dyn TaskHandler>>) -> Result<Self, TaskHandlerRegistryError> {
        if handlers.is_empty() {
            return Err(TaskHandlerRegistryError::Empty);
        }
        let (routes, formats) = handlers
            .iter()
            .enumerate()
            .flat_map(|(index, handler)| handler.formats().into_iter().map(move |format| (index, format)))
            .try_fold((HashMap::new(), Vec::new()), |(mut routes, mut formats), (index, format)| {
                if routes.insert(format.clone(), index).is_some() {
                    return Err(TaskHandlerRegistryError::DuplicateFormat(format));
                }
                formats.push(format);
                Ok((routes, formats))
            })?;
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

    async fn handle(&mut self, task: &TaskDelivery) -> Result<(), TaskHandlerError> {
        match self.routes.get(task.format()) {
            Some(index) => self.handlers[*index].handle(task).await,
            None => Err(TaskHandlerError::Permanent {
                reason: "unsupported task format".to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::{TaskDelivery, TaskFormat, TaskHandler, TaskHandlerError, test_support::claimed};
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    struct RecordingHandler {
        formats: Vec<TaskFormat>,
        calls: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait]
    impl TaskHandler for RecordingHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            self.formats.clone()
        }
        async fn handle(&mut self, task: &TaskDelivery) -> Result<(), TaskHandlerError> {
            self.calls.lock().unwrap().push(task.format().task_type().into());
            Ok(())
        }
    }
    fn handler(formats: Vec<TaskFormat>, calls: &Arc<Mutex<Vec<String>>>) -> Box<dyn TaskHandler> {
        Box::new(RecordingHandler {
            formats,
            calls: calls.clone(),
        })
    }
    #[tokio::test]
    async fn routes_in_order_and_rejects_duplicates_empty_and_unknown_formats() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = TaskFormat::new("first", 1).unwrap();
        let second = TaskFormat::new("second", 2).unwrap();
        let mut registry = TaskHandlerRegistry::try_new(vec![
            handler(vec![first.clone()], &calls),
            handler(vec![second.clone()], &calls),
        ])
        .unwrap();
        assert_eq!(registry.formats(), vec![first.clone(), second.clone()]);
        let base = claimed();
        for format in [second, first.clone()] {
            let input = TaskDelivery::new(
                base.delivery().id(),
                format,
                base.delivery().payload().clone(),
                base.delivery().metadata().clone(),
                1,
            )
            .unwrap();
            registry.handle(&input).await.unwrap();
        }
        assert_eq!(*calls.lock().unwrap(), vec!["second", "first"]);
        assert!(matches!(
            registry.handle(base.delivery()).await,
            Err(TaskHandlerError::Permanent { .. })
        ));
        assert!(matches!(
            TaskHandlerRegistry::try_new(vec![]),
            Err(TaskHandlerRegistryError::Empty)
        ));
        assert!(matches!(
            TaskHandlerRegistry::try_new(vec![handler(vec![], &calls)]),
            Err(TaskHandlerRegistryError::Empty)
        ));
        assert!(matches!(
            TaskHandlerRegistry::try_new(vec![handler(vec![first.clone(), first], &calls)]),
            Err(TaskHandlerRegistryError::DuplicateFormat(_))
        ));
        let error = TaskHandlerRegistryError::from(crate::outbox::TaskFormatError::EmptyType);
        assert!(matches!(error, TaskHandlerRegistryError::InvalidFormat(_)));
    }
}
