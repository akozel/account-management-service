use std::{collections::HashMap, fmt};

use async_trait::async_trait;
use thiserror::Error;

use super::{TaskDelivery, TaskFormat, TaskHandler, TaskHandlerError};

#[derive(Error, PartialEq, Eq)]
pub enum TaskHandlerRegistryError {
    #[error("handler registry must contain at least one handler")]
    Empty,
    #[error("duplicate outbox task format: {} v{}", crate::diagnostics::safe_type(.0.task_type()), .0.task_version())]
    DuplicateFormat(TaskFormat),
    /// A typed handler factory rejected its payload format before registration.
    #[error("{0}")]
    InvalidFormat(#[from] super::TaskFormatError),
}

impl fmt::Debug for TaskHandlerRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("Empty"),
            Self::DuplicateFormat(format) => f
                .debug_struct("DuplicateFormat")
                .field("task_type", &crate::diagnostics::safe_type(format.task_type()))
                .field("task_version", &format.task_version())
                .finish(),
            Self::InvalidFormat(error) => f.debug_tuple("InvalidFormat").field(error).finish(),
        }
    }
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

    /// Borrow the supported formats in routing and polling order.
    pub fn formats(&self) -> &[TaskFormat] {
        &self.formats
    }
}

#[async_trait]
impl TaskHandler for TaskHandlerRegistry {
    fn formats(&self) -> Vec<TaskFormat> {
        self.formats().to_vec()
    }

    async fn handle(&mut self, delivery: &TaskDelivery) -> Result<(), TaskHandlerError> {
        match self.routes.get(delivery.format()) {
            Some(index) => self.handlers[*index].handle(delivery).await,
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

    #[test]
    fn registry_error_formatting_sanitizes_names_without_changing_the_format() {
        for (name, safe_name) in [
            ("create_account", "create_account"),
            ("private_format\nmail@example.com", "unknown"),
        ] {
            let format = TaskFormat::new(name, 2).unwrap();
            let error = TaskHandlerRegistryError::DuplicateFormat(format.clone());
            let rendered = format!("{error} {error:?}");
            assert!(rendered.contains(safe_name));
            assert!(rendered.contains('2'));
            assert!(!rendered.contains("private_"));
            assert!(!rendered.contains("mail@example.com"));
            assert!(matches!(error, TaskHandlerRegistryError::DuplicateFormat(actual) if actual == format));
        }
        assert_eq!(format!("{:?}", TaskHandlerRegistryError::Empty), "Empty");
        let invalid = TaskHandlerRegistryError::from(super::super::TaskFormatError::EmptyType);
        assert!(format!("{invalid:?}").contains("EmptyType"));
        assert!(std::error::Error::source(&invalid).is_some());
    }

    struct RecordingHandler {
        formats: Vec<TaskFormat>,
        calls: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait]
    impl TaskHandler for RecordingHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            self.formats.clone()
        }
        async fn handle(&mut self, delivery: &TaskDelivery) -> Result<(), TaskHandlerError> {
            self.calls.lock().unwrap().push(delivery.format().task_type().into());
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

    #[tokio::test]
    async fn typed_adapters_route_versions_separately_and_reject_duplicate_formats() {
        use crate::outbox::{TaskPayload, TypedTaskHandler, typed_task_handler};
        use serde::{Deserialize, Serialize};
        use serde_json::json;

        #[derive(Serialize, Deserialize)]
        struct Payload<const VERSION: i32> {
            value: String,
        }
        impl<const VERSION: i32> TaskPayload for Payload<VERSION> {
            const TASK_TYPE: &'static str = "versioned_operation";
            const TASK_VERSION: i32 = VERSION;
        }
        struct Handler(Arc<Mutex<Vec<(i32, String)>>>);
        #[async_trait]
        impl<const VERSION: i32> TypedTaskHandler<Payload<VERSION>> for Handler {
            async fn handle(&mut self, payload: Payload<VERSION>, _: &TaskDelivery) -> Result<(), TaskHandlerError> {
                self.0.lock().unwrap().push((VERSION, payload.value));
                Ok(())
            }
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = TaskFormat::of::<Payload<1>>().unwrap();
        let second = TaskFormat::of::<Payload<2>>().unwrap();
        let mut registry = TaskHandlerRegistry::try_new(vec![
            typed_task_handler::<Payload<1>, _>(Handler(calls.clone())).unwrap(),
            typed_task_handler::<Payload<2>, _>(Handler(calls.clone())).unwrap(),
        ])
        .unwrap();
        assert_eq!(registry.formats(), vec![first.clone(), second.clone()]);
        let base = claimed();
        for format in [second, first.clone()] {
            let task = TaskDelivery::new(
                base.delivery().id(),
                format,
                json!({"value":"decoded"}),
                base.delivery().metadata().clone(),
                1,
            )
            .unwrap();
            registry.handle(&task).await.unwrap();
        }
        assert_eq!(*calls.lock().unwrap(), vec![(2, "decoded".into()), (1, "decoded".into())]);
        assert!(matches!(TaskHandlerRegistry::try_new(vec![
            typed_task_handler::<Payload<1>, _>(Handler(calls.clone())).unwrap(),
            typed_task_handler::<Payload<1>, _>(Handler(calls.clone())).unwrap(),
        ]), Err(TaskHandlerRegistryError::DuplicateFormat(format)) if format == first));
        assert_eq!(calls.lock().unwrap().len(), 2);
    }
}
