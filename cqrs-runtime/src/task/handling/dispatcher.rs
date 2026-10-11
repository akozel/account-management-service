use std::{collections::HashMap, fmt};

use thiserror::Error;

use crate::task::{
    data::{attempt::TaskAttempt, format::TaskFormat},
    handling::{adapter::TaskAdapter, handler::TaskHandlerError},
};

#[derive(Error, PartialEq, Eq)]
pub enum TaskDispatcherError {
    #[error("task dispatcher must contain at least one adapter")]
    Empty,
    #[error("duplicate task format: {} v{}", crate::diagnostics::safe_type(.0.task_type()), .0.task_version())]
    DuplicateFormat(TaskFormat),
    /// A task adapter factory rejected its payload format before registration.
    #[error("{0}")]
    InvalidFormat(#[from] crate::task::data::format::TaskFormatError),
}

impl fmt::Debug for TaskDispatcherError {
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

/// Routes each persisted task format to exactly one adapter within a worker.
/// A dispatcher is a router, not a wire adapter:
/// ```compile_fail
/// use cqrs_runtime::task::handling::{adapter::TaskAdapter, dispatcher::TaskDispatcher};
/// fn requires_adapter<T: TaskAdapter>() {}
/// requires_adapter::<TaskDispatcher>();
/// ```
pub struct TaskDispatcher {
    adapters: Vec<Box<dyn TaskAdapter>>,
    formats: Vec<TaskFormat>,
    routes: HashMap<TaskFormat, usize>,
}

impl TaskDispatcher {
    pub fn try_new(adapters: Vec<Box<dyn TaskAdapter>>) -> Result<Self, TaskDispatcherError> {
        if adapters.is_empty() {
            return Err(TaskDispatcherError::Empty);
        }
        let (routes, formats) = adapters
            .iter()
            .enumerate()
            .flat_map(|(index, adapter)| adapter.formats().into_iter().map(move |format| (index, format)))
            .try_fold((HashMap::new(), Vec::new()), |(mut routes, mut formats), (index, format)| {
                if routes.insert(format.clone(), index).is_some() {
                    return Err(TaskDispatcherError::DuplicateFormat(format));
                }
                formats.push(format);
                Ok((routes, formats))
            })?;
        if formats.is_empty() {
            return Err(TaskDispatcherError::Empty);
        }
        Ok(Self {
            adapters,
            formats,
            routes,
        })
    }

    /// Borrow the supported formats in routing and polling order.
    pub fn formats(&self) -> &[TaskFormat] {
        &self.formats
    }

    /// Route an attempt by its complete format, then invoke that adapter.
    pub async fn dispatch(&mut self, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
        match self.routes.get(delivery.format()) {
            Some(index) => self.adapters[*index].invoke(delivery).await,
            None => Err(TaskHandlerError::Permanent {
                reason: "unsupported task format".to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{
        data::{attempt::TaskAttempt, format::TaskFormat},
        handling::{adapter::TaskAdapter, handler::TaskHandlerError},
        test_support::claimed,
    };
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    #[test]
    fn registry_error_formatting_sanitizes_names_without_changing_the_format() {
        for (name, safe_name) in [
            ("create_account", "create_account"),
            ("private_format\nmail@example.com", "unknown"),
        ] {
            let format = TaskFormat::new(name, 2).unwrap();
            let error = TaskDispatcherError::DuplicateFormat(format.clone());
            let rendered = format!("{error} {error:?}");
            assert!(rendered.contains(safe_name));
            assert!(rendered.contains('2'));
            assert!(!rendered.contains("private_"));
            assert!(!rendered.contains("mail@example.com"));
            assert!(matches!(error, TaskDispatcherError::DuplicateFormat(actual) if actual == format));
        }
        assert_eq!(format!("{:?}", TaskDispatcherError::Empty), "Empty");
        let invalid = TaskDispatcherError::from(crate::task::data::format::TaskFormatError::EmptyType);
        assert!(format!("{invalid:?}").contains("EmptyType"));
        assert!(std::error::Error::source(&invalid).is_some());
    }

    struct RecordingAdapter {
        formats: Vec<TaskFormat>,
        calls: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait]
    impl TaskAdapter for RecordingAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            self.formats.clone()
        }
        async fn invoke(&mut self, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
            self.calls.lock().unwrap().push(delivery.format().task_type().into());
            Ok(())
        }
    }
    fn adapter(formats: Vec<TaskFormat>, calls: &Arc<Mutex<Vec<String>>>) -> Box<dyn TaskAdapter> {
        Box::new(RecordingAdapter {
            formats,
            calls: calls.clone(),
        })
    }
    #[tokio::test]
    async fn routes_in_order_and_rejects_duplicates_empty_and_unknown_formats() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = TaskFormat::new("first", 1).unwrap();
        let second = TaskFormat::new("second", 2).unwrap();
        let mut registry = TaskDispatcher::try_new(vec![
            adapter(vec![first.clone()], &calls),
            adapter(vec![second.clone()], &calls),
        ])
        .unwrap();
        assert_eq!(registry.formats(), vec![first.clone(), second.clone()]);
        let base = claimed();
        for format in [second, first.clone()] {
            let input = TaskAttempt::new(
                base.attempt().id(),
                format,
                base.attempt().payload().clone(),
                base.attempt().metadata().clone(),
                1,
            )
            .unwrap();
            registry.dispatch(&input).await.unwrap();
        }
        assert_eq!(*calls.lock().unwrap(), vec!["second", "first"]);
        assert!(matches!(
            registry.dispatch(base.attempt()).await,
            Err(TaskHandlerError::Permanent { .. })
        ));
        assert!(matches!(TaskDispatcher::try_new(vec![]), Err(TaskDispatcherError::Empty)));
        assert!(matches!(
            TaskDispatcher::try_new(vec![adapter(vec![], &calls)]),
            Err(TaskDispatcherError::Empty)
        ));
        assert!(matches!(
            TaskDispatcher::try_new(vec![adapter(vec![first.clone(), first], &calls)]),
            Err(TaskDispatcherError::DuplicateFormat(_))
        ));
        let error = TaskDispatcherError::from(crate::task::data::format::TaskFormatError::EmptyType);
        assert!(matches!(error, TaskDispatcherError::InvalidFormat(_)));
    }

    #[tokio::test]
    async fn typed_adapters_route_versions_separately_and_reject_duplicate_formats() {
        use crate::task::{
            data::format::TaskPayload,
            handling::{handler::TaskHandler, json_adapter::task_adapter},
        };
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
        impl<const VERSION: i32> TaskHandler<Payload<VERSION>> for Handler {
            async fn handle(&mut self, payload: Payload<VERSION>, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
                self.0.lock().unwrap().push((VERSION, payload.value));
                Ok(())
            }
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = TaskFormat::of::<Payload<1>>().unwrap();
        let second = TaskFormat::of::<Payload<2>>().unwrap();
        let mut registry = TaskDispatcher::try_new(vec![
            task_adapter::<Payload<1>, _>(Handler(calls.clone())).unwrap(),
            task_adapter::<Payload<2>, _>(Handler(calls.clone())).unwrap(),
        ])
        .unwrap();
        assert_eq!(registry.formats(), vec![first.clone(), second.clone()]);
        let base = claimed();
        for format in [second, first.clone()] {
            let task = TaskAttempt::new(
                base.attempt().id(),
                format,
                json!({"value":"decoded"}),
                base.attempt().metadata().clone(),
                1,
            )
            .unwrap();
            registry.dispatch(&task).await.unwrap();
        }
        assert_eq!(*calls.lock().unwrap(), vec![(2, "decoded".into()), (1, "decoded".into())]);
        assert!(matches!(TaskDispatcher::try_new(vec![
            task_adapter::<Payload<1>, _>(Handler(calls.clone())).unwrap(),
            task_adapter::<Payload<1>, _>(Handler(calls.clone())).unwrap(),
        ]), Err(TaskDispatcherError::DuplicateFormat(format)) if format == first));
        assert_eq!(calls.lock().unwrap().len(), 2);
    }
}
