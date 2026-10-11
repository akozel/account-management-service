//! Checked format registration and decoding for one versioned task operation.

use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::task::{
    data::{
        attempt::TaskAttempt,
        format::{TaskFormat, TaskFormatError, TaskPayload},
    },
    handling::{
        adapter::TaskAdapter,
        handler::{TaskHandler, TaskHandlerError},
    },
};

/// Adapt a typed operation to the dispatcher and executor interface.
///
/// Checks `P`'s format during construction. Each delivery must match that format
/// and deserialize successfully before the operation is called. Structural
/// failures are permanent, with sanitized expected type/version diagnostics;
/// payloads, metadata, actual arbitrary type names and serde errors are omitted.
/// Decoding borrows the JSON value without cloning the whole payload. Unknown
/// field handling follows `P`'s serde settings. Business results pass unchanged.
///
/// The adapter owns the same mutable handler across deliveries. Register separate
/// adapters for separate DTO versions, or use [`TaskAdapter`] for a custom
/// multi-version handler. This helper requires no optional runtime feature.
pub fn task_adapter<P, H>(handler: H) -> Result<Box<dyn TaskAdapter>, TaskFormatError>
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
    H: TaskHandler<P> + 'static,
{
    Ok(Box::new(JsonTaskAdapter::<P, H> {
        handler,
        format: TaskFormat::of::<P>()?,
        payload: PhantomData,
    }))
}

struct JsonTaskAdapter<P, H> {
    handler: H,
    format: TaskFormat,
    payload: PhantomData<P>,
}

impl<P, H> JsonTaskAdapter<P, H> {
    fn safe_format(&self) -> String {
        let task_type = self.format.task_type();
        let safe_type = crate::diagnostics::safe_type(task_type);
        format!("{safe_type} v{}", self.format.task_version())
    }
}

#[async_trait]
impl<P, H> TaskAdapter for JsonTaskAdapter<P, H>
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
    H: TaskHandler<P>,
{
    fn formats(&self) -> Vec<TaskFormat> {
        vec![self.format.clone()]
    }

    async fn invoke(&mut self, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
        if delivery.format() != &self.format {
            return Err(TaskHandlerError::Permanent {
                reason: format!("unsupported task format for {}", self.safe_format()),
            });
        }
        let payload = P::deserialize(delivery.payload()).map_err(|_| TaskHandlerError::Permanent {
            reason: format!("invalid {} task payload", self.safe_format()),
        })?;
        self.handler.handle(payload, delivery).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{
        cell::Cell,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use uuid::Uuid;

    // Neither this owned DTO nor its handler is Clone or Sync.
    #[derive(Serialize, Deserialize)]
    struct Payload<const VERSION: i32> {
        value: String,
        #[serde(skip)]
        marker: Cell<u8>,
    }

    impl<const VERSION: i32> TaskPayload for Payload<VERSION> {
        const TASK_TYPE: &'static str = "test_operation";
        const TASK_VERSION: i32 = VERSION;
    }

    #[derive(Debug)]
    struct Call {
        value: String,
        count: u32,
        id: Uuid,
        attempt: i32,
        format: TaskFormat,
        payload: serde_json::Value,
        metadata: serde_json::Value,
        original_delivery: bool,
    }

    struct RecordingAdapter {
        calls: Arc<Mutex<Vec<Call>>>,
        count: Cell<u32>,
        delivery_address: usize,
        result: Result<(), TaskHandlerError>,
    }

    #[async_trait]
    impl<const VERSION: i32> TaskHandler<Payload<VERSION>> for RecordingAdapter {
        async fn handle(&mut self, payload: Payload<VERSION>, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
            // Keep both the owned non-Sync DTO and borrowed delivery across await.
            tokio::task::yield_now().await;
            payload.marker.set(1);
            self.count.set(self.count.get() + 1);
            self.calls.lock().unwrap().push(Call {
                value: payload.value,
                count: self.count.get(),
                id: delivery.id(),
                attempt: delivery.number(),
                format: delivery.format().clone(),
                payload: delivery.payload().clone(),
                metadata: delivery.metadata().clone(),
                original_delivery: std::ptr::from_ref(delivery) as usize == self.delivery_address,
            });
            self.result.clone()
        }
    }

    fn adapter(calls: &Arc<Mutex<Vec<Call>>>, delivery: &TaskAttempt, result: Result<(), TaskHandlerError>) -> RecordingAdapter {
        RecordingAdapter {
            calls: calls.clone(),
            count: Cell::new(0),
            delivery_address: std::ptr::from_ref(delivery) as usize,
            result,
        }
    }

    fn delivery(format: TaskFormat, payload: serde_json::Value) -> TaskAttempt {
        TaskAttempt::new(Uuid::new_v4(), format, payload, json!({"correlation_id":"original"}), 3).unwrap()
    }

    #[tokio::test]
    async fn decodes_owned_dto_preserves_original_delivery_and_mutable_handler_state() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let task = delivery(
            TaskFormat::of::<Payload<1>>().unwrap(),
            json!({"value":"accepted", "future_field":{"locale":"en"}}),
        );
        let mut adapter = task_adapter::<Payload<1>, _>(adapter(&calls, &task, Ok(()))).unwrap();
        assert_eq!(adapter.formats(), vec![task.format().clone()]);
        for _ in 0..2 {
            adapter.invoke(&task).await.unwrap();
        }
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        for (index, call) in calls.iter().enumerate() {
            assert_eq!(call.value, "accepted");
            assert_eq!(call.count, index as u32 + 1);
            assert_eq!(call.id, task.id());
            assert_eq!(call.attempt, task.number());
            assert_eq!(&call.format, task.format());
            assert_eq!(&call.payload, task.payload());
            assert_eq!(&call.metadata, task.metadata());
            assert!(call.original_delivery);
        }
    }

    #[tokio::test]
    async fn format_and_decode_failures_are_permanent_and_never_call_business_handler() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let base = delivery(TaskFormat::of::<Payload<1>>().unwrap(), json!({"value":"valid"}));
        let mut adapter = task_adapter::<Payload<1>, _>(adapter(&calls, &base, Ok(()))).unwrap();
        for (format, payload, reason) in [
            (
                TaskFormat::new("private_actual_type\nsecret@example.com", 1).unwrap(),
                json!({"value":"private_payload"}),
                "unsupported task format for test_operation v1",
            ),
            (
                TaskFormat::of::<Payload<2>>().unwrap(),
                json!({"value":"valid"}),
                "unsupported task format for test_operation v1",
            ),
            // Both wrong format and bad schema: format observation wins.
            (
                TaskFormat::of::<Payload<2>>().unwrap(),
                json!({}),
                "unsupported task format for test_operation v1",
            ),
            (base.format().clone(), json!({}), "invalid test_operation v1 task payload"),
            (
                base.format().clone(),
                json!({"value":{"private_payload":"secret"}}),
                "invalid test_operation v1 task payload",
            ),
        ] {
            let task = delivery(format, payload);
            let error = adapter.invoke(&task).await.unwrap_err();
            assert!(matches!(&error, TaskHandlerError::Permanent { reason: actual } if actual == reason));
            assert_eq!(error.to_string(), reason);
            assert!(!format!("{error:?}").contains("private_"));
        }
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn business_results_pass_through_without_retry_or_reason_changes() {
        let task = delivery(TaskFormat::of::<Payload<1>>().unwrap(), json!({"value":"valid"}));
        for result in [
            Ok(()),
            Err(TaskHandlerError::Retry {
                reason: "business retry".into(),
                retry_after: Some(Duration::from_secs(7)),
            }),
            Err(TaskHandlerError::Retry {
                reason: "default backoff".into(),
                retry_after: None,
            }),
            Err(TaskHandlerError::Permanent {
                reason: "business rejection".into(),
            }),
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let mut adapter = task_adapter::<Payload<1>, _>(adapter(&calls, &task, result.clone())).unwrap();
            match (adapter.invoke(&task).await, result) {
                (Ok(()), Ok(())) => {}
                (
                    Err(TaskHandlerError::Retry { reason, retry_after }),
                    Err(TaskHandlerError::Retry {
                        reason: expected,
                        retry_after: delay,
                    }),
                ) => {
                    assert_eq!(reason, expected);
                    assert_eq!(retry_after, delay);
                }
                (Err(TaskHandlerError::Permanent { reason }), Err(TaskHandlerError::Permanent { reason: expected })) => {
                    assert_eq!(reason, expected)
                }
                (actual, expected) => panic!("unexpected result: {actual:?}, expected {expected:?}"),
            }
            assert_eq!(calls.lock().unwrap().len(), 1);
        }
    }

    #[derive(Serialize)]
    struct PrivatePayload;

    impl<'de> Deserialize<'de> for PrivatePayload {
        fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
            Err(serde::de::Error::custom("private_serde_error_sentinel"))
        }
    }
    impl TaskPayload for PrivatePayload {
        const TASK_TYPE: &'static str = "private_expected_type\nsecret@example.com";
        const TASK_VERSION: i32 = 4;
    }
    struct NeverCalled;
    #[async_trait]
    impl TaskHandler<PrivatePayload> for NeverCalled {
        async fn handle(&mut self, _: PrivatePayload, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            panic!("invalid deliveries must not reach business code")
        }
    }

    #[tokio::test]
    async fn arbitrary_expected_types_and_serde_errors_are_not_exposed() {
        let mut adapter = task_adapter::<PrivatePayload, _>(NeverCalled).unwrap();
        assert_eq!(adapter.formats(), vec![TaskFormat::of::<PrivatePayload>().unwrap()]);
        for (format, reason) in [
            (TaskFormat::of::<PrivatePayload>().unwrap(), "invalid unknown v4 task payload"),
            (TaskFormat::new("other", 4).unwrap(), "unsupported task format for unknown v4"),
        ] {
            let task = delivery(format, json!({"private_payload_sentinel":"secret"}));
            let error = adapter.invoke(&task).await.unwrap_err();
            assert_eq!(error.to_string(), reason);
            assert!(!format!("{error:?}").contains("private_"));
        }
    }

    #[derive(Serialize, Deserialize)]
    struct EmptyType;
    impl TaskPayload for EmptyType {
        const TASK_TYPE: &'static str = "";
        const TASK_VERSION: i32 = 1;
    }
    #[async_trait]
    impl TaskHandler<EmptyType> for NeverCalled {
        async fn handle(&mut self, _: EmptyType, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            unreachable!("invalid format cannot construct an adapter")
        }
    }

    #[test]
    fn validates_format_before_registering_handler() {
        let task = delivery(TaskFormat::of::<Payload<1>>().unwrap(), json!({}));
        let calls = Arc::new(Mutex::new(Vec::new()));
        assert!(matches!(
            task_adapter::<EmptyType, _>(NeverCalled),
            Err(TaskFormatError::EmptyType)
        ));
        for invalid in [0, -1] {
            let result = if invalid == 0 {
                task_adapter::<Payload<0>, _>(adapter(&calls, &task, Ok(())))
            } else {
                task_adapter::<Payload<-1>, _>(adapter(&calls, &task, Ok(())))
            };
            assert!(matches!(result, Err(TaskFormatError::InvalidVersion)));
        }
        assert!(calls.lock().unwrap().is_empty());
    }
}
