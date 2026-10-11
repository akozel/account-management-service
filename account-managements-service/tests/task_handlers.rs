use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use account_managements_service::{
    application::{
        email_reservation::tasks::send_verification_code::{
            VerificationCodeDelivery, VerificationCodeSender, send_verification_code_task_handler, v1::SendVerificationCodeTaskV1,
        },
        user_account::{
            service::{AcceptedSubmission, ContinuationError, ProfileSubmission, RegistrationService, SubmissionError},
            tasks::{
                CreateAccountTaskV1, RecordAccountCreatedTaskV1, create_account::create_account_task_handler,
                record_account_created::record_account_created_task_handler,
            },
        },
    },
    domain::{Email, RegistrationProfile},
};
use async_trait::async_trait;
use chrono::NaiveDate;
use cqrs_runtime::outbox::{TaskDelivery, TaskFormat, TaskHandlerError, TaskPayload};
use serde_json::json;
use uuid::Uuid;

fn delivery<P: TaskPayload>(payload: &P) -> TaskDelivery {
    let mut payload = serde_json::to_value(payload).unwrap();
    payload["future_optional_field"] = json!({"locale":"en"});
    TaskDelivery::new(
        Uuid::new_v4(),
        TaskFormat::of::<P>().unwrap(),
        payload,
        json!({"correlation_id":"original"}),
        2,
    )
    .unwrap()
}

fn verification(code: u32) -> SendVerificationCodeTaskV1 {
    SendVerificationCodeTaskV1 {
        email: Email::parse("alice@example.com").unwrap(),
        account_id: "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse().unwrap(),
        verification_code: code,
    }
}

struct SendRecord {
    message: VerificationCodeDelivery,
    delivery: TaskDelivery,
    delivery_address: usize,
}

struct RecordingSender {
    records: Arc<Mutex<Vec<SendRecord>>>,
    result: Result<(), TaskHandlerError>,
}

#[async_trait]
impl VerificationCodeSender for RecordingSender {
    async fn send(&mut self, message: VerificationCodeDelivery, delivery: &TaskDelivery) -> Result<(), TaskHandlerError> {
        // Borrow the original delivery throughout async work; snapshots are test-only.
        tokio::task::yield_now().await;
        self.records.lock().unwrap().push(SendRecord {
            message,
            delivery: delivery.clone(),
            delivery_address: std::ptr::from_ref(delivery) as usize,
        });
        self.result.clone()
    }
}

fn assert_result(actual: Result<(), TaskHandlerError>, expected: Result<(), TaskHandlerError>) {
    match (actual, expected) {
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
        (actual, expected) => panic!("unexpected handler result: {actual:?}, expected {expected:?}"),
    }
}

#[tokio::test]
async fn verification_v1_accepts_extra_fields_and_passes_original_delivery_across_await() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut handler = send_verification_code_task_handler(Box::new(RecordingSender {
        records: records.clone(),
        result: Ok(()),
    }))
    .unwrap();
    assert_eq!(
        handler.formats(),
        vec![TaskFormat::of::<SendVerificationCodeTaskV1>().unwrap()]
    );
    for code in [1_000_000, 1_234_567, 9_999_999] {
        let payload = verification(code);
        let task = delivery(&payload);
        handler.handle(&task).await.unwrap();
        let records = records.lock().unwrap();
        let record = records.last().unwrap();
        assert_eq!(record.message, VerificationCodeDelivery::from(payload));
        assert_eq!(record.delivery_address, std::ptr::from_ref(&task) as usize);
        assert_eq!(record.delivery.id(), task.id());
        assert_eq!(record.delivery.attempt(), task.attempt());
        assert_eq!(record.delivery.format(), task.format());
        assert_eq!(record.delivery.payload(), task.payload());
        assert_eq!(record.delivery.metadata(), task.metadata());
    }
    assert_eq!(records.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn verification_rejects_invalid_business_codes_without_sending() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut handler = send_verification_code_task_handler(Box::new(RecordingSender {
        records: records.clone(),
        result: Ok(()),
    }))
    .unwrap();
    for code in [0, 999_999, 10_000_000, u32::MAX] {
        assert_result(
            handler.handle(&delivery(&verification(code))).await,
            Err(TaskHandlerError::Permanent {
                reason: "invalid verification code".into(),
            }),
        );
    }
    assert!(records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn verification_invalid_dtos_and_versions_never_reach_sender() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut handler = send_verification_code_task_handler(Box::new(RecordingSender {
        records: records.clone(),
        result: Ok(()),
    }))
    .unwrap();
    let base = delivery(&verification(1_234_567));
    let mut invalid_email = base.payload().clone();
    invalid_email["email"] = json!("invalid");
    for (format, payload) in [
        (base.format().clone(), json!({})),
        (base.format().clone(), invalid_email),
        (TaskFormat::new(base.format().task_type(), 3).unwrap(), base.payload().clone()),
    ] {
        let task = TaskDelivery::new(base.id(), format, payload, base.metadata().clone(), base.attempt()).unwrap();
        assert!(matches!(handler.handle(&task).await, Err(TaskHandlerError::Permanent { .. })));
    }
    assert!(records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn verification_preserves_sender_failures_and_backoff() {
    let task = delivery(&verification(1_234_567));
    for result in [
        Err(TaskHandlerError::Retry {
            reason: "sender unavailable".into(),
            retry_after: Some(Duration::from_secs(3)),
        }),
        Err(TaskHandlerError::Permanent {
            reason: "sender rejected operation".into(),
        }),
    ] {
        let records = Arc::new(Mutex::new(Vec::new()));
        let mut handler = send_verification_code_task_handler(Box::new(RecordingSender {
            records: records.clone(),
            result: result.clone(),
        }))
        .unwrap();
        assert_result(handler.handle(&task).await, result);
        assert_eq!(records.lock().unwrap().len(), 1);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Operation {
    Create(CreateAccountTaskV1),
    Record(RecordAccountCreatedTaskV1),
}

struct RecordingRegistration {
    calls: Mutex<Vec<Operation>>,
    result: Result<(), ContinuationError>,
}

#[async_trait]
impl RegistrationService for RecordingRegistration {
    async fn submit_profile(&self, _: ProfileSubmission) -> Result<AcceptedSubmission, SubmissionError> {
        unreachable!("continuation handlers must not submit profiles")
    }
    async fn create_account(&self, payload: CreateAccountTaskV1) -> Result<(), ContinuationError> {
        self.calls.lock().unwrap().push(Operation::Create(payload));
        self.result.clone()
    }
    async fn record_account_created(&self, payload: RecordAccountCreatedTaskV1) -> Result<(), ContinuationError> {
        self.calls.lock().unwrap().push(Operation::Record(payload));
        self.result.clone()
    }
}

fn create_payload() -> CreateAccountTaskV1 {
    let verification = verification(1_234_567);
    CreateAccountTaskV1 {
        email: verification.email,
        account_id: verification.account_id,
        profile: RegistrationProfile::new(
            "Alice".into(),
            "Smith".into(),
            NaiveDate::from_ymd_opt(1990, 5, 12).unwrap(),
            NaiveDate::from_ymd_opt(2026, 9, 20).unwrap(),
        ),
    }
}

fn record_payload() -> RecordAccountCreatedTaskV1 {
    let create = create_payload();
    RecordAccountCreatedTaskV1 {
        email: create.email,
        account_id: create.account_id,
        profile: create.profile,
        created_at: "2026-09-20T12:00:00Z".parse().unwrap(),
    }
}

#[tokio::test]
async fn continuations_forward_decoded_inputs_and_preserve_business_classification() {
    for (result, expected) in [
        (Ok(()), Ok(())),
        (
            Err(ContinuationError::Retry),
            Err(TaskHandlerError::Retry {
                reason: "registration continuation temporarily unavailable".into(),
                retry_after: None,
            }),
        ),
        (
            Err(ContinuationError::Permanent),
            Err(TaskHandlerError::Permanent {
                reason: "registration continuation data does not match".into(),
            }),
        ),
    ] {
        let service = Arc::new(RecordingRegistration {
            calls: Mutex::default(),
            result,
        });
        let create = create_payload();
        let record = record_payload();
        let mut create_handler = create_account_task_handler(service.clone()).unwrap();
        let mut record_handler = record_account_created_task_handler(service.clone()).unwrap();
        assert_eq!(
            create_handler.formats(),
            vec![TaskFormat::of::<CreateAccountTaskV1>().unwrap()]
        );
        assert_eq!(
            record_handler.formats(),
            vec![TaskFormat::of::<RecordAccountCreatedTaskV1>().unwrap()]
        );
        assert_result(create_handler.handle(&delivery(&create)).await, expected.clone());
        assert_result(record_handler.handle(&delivery(&record)).await, expected);
        assert_eq!(
            *service.calls.lock().unwrap(),
            vec![Operation::Create(create), Operation::Record(record)]
        );
    }
}
