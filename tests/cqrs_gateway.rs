use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use account_management_service::{
    application::{
        CommandGatewayError,
        email_reservation::{gateway::EmailReservationGateway, tasks::send_verification_code::v1::SendVerificationCodeTaskV1},
        outbox::NewOutboxTask,
    },
    domain::{Email, EmailReservation, EmailReservationCommand, EmailReservationError, EmailReservationEvent, UserAccountId},
    infrastructure::cqrs::{AggregateConflictRetryPolicy, email_reservation_gateway::CqrsEmailReservationGateway},
};
use cqrs_es::{AggregateError, CqrsFramework, EventEnvelope, EventStore, mem_store::MemStore};

#[tokio::test]
async fn executes_command_and_preserves_domain_rejection() {
    let store = MemStore::<EmailReservation>::default();
    let gateway = CqrsEmailReservationGateway::new(
        move |tasks: Vec<NewOutboxTask>| {
            assert!(tasks.is_empty());
            CqrsFramework::new(store.clone(), Vec::new(), ())
        },
        AggregateConflictRetryPolicy::default(),
    );
    let email = Email::parse("alice@example.com").expect("test email must be valid");
    let account_id: UserAccountId = "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse().unwrap();

    assert_eq!(
        gateway
            .execute(EmailReservationCommand::ReserveEmail {
                email: email.clone(),
                account_id,
                verification_code: 1_234_567
            })
            .await,
        Ok(())
    );
    assert_eq!(
        gateway
            .execute(EmailReservationCommand::ReserveEmail {
                email,
                account_id: UserAccountId::new(),
                verification_code: 7_654_321
            })
            .await,
        Err(CommandGatewayError::Domain(EmailReservationError::AlreadyReserved))
    );
}

#[derive(Clone)]
struct ConflictOnceStore {
    inner: MemStore<EmailReservation>,
    competing_event: EmailReservationEvent,
    loads: Arc<AtomicUsize>,
    commits: Arc<AtomicUsize>,
}

impl EventStore<EmailReservation> for ConflictOnceStore {
    type AC = <MemStore<EmailReservation> as EventStore<EmailReservation>>::AC;

    async fn load_events(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<EventEnvelope<EmailReservation>>, AggregateError<EmailReservationError>> {
        self.inner.load_events(aggregate_id).await
    }

    async fn load_aggregate(&self, aggregate_id: &str) -> Result<Self::AC, AggregateError<EmailReservationError>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.inner.load_aggregate(aggregate_id).await
    }

    async fn commit(
        &self,
        events: Vec<EmailReservationEvent>,
        context: Self::AC,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<EmailReservation>>, AggregateError<EmailReservationError>> {
        if self.commits.fetch_add(1, Ordering::SeqCst) == 0 {
            self.inner
                .commit(vec![self.competing_event.clone()], context, metadata)
                .await?;
            return Err(AggregateError::AggregateConflict);
        }
        self.inner.commit(events, context, metadata).await
    }
}

#[tokio::test]
async fn conflict_retry_reuses_one_framework_and_reloads_before_returning_domain_outcome() {
    let email = Email::parse("alice@example.com").unwrap();
    let account_id: UserAccountId = "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse().unwrap();
    let inner = MemStore::<EmailReservation>::default();
    let loads = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let builds = Arc::new(AtomicUsize::new(0));
    let task_ids = Arc::new(Mutex::new(Vec::new()));
    let store = ConflictOnceStore {
        inner: inner.clone(),
        competing_event: EmailReservationEvent::EmailReserved {
            email: email.clone(),
            account_id,
            verification_code: 1_234_567,
        },
        loads: loads.clone(),
        commits: commits.clone(),
    };
    let built = builds.clone();
    let observed_task_ids = task_ids.clone();
    let gateway = CqrsEmailReservationGateway::new(
        move |tasks: Vec<NewOutboxTask>| {
            built.fetch_add(1, Ordering::SeqCst);
            observed_task_ids.lock().unwrap().extend(tasks.iter().map(|task| task.id));
            CqrsFramework::new(store.clone(), Vec::new(), ())
        },
        AggregateConflictRetryPolicy::new(3, Duration::ZERO, Duration::ZERO, 0.0),
    );
    let command = EmailReservationCommand::ReserveEmail {
        email: email.clone(),
        account_id,
        verification_code: 1_234_567,
    };
    let task = NewOutboxTask::new(SendVerificationCodeTaskV1 {
        email,
        account_id,
        verification_code: 1_234_567,
    })
    .unwrap();

    assert_eq!(
        gateway.execute_with_outbox(command, vec![task.clone()]).await,
        Err(CommandGatewayError::Domain(EmailReservationError::AlreadyReserved))
    );
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(loads.load(Ordering::SeqCst), 2);
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert_eq!(*task_ids.lock().unwrap(), vec![task.id]);
    assert_eq!(inner.load_events("alice@example.com").await.unwrap().len(), 1);
}
