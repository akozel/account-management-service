//! One mutable registry and cloned async-channel receiver per permanent Tokio task.
use crate::outbox::{ClaimedTask, OutboxTaskCoordinator, SettlementResult, TaskFailure, TaskHandlerRegistry};
use async_channel::Receiver;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub(super) async fn run_worker(
    index: usize,
    mut handler: TaskHandlerRegistry,
    coordinator: Arc<dyn OutboxTaskCoordinator>,
    receiver: Receiver<ClaimedTask>,
    available: Arc<Notify>,
    handler_timeout: Duration,
    stop: CancellationToken,
) -> usize {
    loop {
        let task = tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            task = receiver.recv() => task,
        };
        let Ok(task) = task else {
            break;
        };
        available.notify_one();
        if stop.is_cancelled() {
            break;
        }
        let id = task.delivery().id();
        // Graceful stop does not cancel the current invocation or settlement.
        match coordinator.execute_and_settle(&mut handler, task, handler_timeout).await {
            Ok(SettlementResult::Failed(failed)) => log_failed(&failed),
            Ok(SettlementResult::Completed | SettlementResult::RetryScheduled) => {}
            Ok(SettlementResult::OwnershipLost) => error!(task_id = %id, "outbox ownership lost"),
            Err(error) => error!(task_id = %id, error = %error, "outbox acknowledgement failed"),
        }
    }
    index
}

pub(super) fn log_failed(task: &TaskFailure) {
    let task_type = task.format.task_type();
    let safe_type = if task_type.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        task_type
    } else {
        "unknown"
    };
    error!(
        task_id = %task.id,
        task_type = %safe_type,
        task_version = task.format.task_version(),
        attempt = task.attempt,
        reason = %task.cause.as_str(),
        "outbox task failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::{TaskFailureCause, TaskFormat};
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };
    use tracing_subscriber::fmt::MakeWriter;
    #[derive(Clone, Default)]
    struct RecordingWriter(Arc<Mutex<Vec<u8>>>);

    impl RecordingWriter {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for RecordingWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for RecordingWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn confirmed_failure_events_contain_identity_and_safe_reason_only() {
        let writer = RecordingWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_target(false)
            .with_ansi(false)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            for cause in [
                TaskFailureCause::HandlerPermanent,
                TaskFailureCause::AttemptLimitReached,
                TaskFailureCause::LeaseExpired,
            ] {
                let task = TaskFailure {
                    id: uuid::Uuid::from_u128(1),
                    format: TaskFormat::new("create_account", 1).unwrap(),
                    attempt: 2,
                    cause,
                };
                log_failed(&task);
            }
            let task = TaskFailure {
                id: uuid::Uuid::from_u128(2),
                format: TaskFormat::new("secret\nalice@example.com", 1).unwrap(),
                attempt: 1,
                cause: TaskFailureCause::LeaseExpired,
            };
            log_failed(&task);
        });

        let log = writer.text();
        assert_eq!(log.lines().count(), 4);
        assert!(log.contains("task_id=00000000-0000-0000-0000-000000000001"));
        assert!(log.contains("task_type=create_account task_version=1 attempt=2"));
        for reason in ["permanent failure", "retry limit reached", "worker lease expired"] {
            assert!(log.contains(reason));
        }
        assert!(log.contains("task_type=unknown"));
        assert!(!log.contains("secret"));
        assert!(!log.contains("alice@example.com"));
        assert!(!log.contains("token"));
    }

    #[test]
    fn cancelling_pending_receive_after_send_preserves_delivery_and_lease() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let (sender, receiver) = async_channel::bounded(1);
        let other = receiver.clone();
        let mut receive = Box::pin(receiver.recv());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));
        let expected = crate::outbox::test_support::claimed();
        assert!(sender.try_send(expected.clone()).is_ok());
        // Cancel after the waiter is notified, without polling it to Ready.
        drop(receive);
        let received = other.try_recv().unwrap();
        assert_eq!(received.delivery().id(), expected.delivery().id());
        assert_eq!(received.delivery().payload(), expected.delivery().payload());
        assert_eq!(received.lease().lock_token(), expected.lease().lock_token());
        assert_eq!(received.lease().deadline(), expected.lease().deadline());
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn idle_worker_cancels_pending_receive_while_sender_is_open() {
        use crate::outbox::{
            outbox_task_coordinator,
            test_support::{Queue, format, handler},
        };
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };

        let queue = Arc::new(Queue::default());
        let (sender, receiver) = async_channel::bounded(1);
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let mut worker = Box::pin(run_worker(
            0,
            TaskHandlerRegistry::try_new(vec![Box::new(handler())]).unwrap(),
            outbox_task_coordinator(queue.clone(), vec![format()]),
            receiver.clone(),
            Arc::new(Notify::new()),
            Duration::from_secs(30),
            shutdown,
        ));
        // Poll explicitly so shutdown exercises a pending recv, without sleeps.
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(worker.as_mut().poll(&mut context), Poll::Pending));
        assert!(!sender.is_closed());
        stop.cancel();
        assert_eq!(tokio::time::timeout(Duration::from_secs(1), worker).await.unwrap(), 0);
        assert!(!sender.is_closed(), "shutdown must not depend on channel closure");
        assert!(receiver.is_empty());
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }
}
