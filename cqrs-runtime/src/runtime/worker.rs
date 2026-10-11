//! One mutable dispatcher and cloned async-channel receiver per permanent Tokio task.
use crate::task::{
    executor::port::TaskExecutor,
    handling::dispatcher::TaskDispatcher,
    queue::{
        lease::LeasedTask,
        settlement::{SettlementResult, TaskFailure},
    },
};
use async_channel::Receiver;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub(super) async fn run_task_worker(
    index: usize,
    mut dispatcher: TaskDispatcher,
    executor: Arc<dyn TaskExecutor>,
    receiver: Receiver<LeasedTask>,
    capacity_available: Arc<Notify>,
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
        capacity_available.notify_one();
        if stop.is_cancelled() {
            break;
        }
        let id = task.attempt().id();
        // Graceful stop does not cancel the current invocation or settlement.
        match executor.execute_and_settle(&mut dispatcher, task, handler_timeout).await {
            Ok(SettlementResult::Failed(failed)) => log_failed(&failed),
            Ok(SettlementResult::Completed | SettlementResult::RetryScheduled) => {}
            Ok(SettlementResult::OwnershipLost) => error!(task_id = %id, "task ownership lost"),
            Err(error) => error!(task_id = %id, error = %error, "task settlement failed"),
        }
    }
    index
}

pub(super) fn log_failed(task: &TaskFailure) {
    let task_type = task.format.task_type();
    let safe_type = crate::diagnostics::safe_type(task_type);
    error!(
        task_id = %task.id,
        task_type = %safe_type,
        task_version = task.format.task_version(),
        attempt = task.attempt,
        reason = %task.cause.as_str(),
        "task failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{data::format::TaskFormat, queue::settlement::TaskFailureCause};
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };
    use tracing_subscriber::fmt::MakeWriter;

    #[tokio::test]
    async fn a_closed_channel_drains_its_ready_delivery_and_then_stops_the_worker() {
        use crate::task::{executor::default_executor::task_executor, test_support::*};
        let queue = Arc::new(Queue::default());
        let (sender, receiver) = async_channel::bounded(1);
        let task = claimed();
        let id = task.attempt().id();
        sender.send(task).await.unwrap();
        drop(sender);
        let stop = CancellationToken::new();
        let index = run_task_worker(
            7,
            dispatcher(adapter()),
            task_executor(queue.clone()),
            receiver,
            Arc::new(Notify::new()),
            Duration::from_secs(1),
            stop.clone(),
        )
        .await;
        assert_eq!(index, 7);
        assert!(!stop.is_cancelled());
        assert_eq!(
            *queue.outcomes.lock().unwrap(),
            vec![(id, crate::task::queue::settlement::SettlementRequest::Completed)]
        );
    }

    #[tokio::test]
    async fn cancellation_after_dequeue_prevents_handler_execution_and_settlement() {
        use crate::task::{executor::default_executor::task_executor, test_support::*};
        use std::{
            future::Future,
            sync::atomic::{AtomicUsize, Ordering},
            task::{Context, Poll, Wake, Waker},
        };
        struct CancelOnWake(CancellationToken);
        impl Wake for CancelOnWake {
            fn wake(self: Arc<Self>) {
                self.0.cancel();
            }
        }
        let stop = CancellationToken::new();
        let capacity = Arc::new(Notify::new());
        let mut notification = Box::pin(capacity.notified());
        let waker = Waker::from(Arc::new(CancelOnWake(stop.clone())));
        assert!(matches!(
            notification.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        let queue = Arc::new(Queue::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = async_channel::bounded(1);
        sender.send(claimed()).await.unwrap();
        let index = run_task_worker(
            3,
            dispatcher(CountingAdapter(calls.clone())),
            task_executor(queue.clone()),
            receiver.clone(),
            capacity.clone(),
            Duration::from_secs(1),
            stop.clone(),
        )
        .await;
        assert_eq!(index, 3);
        assert!(stop.is_cancelled());
        assert!(receiver.is_empty(), "cancellation must happen after recv consumed the lease");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            queue.outcomes.lock().unwrap().is_empty(),
            "the lease must remain available for durable recovery"
        );
    }
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
                TaskFailureCause::PermanentRejection,
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
        let expected = crate::task::test_support::claimed();
        assert!(sender.try_send(expected.clone()).is_ok());
        // Cancel after the waiter is notified, without polling it to Ready.
        drop(receive);
        let received = other.try_recv().unwrap();
        assert_eq!(received.attempt().id(), expected.attempt().id());
        assert_eq!(received.attempt().payload(), expected.attempt().payload());
        assert_eq!(received.lease().lock_token(), expected.lease().lock_token());
        assert_eq!(received.lease().deadline(), expected.lease().deadline());
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn idle_worker_cancels_pending_receive_while_sender_is_open() {
        use crate::task::{
            executor::default_executor::task_executor,
            test_support::{Queue, adapter},
        };
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };

        let queue = Arc::new(Queue::default());
        let (sender, receiver) = async_channel::bounded(1);
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let mut worker = Box::pin(run_task_worker(
            0,
            TaskDispatcher::try_new(vec![Box::new(adapter())]).unwrap(),
            task_executor(queue.clone()),
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
