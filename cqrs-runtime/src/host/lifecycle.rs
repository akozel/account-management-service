#[cfg(test)]
use crate::runtime::dispatcher_factory::WorkerSetupError;
use std::{future::Future, io};

use crate::runtime::supervisor::TaskRuntimeError;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;

use crate::host::error::HostError;

type TaskRuntimeExit = Result<Result<(), TaskRuntimeError>, JoinError>;

enum StopReason {
    Signal(io::Result<()>),
    Requested,
    Inbound(io::Result<()>),
    TaskRuntime(TaskRuntimeExit),
}

/// Coordinate signal, shared-token cancellation, or unexpected component exit.
///
/// Creating this future starts nothing. Its first poll installs a cancellation
/// guard before spawning `tasks` on Tokio. Inbound and signal futures run in
/// this future and need neither `Send` nor `'static`.
///
/// Observation priority is signal, token cancellation, inbound, then tasks.
/// An unexpected exit remains the primary error; requested shutdown prefers
/// signal-observation errors, then tasks errors, then inbound errors.
/// Secondary failures are logged separately.
/// Fatal tasks errors cancel the shared token before draining, allowing inbound
/// to stop admission immediately. Both components finish before resource cleanup;
/// Inbound and resource cleanup currently have no process-wide shutdown deadline.
/// Dropping or aborting a polled host requests cancellation and detaches the
/// tasks task, allowing its own drain to continue on a live Tokio runtime.
/// The inbound future is dropped; the host cannot await components or guarantee
/// resource cleanup in that case. Normal return waits for both components.
pub async fn run<Inbound, Tasks, Signal>(
    inbound: Inbound,
    tasks: Tasks,
    shutdown: CancellationToken,
    signal: Signal,
) -> Result<(), HostError>
where
    Inbound: Future<Output = io::Result<()>>,
    Tasks: Future<Output = Result<(), TaskRuntimeError>> + Send + 'static,
    Signal: Future<Output = io::Result<()>>,
{
    let _cancel_on_exit = shutdown.clone().drop_guard();
    let task_runtime = tokio::spawn(tasks);
    tokio::pin!(inbound);
    tokio::pin!(signal);
    tokio::pin!(task_runtime);

    let reason = tokio::select! {
        biased;
        result = &mut signal => StopReason::Signal(result),
        _ = shutdown.cancelled() => StopReason::Requested,
        result = &mut inbound => StopReason::Inbound(result),
        result = &mut task_runtime => StopReason::TaskRuntime(result),
    };
    shutdown.cancel();

    match reason {
        StopReason::Signal(signal_result) => {
            let (inbound_result, supervisor_result) = tokio::join!(&mut inbound, &mut task_runtime);
            preserve_primary_failure(
                signal_result.map_err(HostError::ShutdownSignal),
                preserve_primary_failure(
                    expected_runtime_exit(supervisor_result),
                    expected_inbound_exit(inbound_result),
                ),
            )
        }
        StopReason::Requested => {
            let (inbound_result, supervisor_result) = tokio::join!(&mut inbound, &mut task_runtime);
            preserve_primary_failure(
                expected_runtime_exit(supervisor_result),
                expected_inbound_exit(inbound_result),
            )
        }
        StopReason::Inbound(inbound_result) => {
            let primary = match inbound_result {
                Ok(()) => HostError::InboundStopped,
                Err(error) => HostError::Inbound(error),
            };
            preserve_primary_failure(Err(primary), expected_runtime_exit((&mut task_runtime).await))
        }
        StopReason::TaskRuntime(supervisor_result) => preserve_primary_failure(
            unexpected_runtime_exit(supervisor_result),
            expected_inbound_exit((&mut inbound).await),
        ),
    }
}

fn preserve_primary_failure(primary: Result<(), HostError>, secondary: Result<(), HostError>) -> Result<(), HostError> {
    match primary {
        Ok(()) => secondary,
        Err(error) => {
            if let Err(secondary) = secondary {
                secondary.log("additional failure during host shutdown");
            }
            Err(error)
        }
    }
}

fn expected_inbound_exit(result: io::Result<()>) -> Result<(), HostError> {
    result.map_err(HostError::Inbound)
}

fn expected_runtime_exit(result: TaskRuntimeExit) -> Result<(), HostError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(HostError::TaskRuntime(error)),
        Err(error) => Err(HostError::TaskRuntimeJoin(error)),
    }
}

fn unexpected_runtime_exit(result: TaskRuntimeExit) -> Result<(), HostError> {
    match result {
        Ok(Ok(())) => Err(HostError::TaskRuntimeStopped),
        other => expected_runtime_exit(other),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::*;

    async fn wait_for_shutdown(shutdown: CancellationToken, stopped: Arc<AtomicBool>) {
        shutdown.cancelled().await;
        stopped.store(true, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn signal_drains_inbound_and_outbox_before_returning() {
        let shutdown = CancellationToken::new();
        let inbound_shutdown = shutdown.clone();
        let worker_shutdown = shutdown.clone();
        let inbound_stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = Arc::new(AtomicBool::new(false));
        let inbound = {
            let stopped = inbound_stopped.clone();
            async move {
                wait_for_shutdown(inbound_shutdown, stopped).await;
                Ok(())
            }
        };
        let task_runtime = {
            let stopped = worker_stopped.clone();
            async move {
                wait_for_shutdown(worker_shutdown, stopped).await;
                Ok(())
            }
        };

        let result = run(inbound, task_runtime, shutdown, async { Ok(()) }).await;

        assert!(result.is_ok());
        assert!(inbound_stopped.load(Ordering::SeqCst));
        assert!(worker_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn signal_failure_still_drains_components() {
        let shutdown = CancellationToken::new();
        let inbound_shutdown = shutdown.clone();
        let worker_shutdown = shutdown.clone();
        let inbound_stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = Arc::new(AtomicBool::new(false));
        let inbound = {
            let stopped = inbound_stopped.clone();
            async move {
                wait_for_shutdown(inbound_shutdown, stopped).await;
                Ok(())
            }
        };
        let task_runtime = {
            let stopped = worker_stopped.clone();
            async move {
                wait_for_shutdown(worker_shutdown, stopped).await;
                Ok(())
            }
        };
        let signal = async { Err(io::Error::other("signal handler failed")) };

        let result = run(inbound, task_runtime, shutdown, signal).await;

        assert!(matches!(result, Err(HostError::ShutdownSignal(_))));
        assert!(inbound_stopped.load(Ordering::SeqCst));
        assert!(worker_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn inbound_failure_stops_outbox_before_returning_error() {
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let worker_stopped = Arc::new(AtomicBool::new(false));
        let task_runtime = {
            let stopped = worker_stopped.clone();
            async move {
                wait_for_shutdown(worker_shutdown, stopped).await;
                Ok(())
            }
        };
        let inbound = async { Err(io::Error::other("Inbound failed")) };
        let signal = std::future::pending();

        let result = run(inbound, task_runtime, shutdown, signal).await;

        assert!(matches!(result, Err(HostError::Inbound(_))));
        assert!(worker_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unexpected_outbox_exit_stops_inbound_before_returning_error() {
        let shutdown = CancellationToken::new();
        let inbound_shutdown = shutdown.clone();
        let inbound_stopped = Arc::new(AtomicBool::new(false));
        let inbound = {
            let stopped = inbound_stopped.clone();
            async move {
                wait_for_shutdown(inbound_shutdown, stopped).await;
                Ok(())
            }
        };
        let task_runtime = async { Ok(()) };
        let signal = std::future::pending();

        let result = run(inbound, task_runtime, shutdown, signal).await;

        assert!(matches!(result, Err(HostError::TaskRuntimeStopped)));
        assert!(inbound_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn outbox_panic_stops_inbound_before_returning_join_error() {
        let shutdown = CancellationToken::new();
        let inbound_shutdown = shutdown.clone();
        let inbound_stopped = Arc::new(AtomicBool::new(false));
        let inbound = {
            let stopped = inbound_stopped.clone();
            async move {
                wait_for_shutdown(inbound_shutdown, stopped).await;
                Ok(())
            }
        };
        let task_runtime = async {
            panic!("tasks panic");
            #[allow(unreachable_code)]
            Ok(())
        };
        let signal = std::future::pending();

        let result = run(inbound, task_runtime, shutdown, signal).await;

        assert!(matches!(result, Err(HostError::TaskRuntimeJoin(_))));
        assert!(inbound_stopped.load(Ordering::SeqCst));
    }
    fn outbox_error() -> TaskRuntimeError {
        TaskRuntimeError::WorkerSetup(WorkerSetupError::FormatsMismatch { worker_index: 7 })
    }

    #[tokio::test]
    async fn shared_cancellation_stops_inbound_before_outbox_drain_finishes() {
        let shutdown = CancellationToken::new();
        let (fail, failure) = tokio::sync::oneshot::channel::<()>();
        let (release, drain) = tokio::sync::oneshot::channel::<()>();
        let (inbound_started, inbound_shutdown_started) = tokio::sync::oneshot::channel();
        let worker_shutdown = shutdown.clone();
        let task_runtime = async move {
            failure.await.unwrap();
            worker_shutdown.cancel();
            drain.await.unwrap();
            Err(outbox_error())
        };
        let inbound_shutdown = shutdown.clone();
        let inbound = async move {
            inbound_shutdown.cancelled().await;
            inbound_started.send(()).unwrap();
            Ok(())
        };
        let runtime = tokio::spawn(run(inbound, task_runtime, shutdown, std::future::pending()));
        fail.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), inbound_shutdown_started)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !runtime.is_finished(),
            "Inbound must observe cancellation while tasks still drains"
        );
        release.send(()).unwrap();
        assert!(matches!(
            runtime.await.unwrap(),
            Err(HostError::TaskRuntime(TaskRuntimeError::WorkerSetup(
                WorkerSetupError::FormatsMismatch { worker_index: 7 }
            )))
        ));
    }

    #[tokio::test]
    async fn cancellation_and_signal_win_over_simultaneous_successful_component_exits() {
        for signal_ready in [false, true] {
            let shutdown = CancellationToken::new();
            if !signal_ready {
                shutdown.cancel();
            }
            let task_runtime = async { Ok(()) };
            let signal = async move { if signal_ready { Ok(()) } else { std::future::pending().await } };
            assert!(run(async { Ok(()) }, task_runtime, shutdown, signal).await.is_ok());
        }
    }

    #[tokio::test]
    async fn clean_inbound_exit_without_cancellation_is_unexpected() {
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let task_runtime = async move {
            worker_shutdown.cancelled().await;
            Ok(())
        };
        assert!(matches!(
            run(async { Ok(()) }, task_runtime, shutdown, std::future::pending()).await,
            Err(HostError::InboundStopped)
        ));
    }

    #[tokio::test]
    async fn dropping_host_requests_shutdown_and_detaches_outbox() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let (stopped, worker_stopped) = tokio::sync::oneshot::channel();
        let task_runtime = async move {
            worker_shutdown.cancelled().await;
            stopped.send(()).unwrap();
            Ok(())
        };
        let mut runtime = Box::pin(run(
            std::future::pending(),
            task_runtime,
            shutdown.clone(),
            std::future::pending(),
        ));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(runtime.as_mut().poll(&mut context), Poll::Pending));
        drop(runtime);
        assert!(shutdown.is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(1), worker_stopped)
            .await
            .unwrap()
            .unwrap();
    }

    #[derive(Clone, Default)]
    struct RecordingWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for RecordingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RecordingWriter {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[tokio::test]
    async fn primary_failures_survive_shutdown_and_secondary_failures_are_logged() {
        use tracing::instrument::WithSubscriber;
        for initiator in ["signal_failure", "signal", "token", "inbound", "task_runtime"] {
            let writer = RecordingWriter::default();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(writer.clone())
                .with_ansi(false)
                .without_time()
                .finish();
            let dispatch = tracing::Dispatch::new(subscriber);
            // Another test can first register the shared log callsite without
            // a subscriber. Prime it and rebuild its interest under this capture
            // before exercising the error matrix concurrently with those tests.
            tracing::dispatcher::with_default(&dispatch, || {
                HostError::InboundStopped.log("initialize log capture");
                tracing::callsite::rebuild_interest_cache();
            });
            writer.0.lock().unwrap().clear();
            let shutdown = CancellationToken::new();
            if initiator == "token" {
                shutdown.cancel();
            }
            let inbound_shutdown = shutdown.clone();
            let inbound = async move {
                if initiator != "inbound" {
                    inbound_shutdown.cancelled().await;
                }
                Err(io::Error::other("private_inbound_source"))
            };
            let worker_shutdown = shutdown.clone();
            let task_runtime = async move {
                if initiator == "inbound" {
                    worker_shutdown.cancelled().await;
                }
                Err(outbox_error())
            };
            let signal = async move {
                match initiator {
                    "signal_failure" => Err(io::Error::other("private_signal_source")),
                    "signal" => Ok(()),
                    _ => std::future::pending().await,
                }
            };
            let result = run(inbound, task_runtime, shutdown, signal).with_subscriber(dispatch).await;
            match initiator {
                "signal_failure" => assert!(matches!(result, Err(HostError::ShutdownSignal(_)))),
                "inbound" => assert!(matches!(result, Err(HostError::Inbound(_)))),
                _ => assert!(matches!(result, Err(HostError::TaskRuntime(_)))),
            }
            let log = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
            assert!(!log.contains("private_"));
            assert_eq!(
                log.matches("additional failure during host shutdown").count(),
                if initiator == "signal_failure" { 2 } else { 1 }
            );
            if initiator == "inbound" || initiator == "signal_failure" {
                assert!(log.contains("TaskRuntime"));
            }
            if initiator != "inbound" {
                assert!(log.contains("Inbound"));
            }
        }
    }

    #[tokio::test]
    async fn creating_and_dropping_unpolled_host_starts_nothing() {
        let shutdown = CancellationToken::new();
        let started = Arc::new(AtomicBool::new(false));
        let outbox_started = started.clone();
        let inbound_started = started.clone();
        let signal_started = started.clone();
        let host = run(
            async move {
                inbound_started.store(true, Ordering::SeqCst);
                Ok(())
            },
            async move {
                outbox_started.store(true, Ordering::SeqCst);
                Ok(())
            },
            shutdown.clone(),
            async move {
                signal_started.store(true, Ordering::SeqCst);
                Ok(())
            },
        );
        tokio::task::yield_now().await;
        assert!(!started.load(Ordering::SeqCst));
        drop(host);
        tokio::task::yield_now().await;
        assert!(!started.load(Ordering::SeqCst));
        assert!(!shutdown.is_cancelled());
    }

    #[tokio::test]
    async fn drop_and_abort_allow_started_outbox_work_to_drain() {
        use std::task::{Context, Poll, Waker};
        use std::time::Duration;
        use tokio::sync::oneshot;
        for abort in [false, true] {
            let shutdown = CancellationToken::new();
            let token = shutdown.clone();
            let (started, work_started) = oneshot::channel();
            let (cancelled, cancellation_observed) = oneshot::channel();
            let (release, drain) = oneshot::channel();
            let (finished, work_finished) = oneshot::channel();
            let tasks = async move {
                started.send(()).unwrap();
                token.cancelled().await;
                cancelled.send(()).unwrap();
                drain.await.unwrap();
                finished.send(()).unwrap();
                Ok(())
            };
            let mut host = Box::pin(run(std::future::pending(), tasks, shutdown.clone(), std::future::pending()));
            assert!(matches!(
                host.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            tokio::time::timeout(Duration::from_secs(1), work_started)
                .await
                .unwrap()
                .unwrap();
            if abort {
                let task = tokio::spawn(host);
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                drop(host);
            }
            assert!(shutdown.is_cancelled());
            tokio::time::timeout(Duration::from_secs(1), cancellation_observed)
                .await
                .unwrap()
                .unwrap();
            assert!(!release.is_closed(), "host must leave tasks's active work alive");
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), work_finished)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn normal_return_waits_for_both_drains_before_cleanup() {
        use std::time::Duration;
        use tokio::sync::oneshot;
        for inbound_first in [false, true] {
            let shutdown = CancellationToken::new();
            let token = shutdown.clone();
            let host_token = shutdown.clone();
            let (inbound_release, inbound_drain) = oneshot::channel();
            let (inbound_stopped, inbound_finished) = oneshot::channel();
            let (outbox_release, outbox_drain) = oneshot::channel();
            let (outbox_stopped, outbox_finished) = oneshot::channel();
            let cleanup = Arc::new(AtomicBool::new(false));
            let cleanup_started = cleanup.clone();
            let host = tokio::spawn(async move {
                let result = run(
                    async move {
                        shutdown.cancelled().await;
                        inbound_drain.await.unwrap();
                        inbound_stopped.send(()).unwrap();
                        Ok(())
                    },
                    async move {
                        token.cancelled().await;
                        outbox_drain.await.unwrap();
                        outbox_stopped.send(()).unwrap();
                        Ok(())
                    },
                    host_token,
                    async { Ok(()) },
                )
                .await;
                cleanup_started.store(true, Ordering::SeqCst);
                result
            });
            if inbound_first {
                inbound_release.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(1), inbound_finished)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!host.is_finished());
                assert!(!cleanup.load(Ordering::SeqCst));
                outbox_release.send(()).unwrap();
                outbox_finished.await.unwrap();
            } else {
                outbox_release.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(1), outbox_finished)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!host.is_finished());
                assert!(!cleanup.load(Ordering::SeqCst));
                inbound_release.send(()).unwrap();
                inbound_finished.await.unwrap();
            }
            assert!(
                tokio::time::timeout(Duration::from_secs(1), host)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_ok()
            );
            assert!(cleanup.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn inbound_and_signal_can_borrow_non_send_state() {
        use std::rc::Rc;
        let local = Rc::new(());
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let result = run(
            async {
                let _borrow = &local;
                Ok(())
            },
            async move {
                token.cancelled().await;
                Ok(())
            },
            shutdown,
            async {
                let _borrow = &local;
                Ok(())
            },
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn signal_error_wins_over_ready_token_and_components() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let result = run(
            async { Err(io::Error::other("inbound failure")) },
            async { Err(outbox_error()) },
            shutdown,
            async { Err(io::Error::other("signal failure")) },
        )
        .await;
        assert!(matches!(result, Err(HostError::ShutdownSignal(_))));
    }

    #[tokio::test]
    async fn inbound_exit_wins_over_simultaneous_outbox_exit() {
        use std::task::{Context, Poll, Waker};
        let ready = Arc::new(AtomicBool::new(false));
        let outbox_ready = ready.clone();
        let inbound_ready = ready.clone();
        let inbound = std::future::poll_fn(move |_| {
            if inbound_ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        });
        let tasks = async move {
            outbox_ready.store(true, Ordering::SeqCst);
            Ok(())
        };
        let mut host = Box::pin(run(inbound, tasks, CancellationToken::new(), std::future::pending()));
        assert!(matches!(
            host.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        // The tasks task completes before the host gets its next poll.
        tokio::task::yield_now().await;
        assert!(ready.load(Ordering::SeqCst));
        assert!(matches!(host.await, Err(HostError::InboundStopped)));
    }
}
