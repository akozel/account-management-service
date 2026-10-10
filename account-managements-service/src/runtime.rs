//! Coordinates graceful shutdown across the service's inbound adapters.

use std::{fmt, future::Future, io};

use cqrs_runtime::worker::WorkerPoolError;
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::error;

type WorkerExit = Result<Result<(), WorkerPoolError>, JoinError>;

#[derive(Error)]
pub enum RuntimeError {
    #[error("failed to observe a shutdown signal")]
    ShutdownSignal(#[source] io::Error),
    #[error("HTTP server failed")]
    HttpServer(#[source] io::Error),
    #[error("HTTP server terminated before shutdown")]
    HttpServerStopped,
    #[error("outbox supervisor task failed")]
    OutboxSupervisorTask(#[source] JoinError),
    #[error("outbox supervisor failed")]
    OutboxSupervisor(#[source] WorkerPoolError),
    #[error("outbox supervisor terminated before shutdown")]
    OutboxSupervisorStopped,
}

// Debug is also used by Rust's Result termination report. Never format a
// JoinError directly: both its Debug and Display can reveal the panic payload.
impl fmt::Debug for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShutdownSignal(error) => f.debug_tuple("ShutdownSignal").field(error).finish(),
            Self::HttpServer(error) => f.debug_tuple("HttpServer").field(error).finish(),
            Self::OutboxSupervisor(error) => f.debug_tuple("OutboxSupervisor").field(error).finish(),
            Self::OutboxSupervisorTask(error) => f
                .debug_struct("OutboxSupervisorTask")
                .field("task_id", &error.id())
                .field("panicked", &error.is_panic())
                .field("cancelled", &error.is_cancelled())
                .finish(),
            Self::HttpServerStopped | Self::OutboxSupervisorStopped => fmt::Display::fmt(self, f),
        }
    }
}

impl RuntimeError {
    pub fn log(&self, message: &'static str) {
        match self {
            Self::OutboxSupervisorTask(source) => error!(
                error = %self, task_id = %source.id(), panicked = source.is_panic(),
                cancelled = source.is_cancelled(), "{message}"
            ),
            _ => error!(error = ?self, "{message}"),
        }
    }
}

enum StopReason {
    Signal(io::Result<()>),
    Requested,
    HttpServer(io::Result<()>),
    OutboxSupervisor(WorkerExit),
}

/// Coordinate signal, shared-token cancellation, or unexpected component exit.
/// Fatal outbox errors cancel the shared token before draining, allowing HTTP
/// to stop admission immediately. Both components finish before resource cleanup;
/// HTTP and database cleanup currently have no process-wide shutdown deadline.
/// Dropping this future requests cancellation but cannot await graceful draining.
pub async fn run<Server, Signal>(
    server: Server,
    worker: JoinHandle<Result<(), WorkerPoolError>>,
    shutdown: CancellationToken,
    signal: Signal,
) -> Result<(), RuntimeError>
where
    Server: Future<Output = io::Result<()>>,
    Signal: Future<Output = io::Result<()>>,
{
    let _cancel_on_exit = shutdown.clone().drop_guard();
    tokio::pin!(server);
    tokio::pin!(signal);
    tokio::pin!(worker);

    let reason = tokio::select! {
        biased;
        result = &mut signal => StopReason::Signal(result),
        _ = shutdown.cancelled() => StopReason::Requested,
        result = &mut server => StopReason::HttpServer(result),
        result = &mut worker => StopReason::OutboxSupervisor(result),
    };
    shutdown.cancel();

    match reason {
        StopReason::Signal(signal_result) => {
            let (server_result, worker_result) = tokio::join!(&mut server, &mut worker);
            prefer(
                signal_result.map_err(RuntimeError::ShutdownSignal),
                prefer(expected_worker_exit(worker_result), expected_server_exit(server_result)),
            )
        }
        StopReason::Requested => {
            let (server_result, worker_result) = tokio::join!(&mut server, &mut worker);
            prefer(expected_worker_exit(worker_result), expected_server_exit(server_result))
        }
        StopReason::HttpServer(server_result) => {
            let primary = match server_result {
                Ok(()) => RuntimeError::HttpServerStopped,
                Err(error) => RuntimeError::HttpServer(error),
            };
            prefer(Err(primary), expected_worker_exit((&mut worker).await))
        }
        StopReason::OutboxSupervisor(worker_result) => prefer(
            unexpected_worker_exit(worker_result),
            expected_server_exit((&mut server).await),
        ),
    }
}

fn prefer(primary: Result<(), RuntimeError>, secondary: Result<(), RuntimeError>) -> Result<(), RuntimeError> {
    match primary {
        Ok(()) => secondary,
        Err(error) => {
            if let Err(secondary) = secondary {
                secondary.log("additional failure during service shutdown");
            }
            Err(error)
        }
    }
}

fn expected_server_exit(result: io::Result<()>) -> Result<(), RuntimeError> {
    result.map_err(RuntimeError::HttpServer)
}

fn expected_worker_exit(result: WorkerExit) -> Result<(), RuntimeError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(RuntimeError::OutboxSupervisor(error)),
        Err(error) => Err(RuntimeError::OutboxSupervisorTask(error)),
    }
}

fn unexpected_worker_exit(result: WorkerExit) -> Result<(), RuntimeError> {
    match result {
        Ok(Ok(())) => Err(RuntimeError::OutboxSupervisorStopped),
        other => expected_worker_exit(other),
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
    async fn signal_drains_http_and_outbox_before_returning() {
        let shutdown = CancellationToken::new();
        let http_shutdown = shutdown.clone();
        let worker_shutdown = shutdown.clone();
        let http_stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = Arc::new(AtomicBool::new(false));
        let server = {
            let stopped = http_stopped.clone();
            async move {
                wait_for_shutdown(http_shutdown, stopped).await;
                Ok(())
            }
        };
        let worker = {
            let stopped = worker_stopped.clone();
            tokio::spawn(async move {
                wait_for_shutdown(worker_shutdown, stopped).await;
                Ok(())
            })
        };

        let result = run(server, worker, shutdown, async { Ok(()) }).await;

        assert!(result.is_ok());
        assert!(http_stopped.load(Ordering::SeqCst));
        assert!(worker_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn signal_failure_still_drains_components() {
        let shutdown = CancellationToken::new();
        let http_shutdown = shutdown.clone();
        let worker_shutdown = shutdown.clone();
        let http_stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = Arc::new(AtomicBool::new(false));
        let server = {
            let stopped = http_stopped.clone();
            async move {
                wait_for_shutdown(http_shutdown, stopped).await;
                Ok(())
            }
        };
        let worker = {
            let stopped = worker_stopped.clone();
            tokio::spawn(async move {
                wait_for_shutdown(worker_shutdown, stopped).await;
                Ok(())
            })
        };
        let signal = async { Err(io::Error::other("signal handler failed")) };

        let result = run(server, worker, shutdown, signal).await;

        assert!(matches!(result, Err(RuntimeError::ShutdownSignal(_))));
        assert!(http_stopped.load(Ordering::SeqCst));
        assert!(worker_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn http_failure_stops_outbox_before_returning_error() {
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let worker_stopped = Arc::new(AtomicBool::new(false));
        let worker = {
            let stopped = worker_stopped.clone();
            tokio::spawn(async move {
                wait_for_shutdown(worker_shutdown, stopped).await;
                Ok(())
            })
        };
        let server = async { Err(io::Error::other("HTTP failed")) };
        let signal = std::future::pending();

        let result = run(server, worker, shutdown, signal).await;

        assert!(matches!(result, Err(RuntimeError::HttpServer(_))));
        assert!(worker_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unexpected_outbox_exit_stops_http_before_returning_error() {
        let shutdown = CancellationToken::new();
        let http_shutdown = shutdown.clone();
        let http_stopped = Arc::new(AtomicBool::new(false));
        let server = {
            let stopped = http_stopped.clone();
            async move {
                wait_for_shutdown(http_shutdown, stopped).await;
                Ok(())
            }
        };
        let worker = tokio::spawn(async { Ok(()) });
        let signal = std::future::pending();

        let result = run(server, worker, shutdown, signal).await;

        assert!(matches!(result, Err(RuntimeError::OutboxSupervisorStopped)));
        assert!(http_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn outbox_panic_stops_http_before_returning_join_error() {
        let shutdown = CancellationToken::new();
        let http_shutdown = shutdown.clone();
        let http_stopped = Arc::new(AtomicBool::new(false));
        let server = {
            let stopped = http_stopped.clone();
            async move {
                wait_for_shutdown(http_shutdown, stopped).await;
                Ok(())
            }
        };
        let worker = tokio::spawn(async {
            panic!("outbox panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        let signal = std::future::pending();

        let result = run(server, worker, shutdown, signal).await;

        assert!(matches!(result, Err(RuntimeError::OutboxSupervisorTask(_))));
        assert!(http_stopped.load(Ordering::SeqCst));
    }
    fn outbox_error() -> WorkerPoolError {
        WorkerPoolError::HandlerFormatsMismatch { worker_index: 7 }
    }

    #[tokio::test]
    async fn shared_cancellation_stops_http_before_outbox_drain_finishes() {
        let shutdown = CancellationToken::new();
        let (fail, failure) = tokio::sync::oneshot::channel::<()>();
        let (release, drain) = tokio::sync::oneshot::channel::<()>();
        let (http_started, http_shutdown_started) = tokio::sync::oneshot::channel();
        let worker_shutdown = shutdown.clone();
        let worker = tokio::spawn(async move {
            failure.await.unwrap();
            worker_shutdown.cancel();
            drain.await.unwrap();
            Err(outbox_error())
        });
        let http_shutdown = shutdown.clone();
        let server = async move {
            http_shutdown.cancelled().await;
            http_started.send(()).unwrap();
            Ok(())
        };
        let runtime = tokio::spawn(run(server, worker, shutdown, std::future::pending()));
        fail.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), http_shutdown_started)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !runtime.is_finished(),
            "HTTP must observe cancellation while outbox still drains"
        );
        release.send(()).unwrap();
        assert!(matches!(
            runtime.await.unwrap(),
            Err(RuntimeError::OutboxSupervisor(WorkerPoolError::HandlerFormatsMismatch {
                worker_index: 7
            }))
        ));
    }

    #[tokio::test]
    async fn cancellation_and_signal_win_over_simultaneous_successful_component_exits() {
        for signal_ready in [false, true] {
            let shutdown = CancellationToken::new();
            if !signal_ready {
                shutdown.cancel();
            }
            let worker = tokio::spawn(async { Ok(()) });
            while !worker.is_finished() {
                tokio::task::yield_now().await;
            }
            let signal = async move { if signal_ready { Ok(()) } else { std::future::pending().await } };
            assert!(run(async { Ok(()) }, worker, shutdown, signal).await.is_ok());
        }
    }

    #[tokio::test]
    async fn clean_http_exit_without_cancellation_is_unexpected() {
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let worker = tokio::spawn(async move {
            worker_shutdown.cancelled().await;
            Ok(())
        });
        assert!(matches!(
            run(async { Ok(()) }, worker, shutdown, std::future::pending()).await,
            Err(RuntimeError::HttpServerStopped)
        ));
    }

    #[tokio::test]
    async fn dropping_runtime_requests_shutdown_for_both_components() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let (stopped, worker_stopped) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            worker_shutdown.cancelled().await;
            stopped.send(()).unwrap();
            Ok(())
        });
        let mut runtime = Box::pin(run(std::future::pending(), worker, shutdown.clone(), std::future::pending()));
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
        for initiator in ["signal_failure", "signal", "token", "http", "worker"] {
            let writer = RecordingWriter::default();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(writer.clone())
                .with_ansi(false)
                .without_time()
                .finish();
            let shutdown = CancellationToken::new();
            if initiator == "token" {
                shutdown.cancel();
            }
            let http_shutdown = shutdown.clone();
            let server = async move {
                if initiator != "http" {
                    http_shutdown.cancelled().await;
                }
                Err(io::Error::other("injected HTTP failure"))
            };
            let worker_shutdown = shutdown.clone();
            let worker = tokio::spawn(async move {
                if initiator == "http" {
                    worker_shutdown.cancelled().await;
                }
                Err(outbox_error())
            });
            let signal = async move {
                match initiator {
                    "signal_failure" => Err(io::Error::other("injected signal failure")),
                    "signal" => Ok(()),
                    _ => std::future::pending().await,
                }
            };
            let result = run(server, worker, shutdown, signal).with_subscriber(subscriber).await;
            match initiator {
                "signal_failure" => assert!(matches!(result, Err(RuntimeError::ShutdownSignal(_)))),
                "http" => assert!(matches!(result, Err(RuntimeError::HttpServer(_)))),
                _ => assert!(matches!(result, Err(RuntimeError::OutboxSupervisor(_)))),
            }
            let log = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
            assert_eq!(
                log.matches("additional failure during service shutdown").count(),
                if initiator == "signal_failure" { 2 } else { 1 }
            );
            if initiator == "http" || initiator == "signal_failure" {
                assert!(log.contains("OutboxSupervisor"));
            }
            if initiator != "http" {
                assert!(log.contains("HttpServer"));
            }
        }
    }

    #[tokio::test]
    async fn supervisor_task_diagnostics_and_debug_redact_panic_payloads() {
        let writer = RecordingWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let panic = tokio::spawn(async {
            panic!("private_panic_payload_sentinel");
        })
        .await
        .unwrap_err();
        let task_id = panic.id().to_string();
        let error = RuntimeError::OutboxSupervisorTask(panic);
        tracing::subscriber::with_default(subscriber, || error.log("service stopped with an error"));
        let log = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(log.contains(&task_id));
        assert!(log.contains("panicked=true cancelled=false"));
        assert!(!log.contains("private_panic_payload_sentinel"));
        let debug = format!("{error:?}");
        assert!(debug.contains("panicked: true"));
        assert!(!debug.contains("private_panic_payload_sentinel"));
        // Keep the original error as a source without rendering its payload.
        assert!(std::error::Error::source(&error).is_some());
        let task = tokio::spawn(std::future::pending::<()>());
        task.abort();
        let error = RuntimeError::OutboxSupervisorTask(task.await.unwrap_err());
        assert!(format!("{error:?}").contains("cancelled: true"));
    }
    #[tokio::test]
    async fn standard_axum_shutdown_stops_tcp_admission_and_preserves_active_request() {
        use axum::{Router, routing::get};
        use std::time::Duration;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let handler_entered = entered.clone();
        let handler_release = release.clone();
        let router = Router::new().route(
            "/",
            get(move || {
                let entered = handler_entered.clone();
                let release = handler_release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    "completed"
                }
            }),
        );
        let shutdown = CancellationToken::new();
        let host_shutdown = shutdown.clone();
        let cleanup_started = Arc::new(AtomicBool::new(false));
        let cleanup = cleanup_started.clone();
        let token = shutdown.clone();
        let worker_token = token.clone();
        let worker = tokio::spawn(async move {
            worker_token.cancelled().await;
            Ok(())
        });
        let mut host = tokio::spawn(async move {
            let server = async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(token.cancelled_owned())
                    .await
            };
            let result = super::run(server, worker, host_shutdown, std::future::pending()).await;
            cleanup.store(true, Ordering::SeqCst);
            result
        });
        let mut connection = TcpStream::connect(address).await.unwrap();
        connection
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match TcpStream::connect(address).await {
                    Err(_) => break,
                    Ok(stream) => drop(stream),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut host).await.is_err());
        assert!(!cleanup_started.load(Ordering::SeqCst));
        release.notify_one();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(2), connection.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.ends_with("completed"));
        assert!(
            tokio::time::timeout(Duration::from_secs(2), host)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        assert!(cleanup_started.load(Ordering::SeqCst));
    }
}
