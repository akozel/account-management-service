//! Coordinates graceful shutdown across the service's inbound adapters.

use std::{future::Future, io};

use thiserror::Error;
use tokio::{sync::watch, task::JoinError};

use account_management_service::presentation::outbox_worker_pool::OutboxWorkerPoolConfigError;

type WorkerExit = Result<Result<(), OutboxWorkerPoolConfigError>, JoinError>;

#[derive(Debug, Error)]
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
    OutboxSupervisor(#[source] OutboxWorkerPoolConfigError),
    #[error("outbox supervisor terminated before shutdown")]
    OutboxSupervisorStopped,
}

enum StopReason {
    Signal(io::Result<()>),
    HttpServer(io::Result<()>),
    OutboxSupervisor(WorkerExit),
}

/// Runs the HTTP server and outbox supervisor until either a shutdown signal
/// arrives or one of those critical components terminates.
///
/// Once shutdown starts, both components are given the opportunity to drain
/// before the function returns to the composition root for resource cleanup.
pub async fn run<Server, Signal>(
    server: Server,
    worker: tokio::task::JoinHandle<Result<(), OutboxWorkerPoolConfigError>>,
    shutdown: watch::Sender<bool>,
    signal: Signal,
) -> Result<(), RuntimeError>
where
    Server: Future<Output = io::Result<()>>,
    Signal: Future<Output = io::Result<()>>,
{
    tokio::pin!(server);
    tokio::pin!(signal);
    tokio::pin!(worker);

    let reason = tokio::select! {
        result = &mut signal => StopReason::Signal(result),
        result = &mut server => StopReason::HttpServer(result),
        result = &mut worker => StopReason::OutboxSupervisor(result),
    };
    let _ = shutdown.send(true);

    match reason {
        StopReason::Signal(signal_result) => {
            let (server_result, worker_result) = tokio::join!(&mut server, &mut worker);
            signal_result.map_err(RuntimeError::ShutdownSignal)?;
            expected_server_exit(server_result)?;
            expected_worker_exit(worker_result)
        }
        StopReason::HttpServer(server_result) => {
            let _worker_result = (&mut worker).await;
            match server_result {
                Ok(()) => Err(RuntimeError::HttpServerStopped),
                Err(error) => Err(RuntimeError::HttpServer(error)),
            }
        }
        StopReason::OutboxSupervisor(worker_result) => {
            let _server_result = (&mut server).await;
            unexpected_worker_exit(worker_result)
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

    async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>, stopped: Arc<AtomicBool>) {
        if !*shutdown.borrow() {
            shutdown.changed().await.unwrap();
        }
        stopped.store(true, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn signal_drains_http_and_outbox_before_returning() {
        let (shutdown, http_shutdown) = watch::channel(false);
        let worker_shutdown = shutdown.subscribe();
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
        let (shutdown, http_shutdown) = watch::channel(false);
        let worker_shutdown = shutdown.subscribe();
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
        let (shutdown, worker_shutdown) = watch::channel(false);
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
        let (shutdown, http_shutdown) = watch::channel(false);
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
        let (shutdown, http_shutdown) = watch::channel(false);
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
}
