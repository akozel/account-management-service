use std::io;

/// Observe SIGINT or SIGTERM on Unix, and Ctrl-C on other platforms.
///
/// Failure to install/observe a signal handler is returned to the host. A closed
/// Unix SIGTERM stream is an error rather than a successful stop request.
#[cfg(unix)]
pub async fn shutdown_signal() -> io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    wait_for_signal(tokio::signal::ctrl_c(), terminate.recv()).await
}

#[cfg(not(unix))]
pub async fn shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(unix)]
async fn wait_for_signal(
    interrupt: impl std::future::Future<Output = io::Result<()>>,
    terminate: impl std::future::Future<Output = Option<()>>,
) -> io::Result<()> {
    tokio::select! {
        result = interrupt => result,
        signal = terminate => signal
            .ok_or_else(|| io::Error::other("SIGTERM signal stream closed")),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::future::{pending, ready};

    #[tokio::test]
    async fn interrupt_success() {
        assert!(wait_for_signal(ready(Ok(())), pending()).await.is_ok());
    }

    #[tokio::test]
    async fn interrupt_error_preserves_source() {
        let result = wait_for_signal(
            ready(Err(io::Error::new(io::ErrorKind::PermissionDenied, "interrupt failed"))),
            pending(),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn terminate_success() {
        assert!(wait_for_signal(pending(), ready(Some(()))).await.is_ok());
    }

    #[tokio::test]
    async fn terminate_closed_stream_is_error() {
        let error = wait_for_signal(pending(), ready(None)).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "SIGTERM signal stream closed");
    }
}
