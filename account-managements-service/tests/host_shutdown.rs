use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;

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
    let worker = async move {
        worker_token.cancelled().await;
        Ok(())
    };
    let mut host = tokio::spawn(async move {
        let server = async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(token.cancelled_owned())
                .await
        };
        let result = cqrs_runtime::host::run(server, worker, host_shutdown, std::future::pending()).await;
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
