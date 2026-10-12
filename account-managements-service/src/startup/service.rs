//! Prepared service owning PostgreSQL, HTTP and the task runtime.
use std::{future::Future, io};

use axum::Router;
use cqrs_runtime::{
    host::{lifecycle::run, signal::shutdown_signal},
    postgres::{pool::connect_pool, task_runtime_factory::prepare_task_runtime},
    runtime::preparation::TaskRuntime,
};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{
    error::ServiceError,
    wiring::{self, ApplicationParts},
};
use crate::config::ServiceConfig;

/// Preparation retains all handlers; run consumes the service exactly once.
/// Dropping an unstarted service releases its listener and handlers. Asynchronous
/// pool closure is guaranteed on normal preparation failure and awaited run,
/// not by Drop or by aborting a running future.
pub(crate) struct Service {
    pool: PgPool,
    listener: TcpListener,
    router: Router,
    tasks: TaskRuntime,
}

impl Service {
    /// Install the checked logger, connect PostgreSQL and prepare all components.
    /// Workers and HTTP admission start only when run is polled.
    pub(crate) async fn prepare(config: ServiceConfig) -> Result<Self, ServiceError> {
        let result = async {
            config.logging().try_init()?;
            let pool = connect_pool(config.database().clone()).await?;
            let application = wiring::build(&pool);
            Self::prepare_connected(config, pool, application).await
        }
        .await;
        if let Err(error) = &result {
            error.log();
        }
        result
    }

    async fn prepare_connected(config: ServiceConfig, pool: PgPool, application: ApplicationParts) -> Result<Self, ServiceError> {
        let result = async {
            // All fallible handler construction precedes opening the listener.
            let tasks = prepare_task_runtime(
                pool.clone(),
                application.dispatcher_factory,
                config.task_queue().clone(),
                config.task_runtime().clone(),
            )?;
            let listener = TcpListener::bind(config.bind_address()).await?;
            info!(address = %listener.local_addr()?, "HTTP server is listening");
            Ok(Self {
                pool: pool.clone(),
                listener,
                router: application.router,
                tasks,
            })
        }
        .await;
        if result.is_err() {
            pool.close().await;
        }
        result
    }

    /// Await coordinated HTTP/task shutdown, then close PostgreSQL and report.
    pub(crate) async fn run(self) -> Result<(), ServiceError> {
        self.run_until(shutdown_signal()).await
    }

    async fn run_until(self, signal: impl Future<Output = io::Result<()>>) -> Result<(), ServiceError> {
        let Self {
            pool,
            listener,
            router,
            tasks,
        } = self;
        let shutdown = CancellationToken::new();
        let http_shutdown = shutdown.clone();
        let http = async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(http_shutdown.cancelled_owned())
                .await
        };
        let result = run(http, tasks.run(shutdown.clone()), shutdown, signal)
            .await
            .map_err(ServiceError::from);
        // Preserve the original result and report only after resource cleanup.
        pool.close().await;
        match &result {
            Ok(()) => info!("service stopped"),
            Err(error) => error.log(),
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::startup::test_support::{self, Database};
    use axum::routing::get;
    use cqrs_runtime::{
        host::error::HostError,
        runtime::dispatcher_factory::WorkerSetupError,
        task::handling::dispatcher::{TaskDispatcher, TaskDispatcherError},
    };
    use std::{net::SocketAddr, sync::Arc, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::{Notify, oneshot},
    };

    fn config(url: &str, address: SocketAddr) -> ServiceConfig {
        ServiceConfig::from_lookup(|key| match key {
            "DATABASE_URL" => Some(url.to_owned()),
            "BIND_ADDRESS" => Some(address.to_string()),
            "OUTBOX_WORKER_CONCURRENCY" | "OUTBOX_QUEUE_CAPACITY" => Some("1".to_owned()),
            _ => None,
        })
        .unwrap()
    }

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap()
    }

    #[tokio::test]
    async fn preparation_error_closes_pool_and_precedes_listener_binding() {
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = lazy_pool();
        let observer = pool.clone();
        let application = ApplicationParts {
            router: Router::new(),
            dispatcher_factory: Arc::new(|| TaskDispatcher::try_new(vec![])),
        };
        let error = Service::prepare_connected(
            config("postgres://localhost/unused", occupied.local_addr().unwrap()),
            pool,
            application,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(
            error,
            ServiceError::TaskPreparation(WorkerSetupError::Build {
                worker_index: 0,
                source: TaskDispatcherError::Empty,
            })
        ));
        assert!(observer.is_closed());
    }

    #[tokio::test]
    async fn binding_error_closes_pool_after_preparing_real_application() {
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = lazy_pool();
        let observer = pool.clone();
        let application = wiring::build(&pool);
        let error = Service::prepare_connected(
            config("postgres://localhost/unused", occupied.local_addr().unwrap()),
            pool,
            application,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(error, ServiceError::Bind(ref source) if source.kind() == io::ErrorKind::AddrInUse));
        assert!(observer.is_closed());
    }

    #[tokio::test]
    async fn public_preparation_initializes_logger_and_retains_owned_resources() {
        if test_support::scenario().is_some() {
            let url = std::env::var("ACCOUNT_SERVICE_TEST_DATABASE_URL").unwrap();
            let address = "127.0.0.1:0".parse().unwrap();
            let service = Service::prepare(config(&url, address)).await.unwrap();
            let observer = service.pool.clone();
            let address = service.listener.local_addr().unwrap();
            assert_ne!(address.port(), 0);
            assert!(!observer.is_closed());
            assert!(matches!(
                Service::prepare(config(&url, address)).await.err().unwrap(),
                ServiceError::Logging(_),
            ));
            assert!(
                !observer.is_closed(),
                "a second startup cannot close the first service's pool"
            );
            service.run_until(std::future::ready(Ok(()))).await.unwrap();
            assert!(observer.is_closed());
            assert!(
                TcpListener::bind(address).await.is_ok(),
                "the stopped service releases its listener"
            );
            return;
        }
        let database = Database::new().await;
        test_support::isolated(
            concat!(
                module_path!(),
                "::public_preparation_initializes_logger_and_retains_owned_resources"
            ),
            "prepare",
            |child| {
                child.env("ACCOUNT_SERVICE_TEST_DATABASE_URL", &database.url);
            },
        );
        database.close().await;
    }

    #[tokio::test]
    async fn host_failure_closes_pool_before_returning_original_error() {
        let pool = lazy_pool();
        let observer = pool.clone();
        let application = wiring::build(&pool);
        let service = Service::prepare_connected(
            config("postgres://localhost/unused", "127.0.0.1:0".parse().unwrap()),
            pool,
            application,
        )
        .await
        .unwrap();
        let error = service
            .run_until(std::future::ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private_signal_source",
            ))))
            .await
            .unwrap_err();
        assert!(observer.is_closed());
        assert!(matches!(error, ServiceError::Host(HostError::ShutdownSignal(ref source))
            if source.kind() == io::ErrorKind::PermissionDenied));
        assert!(!format!("{error:?} {error}").contains("private_"));
    }

    #[tokio::test]
    async fn service_stops_admission_and_drains_http_before_closing_database() {
        let database = Database::new().await;
        let observer = database.pool.clone();
        let mut service = Service::prepare_connected(
            config(&database.url, "127.0.0.1:0".parse().unwrap()),
            database.pool.clone(),
            wiring::build(&database.pool),
        )
        .await
        .unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        service.router = service.router.route(
            "/probe",
            get({
                let entered = entered.clone();
                let release = release.clone();
                move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        "completed"
                    }
                }
            }),
        );
        let address = service.listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let mut running = tokio::spawn(service.run_until(async {
            stopped.await.unwrap();
            Ok(())
        }));
        let mut connection = TcpStream::connect(address).await.unwrap();
        connection
            .write_all(b"GET /probe HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Ok(connection) = TcpStream::connect(address).await {
                drop(connection);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut running).await.is_err());
        assert!(
            !observer.is_closed(),
            "database remains available until the active request finishes"
        );
        release.notify_one();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(2), connection.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.ends_with("completed"));
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(observer.is_closed());
        database.close().await;
    }
}
