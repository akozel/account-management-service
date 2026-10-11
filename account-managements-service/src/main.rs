mod runtime;

use std::sync::Arc;

use account_managements_service::{
    application::{
        email_reservation::service::email_reservation_service,
        email_reservation::tasks::send_verification_code::send_verification_code_task_handler,
        user_account::{
            service::registration_service,
            tasks::{create_account::create_account_task_handler, record_account_created::record_account_created_task_handler},
        },
    },
    domain::{EmailReservation, UserAccount},
    infrastructure::{
        console::ConsoleVerificationCodeSender,
        cqrs::{email_reservation_gateway::CqrsEmailReservationGateway, user_account_gateway::CqrsUserAccountGateway},
        logging::LogingConfig,
        random::RandomRegistrationMaterialGenerator,
        time::SystemRegistrationClock,
    },
    presentation,
};
use cqrs_runtime::{
    outbox::{OutboxTaskCoordinatorConfig, TaskHandlerRegistry, outbox_task_coordinator_with_config},
    postgres::{PostgresCommandConfig, PostgresOutboxQueue, PostgresOutboxQueueConfig, command_executor},
    worker,
};
use runtime::RuntimeError;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    dotenvy::dotenv().ok();
    let level_filter = LogingConfig::level_filter_from_env().expect("RUST_LOG must be a valid tracing level");
    LogingConfig::default_setup(level_filter);
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let bind_address = std::env::var("BIND_ADDRESS").expect("BIND_ADDRESS must be set");
    let coordinator_config = OutboxTaskCoordinatorConfig::from_env().expect("valid outbox storage timeout");
    let postgres_queue_config = PostgresOutboxQueueConfig::from_env().expect("valid PostgreSQL outbox timeout");

    let pool = postgres_es::default_postgress_pool(&database_url).await;
    let command_config = PostgresCommandConfig::default();
    let gateway = Arc::new(CqrsEmailReservationGateway::new(command_executor::<EmailReservation>(
        pool.clone(),
        command_config.clone(),
    )));
    let queue = Arc::new(
        PostgresOutboxQueue::with_config(pool.clone(), postgres_queue_config).expect("valid PostgreSQL outbox configuration"),
    );
    let account_gateway = Arc::new(CqrsUserAccountGateway::new(command_executor::<UserAccount>(
        pool.clone(),
        command_config,
    )));
    let registration = registration_service(gateway.clone(), account_gateway, Arc::new(SystemRegistrationClock));
    let http_registration = registration.clone();
    let handler_factory: worker::HandlerRegistryFactory = Arc::new(move || {
        TaskHandlerRegistry::try_new(vec![
            send_verification_code_task_handler(Box::new(ConsoleVerificationCodeSender::new(std::io::stdout())))?,
            create_account_task_handler(registration.clone())?,
            record_account_created_task_handler(registration.clone())?,
        ])
    });
    let outbox = outbox_task_coordinator_with_config(
        queue,
        handler_factory().expect("valid handler registry").formats(),
        coordinator_config,
    )
    .expect("valid outbox coordinator configuration");
    let worker_config = worker::OutboxWorkerPoolConfig::from_env().expect("valid worker configuration");
    worker_config
        .validate_for_storage_timeout(outbox.storage_timeout())
        .expect("outbox lease must cover claim, handler, settlement, and safety margin");
    let email_reservation = email_reservation_service(gateway, Arc::new(RandomRegistrationMaterialGenerator));
    let app = presentation::http::router(email_reservation, http_registration);
    let listener = TcpListener::bind(&bind_address).await.expect("unable to bind HTTP listener");
    info!(address = %bind_address, "HTTP server is listening");

    let shutdown = CancellationToken::new();
    let worker = tokio::spawn(worker::run(outbox, handler_factory, worker_config, shutdown.clone()));
    let http_shutdown = shutdown.clone();
    let serve = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(http_shutdown.cancelled_owned())
            .await
    };
    let runtime_result = runtime::run(serve, worker, shutdown, shutdown_signal()).await;
    pool.close().await;
    match runtime_result {
        Ok(()) => {
            info!("service stopped");
            Ok(())
        }
        Err(error) => {
            error.log("service stopped with an error");
            Err(error)
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        signal = terminate.recv() => signal
            .map(|_| ())
            .ok_or_else(|| std::io::Error::other("SIGTERM signal stream closed")),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}
