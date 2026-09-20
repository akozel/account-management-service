use std::sync::Arc;

use account_management_service::{
    application::{
        email_reservation::service::email_reservation_service,
        email_reservation::tasks::send_verification_code::send_verification_code_task_handler,
        outbox::{TaskHandlerRegistry, outbox_task_coordinator},
        user_account::{
            service::registration_service,
            tasks::{create_account::create_account_task_handler, record_account_created::record_account_created_task_handler},
        },
    },
    infrastructure::{
        console_verification_code_sender::ConsoleVerificationCodeSender,
        cqrs::{
            AggregateConflictRetryPolicy, email_reservation_gateway::CqrsEmailReservationGateway,
            user_account_gateway::CqrsUserAccountGateway,
        },
        postgres::event_repository_with_outbox::PostgresEventRepositoryWithOutbox,
        postgres::outbox_queue::PostgresOutboxQueue,
        registration_material_generator::RandomRegistrationMaterialGenerator,
        system_registration_clock::SystemRegistrationClock,
    },
    presentation,
};
use cqrs_es::{CqrsFramework, persist::PersistedEventStore};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let bind_address = std::env::var("BIND_ADDRESS").expect("BIND_ADDRESS must be set");

    let pool = postgres_es::default_postgress_pool(&database_url).await;
    let aggregate_conflict_retry = AggregateConflictRetryPolicy::default();
    let command_pool = pool.clone();
    let gateway = Arc::new(CqrsEmailReservationGateway::new(
        move |tasks| {
            let repository = PostgresEventRepositoryWithOutbox::new(command_pool.clone()).with_outbox(tasks);
            let store = PersistedEventStore::new_snapshot_store(repository, 25);
            CqrsFramework::new(store, Vec::new(), ())
        },
        aggregate_conflict_retry,
    ));
    let queue = Arc::new(PostgresOutboxQueue::new(pool.clone()));
    let account_pool = pool.clone();
    let account_gateway = Arc::new(CqrsUserAccountGateway::new(
        move |tasks| {
            let repository = PostgresEventRepositoryWithOutbox::new(account_pool.clone()).with_outbox(tasks);
            let store = PersistedEventStore::new_snapshot_store(repository, 25);
            CqrsFramework::new(store, Vec::new(), ())
        },
        aggregate_conflict_retry,
    ));
    let registration = registration_service(gateway.clone(), account_gateway, Arc::new(SystemRegistrationClock));
    let http_registration = registration.clone();
    let handler_factory: presentation::outbox_worker_pool::TaskHandlerFactory = Arc::new(move || {
        TaskHandlerRegistry::try_new(vec![
            send_verification_code_task_handler(Box::new(ConsoleVerificationCodeSender::new(std::io::stdout()))),
            create_account_task_handler(registration.clone()),
            record_account_created_task_handler(registration.clone()),
        ])
        .expect("valid outbox handler registry")
    });
    let outbox = outbox_task_coordinator(queue, handler_factory().formats());
    let worker_config = presentation::outbox_worker_pool::OutboxWorkerPoolConfig::from_env().expect("valid worker configuration");
    let email_reservation = email_reservation_service(gateway, Arc::new(RandomRegistrationMaterialGenerator));
    let app = presentation::http::router(email_reservation, http_registration);
    let listener = TcpListener::bind(&bind_address).await.expect("unable to bind HTTP listener");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut worker = tokio::spawn(presentation::outbox_worker_pool::run(
        outbox,
        handler_factory,
        worker_config,
        shutdown_rx,
    ));
    let signal_tx = shutdown_tx.clone();
    let signal = tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("install SIGTERM handler");
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await.expect("install shutdown handler");
        let _ = signal_tx.send(true);
    });
    let mut http_shutdown = shutdown_tx.subscribe();
    let serve = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                if !*http_shutdown.borrow() {
                    let _ = http_shutdown.changed().await;
                }
            })
            .await
    };
    tokio::pin!(serve);
    let server = tokio::select! {
        result = &mut serve => {
            let _ = shutdown_tx.send(true);
            worker.await.expect("outbox supervisor terminated").expect("outbox worker failed");
            result
        }
        result = &mut worker => {
            if !*shutdown_tx.borrow() {
                let _ = shutdown_tx.send(true);
                signal.abort();
                panic!("outbox supervisor terminated unexpectedly: {result:?}");
            }
            result.expect("outbox supervisor terminated").expect("outbox worker failed");
            serve.await
        }
    };
    signal.abort();
    pool.close().await;
    server.expect("HTTP server terminated unexpectedly");
}
