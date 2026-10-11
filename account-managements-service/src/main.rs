mod config;
mod startup;

use std::sync::Arc;

use account_managements_service::{
    application::{
        email_reservation::service::email_reservation_service,
        email_reservation::tasks::send_verification_code::send_verification_code_task_adapter,
        user_account::{
            service::registration_service,
            tasks::{create_account::create_account_task_adapter, record_account_created::record_account_created_task_adapter},
        },
    },
    domain::{EmailReservation, UserAccount},
    infrastructure::{
        console::ConsoleVerificationCodeSender,
        cqrs::{email_reservation_gateway::CqrsEmailReservationGateway, user_account_gateway::CqrsUserAccountGateway},
        random::RandomRegistrationMaterialGenerator,
        time::SystemRegistrationClock,
    },
    presentation,
};
use config::ServiceConfig;
use cqrs_runtime::{
    host,
    postgres::{
        command_factory::{PostgresCommandConfig, command_executor},
        pool::connect_pool,
        task_runtime_factory::prepare_task_runtime,
    },
    runtime,
    task::handling::dispatcher::TaskDispatcher,
};
use startup::{ServiceError, load_dotenv, with_pool_cleanup};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), ServiceError> {
    let result = async {
        load_dotenv()?;
        let config = ServiceConfig::from_env()?;
        config.logging().try_init()?;
        let pool = connect_pool(config.database().clone()).await?;
        with_pool_cleanup(&pool, async {
            let command_config = PostgresCommandConfig::default();
            let gateway = Arc::new(CqrsEmailReservationGateway::new(command_executor::<EmailReservation>(
                pool.clone(),
                command_config.clone(),
            )));
            let account_gateway = Arc::new(CqrsUserAccountGateway::new(command_executor::<UserAccount>(
                pool.clone(),
                command_config,
            )));
            let registration = registration_service(gateway.clone(), account_gateway, Arc::new(SystemRegistrationClock));
            let http_registration = registration.clone();
            let dispatcher_factory: runtime::dispatcher_factory::TaskDispatcherFactory = Arc::new(move || {
                TaskDispatcher::try_new(vec![
                    send_verification_code_task_adapter(Box::new(ConsoleVerificationCodeSender::new(std::io::stdout())))?,
                    create_account_task_adapter(registration.clone())?,
                    record_account_created_task_adapter(registration.clone())?,
                ])
            });
            let task_runtime = prepare_task_runtime(
                pool.clone(),
                dispatcher_factory,
                config.task_queue().clone(),
                config.task_runtime().clone(),
            )?;
            let email_reservation = email_reservation_service(gateway, Arc::new(RandomRegistrationMaterialGenerator));
            let app = presentation::http::router(email_reservation, http_registration);
            let listener = TcpListener::bind(config.bind_address()).await?;
            info!(address = %listener.local_addr()?, "HTTP server is listening");

            let shutdown = CancellationToken::new();
            let http_shutdown = shutdown.clone();
            let serve = async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(http_shutdown.cancelled_owned())
                    .await
            };
            host::lifecycle::run(
                serve,
                task_runtime.run(shutdown.clone()),
                shutdown,
                host::signal::shutdown_signal(),
            )
            .await?;
            Ok(())
        })
        .await
    }
    .await;
    match &result {
        Ok(()) => info!("service stopped"),
        Err(error) => error.log(),
    }
    result
}
