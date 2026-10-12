//! Business services, HTTP routes and fresh task handlers for this executable.
use std::sync::Arc;

use account_managements_service::{
    application::{
        email_reservation::{
            service::email_reservation_service, tasks::send_verification_code::send_verification_code_task_adapter,
        },
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
use axum::Router;
use cqrs_runtime::{
    postgres::command_factory::{PostgresCommandConfig, command_executor},
    runtime::dispatcher_factory::TaskDispatcherFactory,
    task::handling::dispatcher::TaskDispatcher,
};
use sqlx::PgPool;

pub(super) struct ApplicationParts {
    pub(super) router: Router,
    pub(super) dispatcher_factory: TaskDispatcherFactory,
}

/// Assemble dependencies without database operations or background tasks.
pub(super) fn build(pool: &PgPool) -> ApplicationParts {
    let command_config = PostgresCommandConfig::default();
    let email_gateway = Arc::new(CqrsEmailReservationGateway::new(command_executor::<EmailReservation>(
        pool.clone(),
        command_config.clone(),
    )));
    let account_gateway = Arc::new(CqrsUserAccountGateway::new(command_executor::<UserAccount>(
        pool.clone(),
        command_config,
    )));
    let registration = registration_service(email_gateway.clone(), account_gateway, Arc::new(SystemRegistrationClock));
    let http_registration = registration.clone();
    let dispatcher_factory = Arc::new(move || {
        TaskDispatcher::try_new(vec![
            send_verification_code_task_adapter(Box::new(ConsoleVerificationCodeSender::new(std::io::stdout())))?,
            create_account_task_adapter(registration.clone())?,
            record_account_created_task_adapter(registration.clone())?,
        ])
    });
    let email_reservation = email_reservation_service(email_gateway, Arc::new(RandomRegistrationMaterialGenerator));
    ApplicationParts {
        router: presentation::http::router(email_reservation, http_registration),
        dispatcher_factory,
    }
}
