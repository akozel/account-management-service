use crate::application::email_reservation::service::EmailReservationService;
use crate::application::user_account::service::RegistrationService;
use axum::Router;
use std::sync::Arc;
mod email_reservations;
mod user_accounts;

pub fn router(email_reservation: Arc<dyn EmailReservationService>, registration: Arc<dyn RegistrationService>) -> Router {
    Router::new()
        .merge(email_reservations::router(email_reservation))
        .merge(user_accounts::router(registration))
}

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ErrorResponse {
    code: &'static str,
    message: String,
}

pub(super) fn error_response(status: StatusCode, code: &'static str, message: String) -> Response {
    (status, Json(ErrorResponse { code, message })).into_response()
}
