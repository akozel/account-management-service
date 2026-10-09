use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use serde::Serialize;

use super::{EmailReservationsError, EmailReservationsState};
use crate::domain::Email;

#[derive(Serialize)]
pub(super) struct RequestCodeResponse {
    status: &'static str,
}

pub(super) async fn request_code(
    State(state): State<EmailReservationsState>,
    email: Result<Path<String>, PathRejection>,
) -> Result<(StatusCode, Json<RequestCodeResponse>), EmailReservationsError> {
    let Path(email) = email?;
    state.service.request_new_verification_code(Email::parse(&email)?).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(RequestCodeResponse {
            status: "verification_code_requested",
        }),
    ))
}
