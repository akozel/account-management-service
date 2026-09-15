use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

use super::{EmailReservationsError, EmailReservationsState};
use crate::domain::{Email, UserAccountId};

#[derive(Deserialize)]
pub(super) struct ReserveEmailRequest {
    email: String,
}

#[derive(Serialize)]
pub(super) struct ReserveEmailResponse {
    email: Email,
    account_id: UserAccountId,
}

pub(super) async fn reserve(
    State(state): State<EmailReservationsState>,
    payload: Result<Json<ReserveEmailRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ReserveEmailResponse>), EmailReservationsError> {
    let Json(request) = payload?;
    let reserved = state.service.reserve(Email::parse(&request.email)?).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ReserveEmailResponse {
            email: reserved.email,
            account_id: reserved.account_id,
        }),
    ))
}
