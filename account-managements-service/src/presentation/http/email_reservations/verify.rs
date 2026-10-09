use axum::{
    Json,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::header,
};
use serde::{Deserialize, Serialize};

use super::{EmailReservationsError, EmailReservationsState};
use crate::domain::{Email, UserAccountId};

#[derive(Deserialize)]
pub(super) struct VerifyEmailRequest {
    account_id: String,
    code: u32,
}

#[derive(Serialize)]
pub(super) struct VerifyEmailResponse {
    email: Email,
    account_id: UserAccountId,
    account_creation_token: String,
}

pub(super) async fn verify(
    State(state): State<EmailReservationsState>,
    email: Result<Path<String>, PathRejection>,
    payload: Result<Json<VerifyEmailRequest>, JsonRejection>,
) -> Result<([(axum::http::HeaderName, &'static str); 1], Json<VerifyEmailResponse>), EmailReservationsError> {
    let Path(email) = email?;
    let Json(request) = payload?;
    let account_id: UserAccountId = request.account_id.parse()?;
    let verified = state
        .service
        .confirm_email(Email::parse(&email)?, account_id, request.code)
        .await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(VerifyEmailResponse {
            email: verified.email,
            account_id: verified.account_id,
            account_creation_token: verified.account_creation_token,
        }),
    ))
}
