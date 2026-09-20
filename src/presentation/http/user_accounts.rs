use std::sync::Arc;

use axum::{
    Router,
    extract::rejection::JsonRejection,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};

use crate::{
    application::user_account::service::{RegistrationService, SubmissionError},
    domain::{EmailReservationError, InvalidEmail},
};

use super::error_response;

mod submit_profile;

#[derive(Clone)]
pub(super) struct UserAccountsState {
    service: Arc<dyn RegistrationService>,
}

pub(super) fn router(service: Arc<dyn RegistrationService>) -> Router {
    Router::new()
        .route("/user-accounts", post(submit_profile::submit_profile))
        .with_state(UserAccountsState { service })
}

#[derive(Debug)]
pub(super) enum UserAccountsError {
    InvalidRequest,
    InvalidEmail,
    InvalidProfile,
    InvalidAccountCreationToken,
    RegistrationNotFound,
    RegistrationNotCurrent,
    EmailNotVerified,
    ProfileAlreadySubmitted,
    Conflict,
    Unavailable,
    Internal,
}

impl From<JsonRejection> for UserAccountsError {
    fn from(_: JsonRejection) -> Self {
        Self::InvalidRequest
    }
}

impl From<uuid::Error> for UserAccountsError {
    fn from(_: uuid::Error) -> Self {
        Self::InvalidRequest
    }
}

impl From<chrono::ParseError> for UserAccountsError {
    fn from(_: chrono::ParseError) -> Self {
        Self::InvalidProfile
    }
}

impl From<InvalidEmail> for UserAccountsError {
    fn from(_: InvalidEmail) -> Self {
        Self::InvalidEmail
    }
}

impl From<SubmissionError> for UserAccountsError {
    fn from(error: SubmissionError) -> Self {
        match error {
            SubmissionError::Domain(EmailReservationError::InvalidAccountCreationToken) => Self::InvalidAccountCreationToken,
            SubmissionError::Domain(EmailReservationError::NotReserved) => Self::RegistrationNotFound,
            SubmissionError::Domain(EmailReservationError::NotCurrentAccountId) => Self::RegistrationNotCurrent,
            SubmissionError::Domain(EmailReservationError::NotAwaitingProfile) => Self::EmailNotVerified,
            SubmissionError::Domain(EmailReservationError::ProfileAlreadySubmitted) => Self::ProfileAlreadySubmitted,
            SubmissionError::Domain(
                EmailReservationError::EmptyFirstName
                | EmailReservationError::EmptyLastName
                | EmailReservationError::FutureDateOfBirth,
            ) => Self::InvalidProfile,
            SubmissionError::Conflict => Self::Conflict,
            SubmissionError::Unavailable => Self::Unavailable,
            SubmissionError::Domain(_) => Self::Internal,
        }
    }
}

impl IntoResponse for UserAccountsError {
    fn into_response(self) -> Response {
        let retry_after = matches!(self, Self::Conflict);
        let (status, code, message) = match self {
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid_request", "invalid request"),
            Self::InvalidEmail => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_email", "invalid email address"),
            Self::InvalidProfile => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_profile",
                "registration profile is invalid",
            ),
            Self::InvalidAccountCreationToken => (
                StatusCode::FORBIDDEN,
                "invalid_account_creation_token",
                "account creation token is invalid",
            ),
            Self::RegistrationNotFound => (StatusCode::NOT_FOUND, "registration_not_found", "registration was not found"),
            Self::RegistrationNotCurrent => (
                StatusCode::NOT_FOUND,
                "registration_not_current",
                "registration is not current",
            ),
            Self::EmailNotVerified => (StatusCode::CONFLICT, "email_not_verified", "email has not been verified"),
            Self::ProfileAlreadySubmitted => (
                StatusCode::CONFLICT,
                "profile_already_submitted",
                "a registration profile was already submitted",
            ),
            Self::Conflict => (
                StatusCode::TOO_MANY_REQUESTS,
                "registration_conflict",
                "registration changed concurrently",
            ),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                "registration is temporarily unavailable",
            ),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", "registration failed"),
        };
        let mut response = error_response(status, code, message.to_owned());
        if retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}
