use std::sync::Arc;

use axum::{
    Router,
    extract::rejection::{JsonRejection, PathRejection},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};

use crate::{
    application::email_reservation::service::{EmailReservationService, EmailReservationServiceError},
    domain::InvalidEmail,
};

use super::error_response;

mod request_code;
mod reserve;
mod verify;

#[derive(Clone)]
pub(super) struct EmailReservationsState {
    service: Arc<dyn EmailReservationService>,
}

pub(super) fn router(service: Arc<dyn EmailReservationService>) -> Router {
    Router::new()
        .route("/email-reservations", post(reserve::reserve))
        .route(
            "/email-reservations/{email}/verification-codes",
            post(request_code::request_code),
        )
        .route("/email-reservations/{email}/verify", post(verify::verify))
        .route(
            "/email-reservations/{email}/registrations/{account_id}",
            get(registration_status_not_implemented),
        )
        .with_state(EmailReservationsState { service })
}

async fn registration_status_not_implemented() -> Response {
    error_response(
        StatusCode::NOT_IMPLEMENTED,
        "not_implemented",
        "registration status is not implemented".to_owned(),
    )
}

#[derive(Debug)]
pub(super) enum EmailReservationsError {
    InvalidRequest,
    InvalidEmail,
    AlreadyReserved,
    NotReserved,
    NotCurrentAccountId,
    AlreadyVerified,
    AccountCreationStarted,
    InvalidCode,
    Conflict,
    Unavailable,
    Internal,
}

impl From<JsonRejection> for EmailReservationsError {
    fn from(_: JsonRejection) -> Self {
        Self::InvalidRequest
    }
}

impl From<PathRejection> for EmailReservationsError {
    fn from(_: PathRejection) -> Self {
        Self::InvalidRequest
    }
}

impl From<InvalidEmail> for EmailReservationsError {
    fn from(_: InvalidEmail) -> Self {
        Self::InvalidEmail
    }
}

impl From<uuid::Error> for EmailReservationsError {
    fn from(_: uuid::Error) -> Self {
        Self::InvalidRequest
    }
}

impl From<EmailReservationServiceError> for EmailReservationsError {
    fn from(error: EmailReservationServiceError) -> Self {
        match error {
            EmailReservationServiceError::AlreadyReserved => Self::AlreadyReserved,
            EmailReservationServiceError::NotReserved => Self::NotReserved,
            EmailReservationServiceError::NotCurrentAccountId => Self::NotCurrentAccountId,
            EmailReservationServiceError::AlreadyVerified => Self::AlreadyVerified,
            EmailReservationServiceError::AccountCreationStarted => Self::AccountCreationStarted,
            EmailReservationServiceError::InvalidVerificationCode => Self::InvalidCode,
            EmailReservationServiceError::Conflict => Self::Conflict,
            EmailReservationServiceError::Unavailable => Self::Unavailable,
            EmailReservationServiceError::UnexpectedDomainRejection(_) => Self::Internal,
        }
    }
}

impl IntoResponse for EmailReservationsError {
    fn into_response(self) -> Response {
        let retry_after = matches!(self, Self::Conflict);
        let (status, code, message) = match self {
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid_request", "invalid request"),
            Self::InvalidEmail => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_email", "invalid email address"),
            Self::AlreadyReserved => (StatusCode::CONFLICT, "email_already_reserved", "email is already reserved"),
            Self::NotReserved => (StatusCode::NOT_FOUND, "email_not_reserved", "email is not reserved"),
            Self::NotCurrentAccountId => (
                StatusCode::NOT_FOUND,
                "registration_not_current",
                "registration is not current",
            ),
            Self::AlreadyVerified => (StatusCode::CONFLICT, "already_verified", "email is already verified"),
            Self::AccountCreationStarted => (
                StatusCode::CONFLICT,
                "account_creation_started",
                "account creation has already started",
            ),
            Self::InvalidCode => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_verification_code",
                "invalid verification code",
            ),
            Self::Conflict => (
                StatusCode::TOO_MANY_REQUESTS,
                "email_reservation_conflict",
                "email reservation changed concurrently",
            ),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                "email reservation is temporarily unavailable",
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "email reservation failed",
            ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        application::{
            email_reservation::service::{ReservedEmail, VerifiedEmail},
            user_account::{
                service::{AcceptedSubmission, ContinuationError, ProfileSubmission, RegistrationService, SubmissionError},
                tasks::{CreateAccountTaskV1, RecordAccountCreatedTaskV1},
            },
        },
        domain::{Email, UserAccountId},
    };
    use async_trait::async_trait;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, header},
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;

    const ID: &str = "26e91668-f9fc-4be4-bd9d-321fba42e18b";

    struct StubService(Option<EmailReservationServiceError>);

    struct StubRegistrationService;

    #[async_trait]
    impl EmailReservationService for StubService {
        async fn reserve(&self, email: Email) -> Result<ReservedEmail, EmailReservationServiceError> {
            if let Some(error) = &self.0 {
                return Err(error.clone());
            }
            Ok(ReservedEmail {
                email,
                account_id: ID.parse().unwrap(),
            })
        }
        async fn request_new_verification_code(&self, _: Email) -> Result<(), EmailReservationServiceError> {
            self.0.clone().map_or(Ok(()), Err)
        }
        async fn confirm_email(
            &self,
            email: Email,
            account_id: UserAccountId,
            _: u32,
        ) -> Result<VerifiedEmail, EmailReservationServiceError> {
            if let Some(error) = &self.0 {
                return Err(error.clone());
            }
            Ok(VerifiedEmail {
                email,
                account_id,
                account_creation_token: "opaque-test-token".into(),
            })
        }
    }

    #[async_trait]
    impl RegistrationService for StubRegistrationService {
        async fn submit_profile(&self, submission: ProfileSubmission) -> Result<AcceptedSubmission, SubmissionError> {
            Ok(AcceptedSubmission {
                email: submission.email,
                account_id: submission.account_id,
            })
        }

        async fn create_account(&self, _: CreateAccountTaskV1) -> Result<(), ContinuationError> {
            unreachable!("HTTP router does not continue outbox tasks")
        }

        async fn record_account_created(&self, _: RecordAccountCreatedTaskV1) -> Result<(), ContinuationError> {
            unreachable!("HTTP router does not continue outbox tasks")
        }
    }

    async fn call(
        uri: &str,
        body: Option<Value>,
        error: Option<EmailReservationServiceError>,
    ) -> (StatusCode, Value, axum::http::HeaderMap) {
        let app = super::super::router(Arc::new(StubService(error)), Arc::new(StubRegistrationService));
        let response = app
            .oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(body.map_or(Body::empty(), |body| Body::from(body.to_string())))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, body, headers)
    }

    #[tokio::test]
    async fn both_resource_routers_have_the_documented_routes_and_old_routes_are_gone() {
        let (status, body, _) = call("/email-reservations", Some(json!({"email":" Alice@Example.COM "})), None).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body, json!({"email":"alice@example.com","account_id":ID}));
        let (status, body, _) = call("/email-reservations/alice%40example.com/verification-codes", None, None).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body, json!({"status":"verification_code_requested"}));
        let (status, body, headers) = call(
            "/email-reservations/alice%40example.com/verify",
            Some(json!({"account_id":ID,"code":1234567})),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(
            body,
            json!({"email":"alice@example.com","account_id":ID,"account_creation_token":"opaque-test-token"})
        );
        let (status, body, _) = call(
            "/user-accounts",
            Some(json!({
                "email": "Alice@Example.COM",
                "account_id": ID,
                "account_creation_token": "opaque-test-token",
                "first_name": "Alice",
                "last_name": "Smith",
                "date_of_birth": "1990-05-12"
            })),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            body,
            json!({
                "account_id": ID,
                "status_url": format!("/email-reservations/alice%40example.com/registrations/{ID}")
            })
        );
        for old in [
            "/email-verifications/alice@example.com/request-new-code",
            "/email-verifications/alice@example.com/confirm?code=1234567",
        ] {
            assert_eq!(call(old, None, None).await.0, StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn registration_status_is_an_explicit_placeholder() {
        let app = super::super::router(Arc::new(StubService(None)), Arc::new(StubRegistrationService));
        let response = app
            .oneshot(
                Request::get(format!("/email-reservations/alice%40example.com/registrations/{ID}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"code":"not_implemented","message":"registration status is not implemented"})
        );
    }

    #[tokio::test]
    async fn malformed_inputs_and_invalid_email_are_safe() {
        for (uri, body, status) in [
            ("/email-reservations", Some(json!({})), StatusCode::BAD_REQUEST),
            (
                "/email-reservations",
                Some(json!({"email":"invalid"})),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "/email-reservations/alice%40example.com/verify",
                Some(json!({"account_id":"bad","code":1234567})),
                StatusCode::BAD_REQUEST,
            ),
            (
                "/email-reservations/alice%40example.com/verify",
                Some(json!({"account_id":ID,"code":"secret"})),
                StatusCode::BAD_REQUEST,
            ),
            (
                "/email-reservations/invalid/verification-codes",
                None,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
        ] {
            let (actual, response, _) = call(uri, body, None).await;
            assert_eq!(actual, status, "{uri}");
            assert!(!response.to_string().contains("secret"));
        }
    }

    #[tokio::test]
    async fn service_failures_map_to_stable_safe_http_errors() {
        use EmailReservationServiceError as E;
        for (error, status, code) in [
            (E::AlreadyReserved, StatusCode::CONFLICT, "email_already_reserved"),
            (E::NotReserved, StatusCode::NOT_FOUND, "email_not_reserved"),
            (E::NotCurrentAccountId, StatusCode::NOT_FOUND, "registration_not_current"),
            (E::AlreadyVerified, StatusCode::CONFLICT, "already_verified"),
            (E::AccountCreationStarted, StatusCode::CONFLICT, "account_creation_started"),
            (
                E::InvalidVerificationCode,
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_verification_code",
            ),
            (E::Conflict, StatusCode::TOO_MANY_REQUESTS, "email_reservation_conflict"),
            (E::Unavailable, StatusCode::SERVICE_UNAVAILABLE, "service_unavailable"),
            (
                E::UnexpectedDomainRejection(crate::domain::EmailReservationError::AccountIdUnchanged),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ),
        ] {
            let (actual, body, headers) = call(
                "/email-reservations/alice%40example.com/verify",
                Some(json!({"account_id":ID,"code":1234567})),
                Some(error),
            )
            .await;
            assert_eq!(actual, status);
            assert_eq!(body["code"], code);
            if code == "email_reservation_conflict" {
                assert_eq!(headers.get(header::RETRY_AFTER).unwrap(), "1");
            } else {
                assert!(headers.get(header::RETRY_AFTER).is_none());
            }
            assert!(!body.to_string().contains("opaque-test-token"));
        }
    }
}
