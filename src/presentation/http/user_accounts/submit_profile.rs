use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use chrono::NaiveDate;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};

use crate::{
    application::user_account::service::ProfileSubmission,
    domain::{Email, UserAccountId},
};

use super::{UserAccountsError, UserAccountsState};

const PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

#[derive(Deserialize)]
pub(super) struct SubmitProfileRequest {
    email: String,
    account_id: String,
    account_creation_token: String,
    first_name: String,
    last_name: String,
    date_of_birth: String,
}

#[derive(Serialize)]
pub(super) struct SubmitProfileResponse {
    account_id: UserAccountId,
    status_url: String,
}

pub(super) async fn submit_profile(
    State(state): State<UserAccountsState>,
    payload: Result<Json<SubmitProfileRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<SubmitProfileResponse>), UserAccountsError> {
    let Json(request) = payload?;
    let submission = ProfileSubmission {
        email: Email::parse(&request.email)?,
        account_id: request.account_id.parse()?,
        account_creation_token: request.account_creation_token,
        first_name: request.first_name,
        last_name: request.last_name,
        date_of_birth: NaiveDate::parse_from_str(&request.date_of_birth, "%Y-%m-%d")?,
    };
    let accepted = state.service.submit_profile(submission).await?;
    let email = utf8_percent_encode(accepted.email.as_str(), PATH_SEGMENT);
    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitProfileResponse {
            account_id: accepted.account_id,
            status_url: format!("/email-reservations/{email}/registrations/{}", accepted.account_id),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, header},
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::{
        application::user_account::{
            service::{AcceptedSubmission, ContinuationError, RegistrationService, SubmissionError},
            tasks::{CreateAccountTaskV1, RecordAccountCreatedTaskV1},
        },
        domain::EmailReservationError,
    };

    use super::*;

    const ID: &str = "26e91668-f9fc-4be4-bd9d-321fba42e18b";
    const VALID_BODY: &str = r#"{
        "email":" Alice+Tag@Example.COM ",
        "account_id":"26e91668-f9fc-4be4-bd9d-321fba42e18b",
        "account_creation_token":"never-leak-token",
        "first_name":" Sensitive ",
        "last_name":" Person ",
        "date_of_birth":"1990-05-12"
    }"#;

    #[derive(Default)]
    struct StubService {
        error: Option<SubmissionError>,
        submissions: Mutex<Vec<ProfileSubmission>>,
    }

    impl StubService {
        fn failing(error: SubmissionError) -> Self {
            Self {
                error: Some(error),
                submissions: Mutex::default(),
            }
        }
    }

    #[async_trait]
    impl RegistrationService for StubService {
        async fn submit_profile(&self, submission: ProfileSubmission) -> Result<AcceptedSubmission, SubmissionError> {
            self.submissions.lock().unwrap().push(submission.clone());
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            Ok(AcceptedSubmission {
                email: submission.email,
                account_id: submission.account_id,
            })
        }

        async fn create_account(&self, _: CreateAccountTaskV1) -> Result<(), ContinuationError> {
            unreachable!("HTTP resource does not continue outbox tasks")
        }

        async fn record_account_created(&self, _: RecordAccountCreatedTaskV1) -> Result<(), ContinuationError> {
            unreachable!("HTTP resource does not continue outbox tasks")
        }
    }

    async fn call(service: Arc<StubService>, body: impl Into<Body>) -> (StatusCode, Value, axum::http::HeaderMap) {
        let response = super::super::router(service)
            .oneshot(
                Request::post("/user-accounts")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(body.into())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        let body = serde_json::from_slice(&bytes).unwrap();
        (status, body, headers)
    }

    #[tokio::test]
    async fn accepts_a_profile_and_returns_the_canonical_encoded_status_url() {
        let service = Arc::new(StubService::default());

        let (status, body, headers) = call(service.clone(), VALID_BODY).await;

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            body,
            json!({
                "account_id": ID,
                "status_url": format!("/email-reservations/alice%2Btag%40example.com/registrations/{ID}")
            })
        );
        assert!(
            headers
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        let submissions = service.submissions.lock().unwrap();
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0].email.as_str(), "alice+tag@example.com");
        assert_eq!(submissions[0].account_id.to_string(), ID);
        assert_eq!(submissions[0].account_creation_token, "never-leak-token");
        assert_eq!(submissions[0].first_name, " Sensitive ");
        assert_eq!(submissions[0].last_name, " Person ");
        assert_eq!(submissions[0].date_of_birth, NaiveDate::from_ymd_opt(1990, 5, 12).unwrap());
    }

    #[tokio::test]
    async fn rejects_malformed_json_uuid_and_missing_fields_as_invalid_requests() {
        for body in [
            "{",
            r#"{"email":"alice@example.com"}"#,
            r#"{
                "email":"alice@example.com",
                "account_id":"not-a-uuid",
                "account_creation_token":"secret",
                "first_name":"Alice",
                "last_name":"Smith",
                "date_of_birth":"1990-05-12"
            }"#,
        ] {
            let (status, response, _) = call(Arc::new(StubService::default()), body.to_owned()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(response["code"], "invalid_request");
            assert!(!response.to_string().contains("secret"));
        }
    }

    #[tokio::test]
    async fn rejects_invalid_email_and_date_before_calling_the_service() {
        for (field, value, code) in [
            ("email", "not-an-email", "invalid_email"),
            ("date_of_birth", "1990-02-30", "invalid_profile"),
        ] {
            let service = Arc::new(StubService::default());
            let mut request: Value = serde_json::from_str(VALID_BODY).unwrap();
            request[field] = json!(value);
            let (status, response, _) = call(service.clone(), request.to_string()).await;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
            assert_eq!(response["code"], code);
            assert!(service.submissions.lock().unwrap().is_empty());
            assert!(!response.to_string().contains(value));
        }
    }

    #[tokio::test]
    async fn maps_expected_application_failures_to_safe_http_errors() {
        use EmailReservationError as Domain;

        for (error, status, code) in [
            (
                SubmissionError::Domain(Domain::InvalidAccountCreationToken),
                StatusCode::FORBIDDEN,
                "invalid_account_creation_token",
            ),
            (
                SubmissionError::Domain(Domain::NotReserved),
                StatusCode::NOT_FOUND,
                "registration_not_found",
            ),
            (
                SubmissionError::Domain(Domain::NotCurrentAccountId),
                StatusCode::NOT_FOUND,
                "registration_not_current",
            ),
            (
                SubmissionError::Domain(Domain::NotAwaitingProfile),
                StatusCode::CONFLICT,
                "email_not_verified",
            ),
            (
                SubmissionError::Domain(Domain::ProfileAlreadySubmitted),
                StatusCode::CONFLICT,
                "profile_already_submitted",
            ),
            (
                SubmissionError::Domain(Domain::EmptyFirstName),
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_profile",
            ),
            (
                SubmissionError::Domain(Domain::EmptyLastName),
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_profile",
            ),
            (
                SubmissionError::Domain(Domain::FutureDateOfBirth),
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_profile",
            ),
            (
                SubmissionError::Conflict,
                StatusCode::TOO_MANY_REQUESTS,
                "registration_conflict",
            ),
            (
                SubmissionError::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
            ),
        ] {
            let (actual, body, headers) = call(Arc::new(StubService::failing(error)), VALID_BODY).await;
            assert_eq!(actual, status);
            assert_eq!(body["code"], code);
            if code == "registration_conflict" {
                assert_eq!(headers.get(header::RETRY_AFTER).unwrap(), "1");
            } else {
                assert!(headers.get(header::RETRY_AFTER).is_none());
            }
            assert!(
                headers
                    .get(header::CONTENT_TYPE)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("application/json")
            );
            assert!(!body.to_string().contains("never-leak-token"));
            assert!(!body.to_string().contains("Sensitive"));
        }
    }

    #[tokio::test]
    async fn hides_every_unexpected_domain_rejection() {
        use EmailReservationError as Domain;

        for error in [
            Domain::AlreadyReserved,
            Domain::AlreadyVerified,
            Domain::InvalidVerificationCode,
            Domain::InvalidVerificationCodeFormat,
            Domain::AccountIdUnchanged,
            Domain::AccountCreationStarted,
            Domain::AccountCreationMismatch,
            Domain::AccountCreationAlreadyCompleted,
        ] {
            let (status, body, _) = call(Arc::new(StubService::failing(SubmissionError::Domain(error))), VALID_BODY).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(body, json!({"code":"internal_error", "message":"registration failed"}));
            assert!(!body.to_string().contains("never-leak-token"));
        }
    }
}
