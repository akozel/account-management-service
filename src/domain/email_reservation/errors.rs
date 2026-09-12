use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EmailReservationError {
    #[error("email is already reserved")]
    AlreadyReserved,
    #[error("email is not reserved")]
    NotReserved,
    #[error("account ID is no longer current for this email")]
    NotCurrentAccountId,
    #[error("email is already verified")]
    AlreadyVerified,
    #[error("verification code does not match")]
    InvalidVerificationCode,
    #[error("verification code must have seven decimal digits")]
    InvalidVerificationCodeFormat,
    #[error("new account ID must differ from the current one")]
    AccountIdUnchanged,
    #[error("profile submission requires a verified email")]
    NotAwaitingProfile,
    #[error("account creation token does not match")]
    InvalidAccountCreationToken,
    #[error("profile was already submitted and the account creation token was consumed")]
    ProfileAlreadySubmitted,
    #[error("first name must not be empty")]
    EmptyFirstName,
    #[error("last name must not be empty")]
    EmptyLastName,
    #[error("date of birth is after the validation date")]
    FutureDateOfBirth,
    #[error("account creation has already started")]
    AccountCreationStarted,
    #[error("account creation data does not match the accepted profile")]
    AccountCreationMismatch,
    #[error("account creation was already completed with the same data")]
    AccountCreationAlreadyCompleted,
}
