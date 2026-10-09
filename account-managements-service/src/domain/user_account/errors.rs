use chrono::NaiveDate;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UserAccountError {
    #[error("first name must not be empty")]
    EmptyFirstName,
    #[error("last name must not be empty")]
    EmptyLastName,
    #[error("date of birth {date_of_birth} is after validation date {validation_date}")]
    FutureDateOfBirth {
        date_of_birth: NaiveDate,
        validation_date: NaiveDate,
    },
    #[error("account already exists with different data")]
    AlreadyExistsWithDifferentData,
    #[error("account was already created with the same data")]
    AlreadyCreated,
}
