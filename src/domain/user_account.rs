use std::{fmt, str::FromStr};

use chrono::NaiveDate;
use email_address::EmailAddress;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserAccountId(Uuid);

impl UserAccountId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for UserAccountId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for UserAccountId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl From<Uuid> for UserAccountId {
    fn from(value: Uuid) -> Self {
        Self::from_uuid(value)
    }
}

impl FromStr for UserAccountId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Email(EmailAddress);

impl Email {
    pub fn parse(value: &str) -> Result<Self, InvalidEmail> {
        value
            .trim()
            .parse::<EmailAddress>()
            .map(Self)
            .map_err(|_| InvalidEmail)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for Email {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for Email {
    type Err = InvalidEmail;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("invalid email address")]
pub struct InvalidEmail;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserAccount {
    id: UserAccountId,
    first_name: String,
    last_name: String,
    date_of_birth: NaiveDate,
    email: Email,
}

impl UserAccount {
    pub fn new(
        id: UserAccountId,
        first_name: String,
        last_name: String,
        date_of_birth: NaiveDate,
        email: Email,
        current_date: NaiveDate,
    ) -> Result<Self, UserAccountError> {
        let first_name = first_name.trim().to_owned();
        if first_name.is_empty() {
            return Err(UserAccountError::EmptyFirstName);
        }

        let last_name = last_name.trim().to_owned();
        if last_name.is_empty() {
            return Err(UserAccountError::EmptyLastName);
        }

        if date_of_birth > current_date {
            return Err(UserAccountError::FutureDateOfBirth {
                date_of_birth,
                current_date,
            });
        }

        Ok(Self {
            id,
            first_name,
            last_name,
            date_of_birth,
            email,
        })
    }

    pub const fn id(&self) -> &UserAccountId {
        &self.id
    }

    pub fn first_name(&self) -> &str {
        &self.first_name
    }

    pub fn last_name(&self) -> &str {
        &self.last_name
    }

    pub const fn date_of_birth(&self) -> NaiveDate {
        self.date_of_birth
    }

    pub const fn email(&self) -> &Email {
        &self.email
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UserAccountError {
    #[error("first name must not be empty")]
    EmptyFirstName,
    #[error("last name must not be empty")]
    EmptyLastName,
    #[error("date of birth {date_of_birth} is after current date {current_date}")]
    FutureDateOfBirth {
        date_of_birth: NaiveDate,
        current_date: NaiveDate,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("test date must be valid")
    }

    fn email() -> Email {
        Email::parse("alice@example.com").expect("test email must be valid")
    }

    #[test]
    fn creates_user_account_and_trims_names() {
        let id = UserAccountId::new();
        let account = UserAccount::new(
            id,
            "  Alice ".to_owned(),
            " Smith  ".to_owned(),
            date(1990, 5, 12),
            email(),
            date(2026, 9, 14),
        )
        .expect("account data must be valid");

        assert_eq!(account.id(), &id);
        assert_eq!(account.first_name(), "Alice");
        assert_eq!(account.last_name(), "Smith");
        assert_eq!(account.date_of_birth(), date(1990, 5, 12));
        assert_eq!(account.email().as_str(), "alice@example.com");
    }

    #[test]
    fn rejects_empty_first_name() {
        let result = UserAccount::new(
            UserAccountId::new(),
            "  ".to_owned(),
            "Smith".to_owned(),
            date(1990, 5, 12),
            email(),
            date(2026, 9, 14),
        );

        assert_eq!(result, Err(UserAccountError::EmptyFirstName));
    }

    #[test]
    fn rejects_empty_last_name() {
        let result = UserAccount::new(
            UserAccountId::new(),
            "Alice".to_owned(),
            "\t".to_owned(),
            date(1990, 5, 12),
            email(),
            date(2026, 9, 14),
        );

        assert_eq!(result, Err(UserAccountError::EmptyLastName));
    }

    #[test]
    fn rejects_future_date_of_birth() {
        let result = UserAccount::new(
            UserAccountId::new(),
            "Alice".to_owned(),
            "Smith".to_owned(),
            date(2026, 9, 15),
            email(),
            date(2026, 9, 14),
        );

        assert_eq!(
            result,
            Err(UserAccountError::FutureDateOfBirth {
                date_of_birth: date(2026, 9, 15),
                current_date: date(2026, 9, 14),
            })
        );
    }

    #[test]
    fn accepts_current_date_as_date_of_birth() {
        let current_date = date(2026, 9, 14);
        let result = UserAccount::new(
            UserAccountId::new(),
            "Alice".to_owned(),
            "Smith".to_owned(),
            current_date,
            email(),
            current_date,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn validates_and_trims_email() {
        let email = Email::parse("  alice@example.com ").expect("email must be valid");

        assert_eq!(email.as_str(), "alice@example.com");
        assert_eq!(Email::parse("not-an-email"), Err(InvalidEmail));
    }

    #[test]
    fn parses_user_account_id() {
        let raw = "26e91668-f9fc-4be4-bd9d-321fba42e18b";
        let id: UserAccountId = raw.parse().expect("UUID must be valid");

        assert_eq!(id.to_string(), raw);
    }
}
