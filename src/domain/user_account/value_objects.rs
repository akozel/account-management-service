use std::{fmt, str::FromStr};

use email_address::EmailAddress;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_and_trims_email() {
        let email = Email::parse("  alice@example.com ").expect("test email must be valid");

        assert_eq!(email.as_str(), "alice@example.com");
        assert_eq!(Email::parse("not-an-email"), Err(InvalidEmail));
    }

    #[test]
    fn parses_and_displays_email() {
        let email: Email = "alice@example.com".parse().expect("email must be valid");

        assert_eq!(email.to_string(), "alice@example.com");
    }
}
