use std::{fmt, str::FromStr};

use email_address::{EmailAddress, Options};
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Email(EmailAddress);

impl Email {
    pub fn parse(value: &str) -> Result<Self, InvalidEmail> {
        // Quoted local parts introduce alternate spellings of the same mailbox.
        // The registration contract accepts an unquoted mailbox and a DNS domain.
        if value.contains('"') {
            return Err(InvalidEmail);
        }
        EmailAddress::parse_with_options(
            &value.trim().to_lowercase(),
            Options::default().without_display_text().without_domain_literal(),
        )
        .map(Self)
        .map_err(|_| InvalidEmail)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for Email {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EmailVisitor;

        impl de::Visitor<'_> for EmailVisitor {
            type Value = Email;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an email address without a display name")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Email, E> {
                Email::parse(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(EmailVisitor)
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
    fn validates_trims_and_normalizes_email() {
        let email = Email::parse("  Alice@Example.COM ").expect("test email must be valid");

        assert_eq!(email.as_str(), "alice@example.com");
        assert_eq!(Email::parse("not-an-email"), Err(InvalidEmail));
    }

    #[test]
    fn parses_and_displays_email() {
        let email: Email = "alice@example.com".parse().expect("email must be valid");

        assert_eq!(email.to_string(), "alice@example.com");
    }

    #[test]
    fn rejects_alternate_mailbox_spellings() {
        for value in [
            "Alice <alice@example.com>",
            "Bob <alice@example.com>",
            "<alice@example.com>",
            "\"alice\"@example.com",
            "alice@[127.0.0.1]",
        ] {
            assert_eq!(Email::parse(value), Err(InvalidEmail), "accepted {value}");
            assert!(serde_json::from_value::<Email>(serde_json::json!(value)).is_err());
        }
    }

    #[test]
    fn normalizes_every_construction_path_and_round_trips() {
        let expected = Email::parse("alice+tag@example.com").expect("valid email");
        for value in ["alice+tag@example.com", "Alice+Tag@EXAMPLE.COM", "  Alice+Tag@Example.com  "] {
            let parsed: Email = value.parse().expect("valid email");
            let decoded: Email = serde_json::from_value(serde_json::json!(value)).expect("valid email JSON");
            assert_eq!(parsed, expected);
            assert_eq!(decoded, expected);
            assert_eq!(
                serde_json::to_value(&decoded).expect("serializable email"),
                "alice+tag@example.com"
            );
            assert_eq!(Email::parse(decoded.as_str()).expect("canonical email"), decoded);
        }
        assert!(serde_json::from_value::<Email>(serde_json::json!(123)).is_err());
        assert!(serde_json::from_value::<Email>(serde_json::json!("invalid")).is_err());
    }
}
