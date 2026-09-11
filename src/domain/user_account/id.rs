use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_converts_and_displays_user_account_id() {
        let raw = "26e91668-f9fc-4be4-bd9d-321fba42e18b";
        let uuid = Uuid::parse_str(raw).expect("UUID must be valid");
        let id = UserAccountId::from_uuid(uuid);
        let converted_id = UserAccountId::from(uuid);

        assert_eq!(id.as_uuid(), &uuid);
        assert_eq!(id, converted_id);
        assert_eq!(id.to_string(), raw);
    }

    #[test]
    fn parses_valid_user_account_id_and_rejects_invalid_one() {
        let raw = "26e91668-f9fc-4be4-bd9d-321fba42e18b";
        let id: UserAccountId = raw.parse().expect("UUID must be valid");

        assert_eq!(id.to_string(), raw);
        assert!("not-a-uuid".parse::<UserAccountId>().is_err());
    }

    #[test]
    fn creates_random_id_by_default() {
        let id = UserAccountId::default();

        assert_eq!(id.as_uuid().get_version_num(), 4);
    }
}
