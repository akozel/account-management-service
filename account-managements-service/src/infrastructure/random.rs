//! Entropy-backed generation adapters.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

use crate::{application::email_reservation::service::RegistrationMaterialGenerator, domain::UserAccountId};

/// Seven decimal digits, sampled by rand's automatically seeded cryptographic RNG.
fn verification_code() -> u32 {
    rand::random_range(1_000_000..10_000_000)
}

pub struct RandomRegistrationMaterialGenerator;

impl RegistrationMaterialGenerator for RandomRegistrationMaterialGenerator {
    fn account_id(&self) -> UserAccountId {
        UserAccountId::new()
    }

    fn verification_code(&self) -> u32 {
        verification_code()
    }

    fn account_creation_token(&self) -> String {
        URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_seven_digit_code_and_256_bit_token() {
        let generator = RandomRegistrationMaterialGenerator;
        for _ in 0..128 {
            assert!((1_000_000..=9_999_999).contains(&generator.verification_code()));
        }
        let token = generator.account_creation_token();
        assert_eq!(URL_SAFE_NO_PAD.decode(&token).unwrap().len(), 32);
        assert_eq!(generator.account_id().as_uuid().get_version_num(), 4);
    }
}
