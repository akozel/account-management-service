/// Seven decimal digits, sampled by rand's automatically seeded cryptographic RNG.
pub fn verification_code() -> u32 {
    rand::random_range(1_000_000..10_000_000)
}

#[cfg(test)]
mod tests {
    #[test]
    fn generates_seven_digit_codes() {
        for _ in 0..128 {
            assert!((1_000_000..10_000_000).contains(&super::verification_code()));
        }
    }
}
