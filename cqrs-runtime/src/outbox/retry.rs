use std::time::Duration;
/// Jittered exponential redelivery backoff, also used after lease expiry.
pub fn redelivery_backoff(attempt: i32) -> Duration {
    let exponent = attempt.saturating_sub(1).clamp(0, 6) as u32;
    let base = (5_u64 * 2_u64.pow(exponent)).min(300);
    let jitter = rand::random_range(0.8..=1.2);
    Duration::from_secs_f64((base as f64 * jitter).min(300.0))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backoff_is_bounded_for_every_attempt_range() {
        for attempt in [i32::MIN, 0, 1, 2, 6, 7, 10, i32::MAX] {
            for _ in 0..100 {
                let delay = redelivery_backoff(attempt);
                assert!((Duration::from_secs(4)..=Duration::from_secs(300)).contains(&delay));
                if attempt >= 7 {
                    assert!(delay >= Duration::from_secs(240));
                }
            }
        }
    }
}
