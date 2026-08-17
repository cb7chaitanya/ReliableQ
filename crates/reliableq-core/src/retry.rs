//! Retry backoff math (spec sec. 10). Pure and deterministic given an
//! injected RNG, so tests never depend on production randomness.
//!
//! ```text
//! cap(n) = min(max_delay, base_delay * 2^(n - 1))
//! delay  = uniform(0, cap(n))
//! ```

use std::time::Duration;

use rand::Rng;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub base_delay: Duration,
    pub multiplier: u32,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub const DEFAULT: RetryPolicy = RetryPolicy {
        base_delay: Duration::from_secs(1),
        multiplier: 2,
        max_delay: Duration::from_secs(60),
    };

    /// `cap(n)` for attempt number `n` (1-indexed, the attempt that just
    /// failed): `min(max_delay, base_delay * multiplier^(n-1))`, using
    /// saturating arithmetic so extreme configuration cannot overflow or
    /// panic.
    pub fn cap(&self, attempt_number: u32) -> Duration {
        let exponent = attempt_number.saturating_sub(1);
        let multiplier_pow = saturating_pow(self.multiplier, exponent);
        let scaled = self.base_delay.saturating_mul(multiplier_pow);
        scaled.min(self.max_delay)
    }

    /// Full-jitter delay for attempt `attempt_number`: uniform random
    /// value in `[0, cap(attempt_number)]`.
    pub fn delay<R: Rng + ?Sized>(&self, attempt_number: u32, rng: &mut R) -> Duration {
        let cap = self.cap(attempt_number);
        if cap.is_zero() {
            return Duration::ZERO;
        }
        let cap_millis = cap.as_millis().min(u128::from(u64::MAX)) as u64;
        let jitter_millis = rng.gen_range(0..=cap_millis);
        Duration::from_millis(jitter_millis)
    }
}

fn saturating_pow(base: u32, exponent: u32) -> u32 {
    let mut result: u32 = 1;
    for _ in 0..exponent {
        result = result.saturating_mul(base);
        if result == u32::MAX {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn cap_grows_exponentially_then_saturates_at_max_delay() {
        let policy = RetryPolicy {
            base_delay: Duration::from_secs(1),
            multiplier: 2,
            max_delay: Duration::from_secs(60),
        };
        assert_eq!(policy.cap(1), Duration::from_secs(1));
        assert_eq!(policy.cap(2), Duration::from_secs(2));
        assert_eq!(policy.cap(3), Duration::from_secs(4));
        assert_eq!(policy.cap(4), Duration::from_secs(8));
        assert_eq!(policy.cap(5), Duration::from_secs(16));
        assert_eq!(policy.cap(6), Duration::from_secs(32));
        assert_eq!(
            policy.cap(7),
            Duration::from_secs(60),
            "capped at max_delay"
        );
        assert_eq!(policy.cap(20), Duration::from_secs(60), "stays capped");
    }

    #[test]
    fn cap_never_overflows_for_extreme_configuration() {
        let policy = RetryPolicy {
            base_delay: Duration::from_secs(3600),
            multiplier: 1000,
            max_delay: Duration::from_secs(60),
        };
        // Should saturate to max_delay, not panic or wrap.
        assert_eq!(policy.cap(50), Duration::from_secs(60));
    }

    #[test]
    fn cap_handles_attempt_number_one_as_base_delay() {
        let policy = RetryPolicy::DEFAULT;
        assert_eq!(policy.cap(1), policy.base_delay);
    }

    #[test]
    fn delay_is_within_zero_to_cap_bounds() {
        let policy = RetryPolicy::DEFAULT;
        let mut rng = StdRng::seed_from_u64(42);
        for attempt in 1..=10 {
            let cap = policy.cap(attempt);
            for _ in 0..50 {
                let delay = policy.delay(attempt, &mut rng);
                assert!(delay <= cap, "delay {delay:?} exceeded cap {cap:?}");
            }
        }
    }

    #[test]
    fn delay_is_deterministic_given_the_same_seed() {
        let policy = RetryPolicy::DEFAULT;
        let mut rng_a = StdRng::seed_from_u64(7);
        let mut rng_b = StdRng::seed_from_u64(7);
        let sequence_a: Vec<Duration> = (1..=5).map(|n| policy.delay(n, &mut rng_a)).collect();
        let sequence_b: Vec<Duration> = (1..=5).map(|n| policy.delay(n, &mut rng_b)).collect();
        assert_eq!(sequence_a, sequence_b);
    }

    #[test]
    fn delay_covers_a_spread_of_values_not_a_single_point() {
        // Statistical sanity check, not an exact-value assertion (spec
        // sec. 10: "tests must not depend on exact production
        // randomness").
        let policy = RetryPolicy {
            base_delay: Duration::from_secs(10),
            multiplier: 2,
            max_delay: Duration::from_secs(600),
        };
        let mut rng = StdRng::seed_from_u64(99);
        let samples: Vec<Duration> = (0..200).map(|_| policy.delay(3, &mut rng)).collect();
        let min = samples.iter().min().unwrap();
        let max = samples.iter().max().unwrap();
        assert!(
            max.as_millis() - min.as_millis() > 1000,
            "200 samples of full jitter should spread out meaningfully, got min={min:?} max={max:?}"
        );
    }
}
