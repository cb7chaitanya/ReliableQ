//! Failure classification for downstream call outcomes (spec sec. 9.2,
//! 12): transient failures retry, permanent failures do not, and
//! ambiguous failures (no response received at all) retry because
//! idempotency (ADR 0004) makes that safe.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// A response was received indicating a temporary problem
    /// (408, 429, 5xx): safe and expected to retry.
    Transient,
    /// A validated business rejection (4xx other than 408/429, e.g.
    /// 422): retrying would not change the outcome.
    Permanent,
    /// No response was received at all (timeout, connection error):
    /// unknown whether the effect happened, but retrying is safe
    /// because the downstream call is idempotent.
    Ambiguous,
}

impl FailureClass {
    pub const fn is_retryable(self) -> bool {
        matches!(self, FailureClass::Transient | FailureClass::Ambiguous)
    }
}

/// Classifies a downstream HTTP status code that was actually received.
/// Only call this for non-2xx responses.
pub fn classify_http_status(status: u16) -> FailureClass {
    match status {
        408 | 429 => FailureClass::Transient,
        500..=599 => FailureClass::Transient,
        _ => FailureClass::Permanent,
    }
}

/// Classifies a failure to get any HTTP response at all (timeout,
/// connection refused/reset, DNS failure, ...).
pub const fn classify_network_error() -> FailureClass {
    FailureClass::Ambiguous
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_and_timeout_status_are_transient() {
        assert_eq!(classify_http_status(408), FailureClass::Transient);
        assert_eq!(classify_http_status(429), FailureClass::Transient);
    }

    #[test]
    fn server_errors_are_transient() {
        for status in [500, 502, 503, 504, 599] {
            assert_eq!(classify_http_status(status), FailureClass::Transient);
        }
    }

    #[test]
    fn business_rejection_is_permanent() {
        assert_eq!(classify_http_status(422), FailureClass::Permanent);
        assert_eq!(classify_http_status(400), FailureClass::Permanent);
        assert_eq!(classify_http_status(404), FailureClass::Permanent);
    }

    #[test]
    fn network_error_is_ambiguous() {
        assert_eq!(classify_network_error(), FailureClass::Ambiguous);
    }

    #[test]
    fn transient_and_ambiguous_are_retryable_permanent_is_not() {
        assert!(FailureClass::Transient.is_retryable());
        assert!(FailureClass::Ambiguous.is_retryable());
        assert!(!FailureClass::Permanent.is_retryable());
    }
}
