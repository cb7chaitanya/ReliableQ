//! Redaction helpers for structured logs (spec sec. 13.1: "Never log
//! database URLs, auth headers, raw lease tokens, or unredacted
//! arbitrary payloads").

use sha2::{Digest, Sha256};
use uuid::Uuid;

/// A short, non-reversible fingerprint of a lease token, safe to log:
/// enough to visually correlate "same token" across log lines without
/// exposing a value that could be used to forge fencing.
pub fn lease_token_hash(token: Uuid) -> String {
    let digest = Sha256::digest(token.as_bytes());
    hex::encode(&digest[..6])
}

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_token_hash_is_deterministic_and_short() {
        let token = Uuid::new_v4();
        let a = lease_token_hash(token);
        let b = lease_token_hash(token);
        assert_eq!(a, b);
        assert_eq!(a.len(), 12);
    }

    #[test]
    fn lease_token_hash_differs_across_tokens() {
        assert_ne!(
            lease_token_hash(Uuid::new_v4()),
            lease_token_hash(Uuid::new_v4())
        );
    }

    #[test]
    fn lease_token_hash_never_contains_the_original_uuid_text() {
        let token = Uuid::new_v4();
        let hash = lease_token_hash(token);
        assert!(!hash.contains(&token.to_string()));
    }
}
