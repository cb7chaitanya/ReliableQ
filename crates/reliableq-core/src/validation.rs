//! Request/payload validation shared by the API (submission) and the
//! worker (execution needs the same charge payload shape). Keeping this
//! in one place means a payload that passes submission validation is
//! guaranteed to parse the same way when a worker later executes it.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_KIND_LEN: usize = 64;
pub const MAX_CUSTOMER_ID_LEN: usize = 128;
pub const MIN_MAX_ATTEMPTS: i32 = 1;
pub const MAX_MAX_ATTEMPTS: i32 = 20;

/// The only job payload shape this project executes (see SPEC.md sec. 3
/// non-goals: no generic workflow engine). `kind` is still a free-text
/// column so the schema does not hard-code this, but validation and the
/// worker's charge handler both assume this shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChargePayload {
    pub customer_id: String,
    pub amount_cents: i64,
    pub currency: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("kind must not be empty")]
    EmptyKind,
    #[error("kind must be at most {MAX_KIND_LEN} characters")]
    KindTooLong,
    #[error("payload does not match the expected charge shape: {0}")]
    InvalidPayloadShape(String),
    #[error("payload.customer_id must not be empty")]
    EmptyCustomerId,
    #[error("payload.customer_id must be at most {MAX_CUSTOMER_ID_LEN} characters")]
    CustomerIdTooLong,
    #[error("payload.amount_cents must be positive")]
    NonPositiveAmount,
    #[error("payload.currency must be a three-letter uppercase ISO 4217 code")]
    InvalidCurrency,
    #[error("max_attempts must be between {MIN_MAX_ATTEMPTS} and {MAX_MAX_ATTEMPTS}")]
    MaxAttemptsOutOfRange,
}

pub fn validate_kind(kind: &str) -> Result<(), ValidationError> {
    if kind.trim().is_empty() {
        return Err(ValidationError::EmptyKind);
    }
    if kind.len() > MAX_KIND_LEN {
        return Err(ValidationError::KindTooLong);
    }
    Ok(())
}

pub fn validate_max_attempts(max_attempts: i32) -> Result<(), ValidationError> {
    if !(MIN_MAX_ATTEMPTS..=MAX_MAX_ATTEMPTS).contains(&max_attempts) {
        return Err(ValidationError::MaxAttemptsOutOfRange);
    }
    Ok(())
}

/// Parses and validates a raw JSON payload as a [`ChargePayload`].
pub fn parse_charge_payload(payload: &serde_json::Value) -> Result<ChargePayload, ValidationError> {
    let parsed: ChargePayload = serde_json::from_value(payload.clone())
        .map_err(|err| ValidationError::InvalidPayloadShape(err.to_string()))?;
    validate_charge_payload(&parsed)?;
    Ok(parsed)
}

pub fn validate_charge_payload(payload: &ChargePayload) -> Result<(), ValidationError> {
    if payload.customer_id.trim().is_empty() {
        return Err(ValidationError::EmptyCustomerId);
    }
    if payload.customer_id.len() > MAX_CUSTOMER_ID_LEN {
        return Err(ValidationError::CustomerIdTooLong);
    }
    if payload.amount_cents <= 0 {
        return Err(ValidationError::NonPositiveAmount);
    }
    let is_valid_currency =
        payload.currency.len() == 3 && payload.currency.chars().all(|c| c.is_ascii_uppercase());
    if !is_valid_currency {
        return Err(ValidationError::InvalidCurrency);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn valid_kind_passes() {
        assert!(validate_kind("charge").is_ok());
    }

    #[test]
    fn empty_kind_rejected() {
        assert_eq!(validate_kind("  "), Err(ValidationError::EmptyKind));
    }

    #[test]
    fn overlong_kind_rejected() {
        let kind = "x".repeat(MAX_KIND_LEN + 1);
        assert_eq!(validate_kind(&kind), Err(ValidationError::KindTooLong));
    }

    #[test]
    fn max_attempts_bounds() {
        assert!(validate_max_attempts(1).is_ok());
        assert!(validate_max_attempts(MAX_MAX_ATTEMPTS).is_ok());
        assert_eq!(
            validate_max_attempts(0),
            Err(ValidationError::MaxAttemptsOutOfRange)
        );
        assert_eq!(
            validate_max_attempts(MAX_MAX_ATTEMPTS + 1),
            Err(ValidationError::MaxAttemptsOutOfRange)
        );
    }

    #[test]
    fn valid_charge_payload_parses() {
        let payload = json!({
            "customer_id": "c123",
            "amount_cents": 5000,
            "currency": "INR",
        });
        let parsed = parse_charge_payload(&payload).unwrap();
        assert_eq!(parsed.customer_id, "c123");
        assert_eq!(parsed.amount_cents, 5000);
        assert_eq!(parsed.currency, "INR");
    }

    #[test]
    fn non_positive_amount_rejected() {
        let payload = json!({
            "customer_id": "c123",
            "amount_cents": 0,
            "currency": "INR",
        });
        assert_eq!(
            parse_charge_payload(&payload),
            Err(ValidationError::NonPositiveAmount)
        );
    }

    #[test]
    fn lowercase_currency_rejected() {
        let payload = json!({
            "customer_id": "c123",
            "amount_cents": 100,
            "currency": "inr",
        });
        assert_eq!(
            parse_charge_payload(&payload),
            Err(ValidationError::InvalidCurrency)
        );
    }

    #[test]
    fn two_letter_currency_rejected() {
        let payload = json!({
            "customer_id": "c123",
            "amount_cents": 100,
            "currency": "IN",
        });
        assert_eq!(
            parse_charge_payload(&payload),
            Err(ValidationError::InvalidCurrency)
        );
    }

    #[test]
    fn empty_customer_id_rejected() {
        let payload = json!({
            "customer_id": "",
            "amount_cents": 100,
            "currency": "INR",
        });
        assert_eq!(
            parse_charge_payload(&payload),
            Err(ValidationError::EmptyCustomerId)
        );
    }

    #[test]
    fn missing_field_rejected_as_invalid_shape() {
        let payload = json!({ "customer_id": "c123" });
        assert!(matches!(
            parse_charge_payload(&payload),
            Err(ValidationError::InvalidPayloadShape(_))
        ));
    }
}
