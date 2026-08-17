//! `charges` repository. [`insert_or_get_charge`] is the sole write
//! path: an `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`
//! atomically either creates the row or discovers it already exists,
//! with no separate check-then-insert race window (spec sec. 8.5:
//! "Concurrent duplicate requests must produce one charge row"). The
//! table's unique constraint on `idempotency_key` remains the actual
//! enforcement mechanism (spec sec. 7.4) — this function is just the
//! graceful path on top of it.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::jobs::RepoError;

#[derive(Debug, Clone, FromRow)]
pub struct ChargeRow {
    pub id: Uuid,
    pub idempotency_key: String,
    pub customer_id: String,
    pub amount_cents: i64,
    pub currency: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum InsertChargeOutcome {
    /// No prior charge existed for this key; this request created it.
    Created(ChargeRow),
    /// A charge already existed for this key with an identical
    /// semantic payload — the existing row, safe to return as-is.
    Replayed(ChargeRow),
    /// A charge already existed for this key with a *different*
    /// payload — a genuine idempotency-key collision, not a replay.
    Conflict(ChargeRow),
}

pub async fn insert_or_get_charge(
    pool: &PgPool,
    id: Uuid,
    idempotency_key: &str,
    customer_id: &str,
    amount_cents: i64,
    currency: &str,
) -> Result<InsertChargeOutcome, RepoError> {
    let inserted = sqlx::query_as::<_, ChargeRow>(
        r#"
        INSERT INTO charges (id, idempotency_key, customer_id, amount_cents, currency)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(idempotency_key)
    .bind(customer_id)
    .bind(amount_cents)
    .bind(currency)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = inserted {
        return Ok(InsertChargeOutcome::Created(row));
    }

    // Someone won the race (possibly this exact caller, retrying).
    // amount_cents/currency are never null in the schema, so an
    // existing row is guaranteed to be found here.
    let existing = find_by_idempotency_key(pool, idempotency_key)
        .await?
        .ok_or_else(|| {
            RepoError::Inconsistent(format!(
                "charge for idempotency_key {idempotency_key:?} disappeared immediately after an ON CONFLICT DO NOTHING"
            ))
        })?;

    let is_same_semantic_request = existing.customer_id == customer_id
        && existing.amount_cents == amount_cents
        && existing.currency == currency;

    if is_same_semantic_request {
        Ok(InsertChargeOutcome::Replayed(existing))
    } else {
        Ok(InsertChargeOutcome::Conflict(existing))
    }
}

pub async fn find_by_idempotency_key(
    pool: &PgPool,
    idempotency_key: &str,
) -> Result<Option<ChargeRow>, RepoError> {
    let row = sqlx::query_as::<_, ChargeRow>("SELECT * FROM charges WHERE idempotency_key = $1")
        .bind(idempotency_key)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}
