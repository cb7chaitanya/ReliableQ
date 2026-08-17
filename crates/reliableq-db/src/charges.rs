//! `charges` repository. M1 is intentionally naive: [`insert_charge`]
//! does not check for an existing row before inserting. A genuine
//! `idempotency_key` reuse hits the table's unique constraint and
//! surfaces as [`RepoError::Database`] rather than a graceful replay —
//! see `docs/failure-lab.md` M3, which replaces this with an atomic
//! check-and-insert-or-replay plus payload-conflict detection.

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

pub async fn insert_charge(
    pool: &PgPool,
    id: Uuid,
    idempotency_key: &str,
    customer_id: &str,
    amount_cents: i64,
    currency: &str,
) -> Result<ChargeRow, RepoError> {
    let row = sqlx::query_as::<_, ChargeRow>(
        r#"
        INSERT INTO charges (id, idempotency_key, customer_id, amount_cents, currency)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(idempotency_key)
    .bind(customer_id)
    .bind(amount_cents)
    .bind(currency)
    .fetch_one(pool)
    .await?;
    Ok(row)
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
