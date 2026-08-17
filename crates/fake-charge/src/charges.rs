//! `POST /v1/charges` (spec sec. 8.5). M1 scope only: validates the
//! request and the required `Idempotency-Key` header, then inserts.
//! There is no pre-check/replay yet — see reliableq-db::charges and
//! docs/failure-lab.md M3.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use reliableq_core::validation::parse_charge_payload;
use reliableq_db::charges;
use serde::Serialize;
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/charges", post(create_charge))
}

#[derive(Debug, Serialize)]
pub struct ChargeResponse {
    pub id: Uuid,
    pub idempotency_key: String,
    pub customer_id: String,
    pub amount_cents: i64,
    pub currency: String,
    pub created_at: DateTime<Utc>,
    pub replayed: bool,
}

fn idempotency_key(headers: &HeaderMap) -> Result<String, ApiError> {
    let raw = headers
        .get("Idempotency-Key")
        .ok_or_else(|| ApiError::invalid_argument("Idempotency-Key header is required"))?
        .to_str()
        .map_err(|_| ApiError::invalid_argument("Idempotency-Key header must be valid UTF-8"))?
        .trim();
    if raw.is_empty() {
        return Err(ApiError::invalid_argument(
            "Idempotency-Key header must not be empty",
        ));
    }
    Ok(raw.to_string())
}

async fn create_charge(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<ChargeResponse>), ApiError> {
    let key = idempotency_key(&headers)?;
    let payload = parse_charge_payload(&body)?;

    let id = Uuid::new_v4();
    let row = charges::insert_charge(
        &state.db,
        id,
        &key,
        &payload.customer_id,
        payload.amount_cents,
        &payload.currency,
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(ChargeResponse {
            id: row.id,
            idempotency_key: row.idempotency_key,
            customer_id: row.customer_id,
            amount_cents: row.amount_cents,
            currency: row.currency,
            created_at: row.created_at,
            replayed: false,
        }),
    ))
}
