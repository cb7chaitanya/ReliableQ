//! `/health/live` and `/health/ready` (spec sec. 8.4).
//!
//! Liveness never touches PostgreSQL: it only proves the process' async
//! runtime is scheduling tasks. Readiness does touch PostgreSQL, because a
//! process that is alive but cannot reach its database should be taken out
//! of a load balancer's rotation.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
}

async fn live() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn ready(State(state): State<AppState>) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match sqlx::query("select 1").execute(&state.db).await {
        Ok(_) => Ok(Json(json!({ "status": "ok" }))),
        Err(err) => {
            tracing::warn!(error = %err, "readiness check failed: database unreachable");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable", "reason": "database unreachable" })),
            ))
        }
    }
}
