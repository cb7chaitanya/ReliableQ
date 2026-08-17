//! reliableq-api: validates submissions, persists jobs, and exposes
//! read/list/retry endpoints, health probes, and Prometheus metrics.
//!
//! Job submission/inspection/retry handlers land here in M1/M5; this
//! module currently wires up the app skeleton and health probes only.

pub mod health;

use axum::Router;
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
}

pub fn build_app(state: AppState) -> Router {
    Router::new().merge(health::routes()).with_state(state)
}
