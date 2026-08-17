//! reliableq-api: validates submissions, persists jobs, and exposes
//! read/list/retry endpoints, health probes, and Prometheus metrics.
//!
//! The retry endpoint (sec. 8.3) and `/metrics` (sec. 13.2) land in
//! M5/M7; this crate currently wires up submit/get/list, health probes,
//! and app-level body-size/timeout bounds.

pub mod error;
pub mod health;
pub mod jobs;

use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::{BoxError, Router};
use sqlx::PgPool;
use tower::ServiceBuilder;
use tower::timeout::TimeoutLayer;

use crate::error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
}

pub fn build_app(state: AppState, max_body_bytes: usize, request_timeout: Duration) -> Router {
    let timeout = ServiceBuilder::new()
        .layer(axum::error_handling::HandleErrorLayer::new(
            handle_timeout_error,
        ))
        .layer(TimeoutLayer::new(request_timeout));

    Router::new()
        .merge(health::routes())
        .merge(jobs::routes())
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(timeout)
        .with_state(state)
}

async fn handle_timeout_error(err: BoxError) -> ApiError {
    if err.is::<tower::timeout::error::Elapsed>() {
        ApiError::new(StatusCode::REQUEST_TIMEOUT, "TIMEOUT", "request timed out")
    } else {
        ApiError::internal(err.to_string())
    }
}
