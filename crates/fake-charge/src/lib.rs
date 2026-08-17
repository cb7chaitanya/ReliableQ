//! fake-charge: a standalone service that records charges and
//! deduplicates by idempotency key. M1 implements the charge endpoint
//! without the dedup/replay behavior yet (see charges.rs doc comment).

pub mod charges;
pub mod error;

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
        .merge(charges::routes())
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
