//! fake-charge: a standalone service that records charges and
//! deduplicates by idempotency key (see charges.rs doc comment).
//! Deterministic failure injection (spec sec. 12) is available via
//! chaos.rs but its HTTP control route is only mounted when
//! `enable_test_control` is explicitly passed — never in production.

pub mod chaos;
pub mod charges;
pub mod error;

use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::{BoxError, Router};
use sqlx::PgPool;
use tower::ServiceBuilder;
use tower::timeout::TimeoutLayer;

use crate::chaos::ChaosState;
use crate::error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub chaos: ChaosState,
}

pub fn build_app(
    state: AppState,
    max_body_bytes: usize,
    request_timeout: Duration,
    enable_test_control: bool,
) -> Router {
    let timeout = ServiceBuilder::new()
        .layer(axum::error_handling::HandleErrorLayer::new(
            handle_timeout_error,
        ))
        .layer(TimeoutLayer::new(request_timeout));

    let mut router = Router::new().merge(charges::routes());
    if enable_test_control {
        router = router.merge(chaos::routes());
    }

    router
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
