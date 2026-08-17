//! reliableq-api: validates submissions, persists jobs, and exposes
//! read/list/retry endpoints, health probes, and Prometheus metrics.

pub mod correlation;
pub mod error;
pub mod health;
pub mod jobs;
pub mod metrics;

use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::{BoxError, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::PgPool;
use tower::ServiceBuilder;
use tower::timeout::TimeoutLayer;

use crate::error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub metrics_handle: PrometheusHandle,
}

impl AppState {
    pub fn new(db: PgPool) -> Self {
        Self {
            db,
            metrics_handle: metrics::recorder_handle(),
        }
    }
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
        .merge(metrics::routes())
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(timeout)
        .layer(axum::middleware::from_fn(correlation::middleware))
        .with_state(state)
}

async fn handle_timeout_error(err: BoxError) -> ApiError {
    if err.is::<tower::timeout::error::Elapsed>() {
        ApiError::new(StatusCode::REQUEST_TIMEOUT, "TIMEOUT", "request timed out")
    } else {
        ApiError::internal(err.to_string())
    }
}
