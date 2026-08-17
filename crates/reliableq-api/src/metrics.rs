//! `GET /metrics` (spec sec. 13.2) plus a periodic refresher for the
//! two gauges that reflect whole-table state
//! (`reliableq_job_queue_depth`, `reliableq_oldest_pending_age_seconds`)
//! rather than a single request/attempt.

use std::sync::OnceLock;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use reliableq_core::domain::JobStatus;
use sqlx::PgPool;

use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new().route("/metrics", get(render_metrics))
}

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// A `metrics` global recorder can only be installed once per process.
/// `AppState::new` calls this every time it's constructed (once for
/// real in `main`, many times across a test binary's tests), so the
/// install happens exactly once and every caller gets the same handle.
pub fn recorder_handle() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .expect("failed to install prometheus recorder")
        })
        .clone()
}

async fn render_metrics(State(state): State<AppState>) -> Response {
    (
        [("content-type", "text/plain; version=0.0.4")],
        state.metrics_handle.render(),
    )
        .into_response()
}

const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Runs until the process exits. Never panics on a query failure — a
/// transient DB hiccup here should not take metrics reporting down for
/// longer than one refresh cycle.
pub async fn run_gauge_refresher(pool: PgPool, handle: PrometheusHandle) {
    loop {
        if let Err(err) = refresh_once(&pool).await {
            tracing::warn!(error = %err, "failed to refresh queue-depth gauges");
        }
        // Rendering periodically keeps the exporter's internal
        // descriptor cache warm even if no request has hit /metrics
        // yet; cheap and side-effect-free.
        let _ = handle.render();
        tokio::time::sleep(REFRESH_INTERVAL).await;
    }
}

async fn refresh_once(pool: &PgPool) -> Result<(), sqlx::Error> {
    for status in [
        JobStatus::Pending,
        JobStatus::Running,
        JobStatus::Succeeded,
        JobStatus::Dead,
    ] {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status = $1")
            .bind(status.as_db_str())
            .fetch_one(pool)
            .await?;
        metrics::gauge!("reliableq_job_queue_depth", "status" => status.as_db_str())
            .set(count as f64);
    }

    // Postgres's EXTRACT(EPOCH FROM ...) returns NUMERIC, not
    // double precision, so an explicit cast is required — without it
    // sqlx's f64 decode fails every refresh cycle (caught by actually
    // running this against live postgres, not just compiling it).
    let oldest_pending_age_seconds: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (now() - created_at))::float8 FROM jobs \
         WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    metrics::gauge!("reliableq_oldest_pending_age_seconds")
        .set(oldest_pending_age_seconds.unwrap_or(0.0));

    Ok(())
}
