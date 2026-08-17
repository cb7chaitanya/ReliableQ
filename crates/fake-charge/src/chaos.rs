//! Deterministic failure injection for fake-charge (spec sec. 12).
//! Disabled by default; only mounted when `FAKE_CHARGE_ENABLE_TEST_CONTROL`
//! is explicitly set, which production deployments must never set. The
//! [`ChaosState`] itself always exists on [`crate::AppState`] (so the
//! charge handler has one code path regardless), but with the control
//! route unmounted it can only ever be in [`ChaosMode::Normal`].

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;

#[derive(Debug, Clone, Default)]
pub struct ChaosState {
    inner: Arc<Mutex<ChaosMode>>,
}

#[derive(Debug, Clone, Copy, Default)]
enum ChaosMode {
    #[default]
    Normal,
    FailNext {
        remaining: u32,
        status: u16,
    },
    PermanentReject,
}

/// Outcome of consulting chaos state for one request: either let the
/// request proceed normally, or short-circuit with this status.
pub enum ChaosDecision {
    Proceed,
    Reject(StatusCode),
}

impl ChaosState {
    /// Consumes one unit of injected failure, if any is configured.
    pub fn decide(&self) -> ChaosDecision {
        let mut mode = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match *mode {
            ChaosMode::Normal => ChaosDecision::Proceed,
            ChaosMode::PermanentReject => ChaosDecision::Reject(StatusCode::UNPROCESSABLE_ENTITY),
            ChaosMode::FailNext { remaining, status } => {
                let next_remaining = remaining.saturating_sub(1);
                *mode = if next_remaining == 0 {
                    ChaosMode::Normal
                } else {
                    ChaosMode::FailNext {
                        remaining: next_remaining,
                        status,
                    }
                };
                let status_code =
                    StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
                ChaosDecision::Reject(status_code)
            }
        }
    }

    fn set(&self, mode: ChaosMode) {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner()) = mode;
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum ControlRequest {
    Normal,
    FailNext { n: u32, status: u16 },
    PermanentReject,
}

#[derive(Debug, Serialize)]
struct ControlResponse {
    ok: bool,
}

async fn set_control(
    State(state): State<AppState>,
    Json(body): Json<ControlRequest>,
) -> Result<Json<ControlResponse>, ApiError> {
    let mode = match body {
        ControlRequest::Normal => ChaosMode::Normal,
        ControlRequest::FailNext { n, status } => {
            if n == 0 {
                return Err(ApiError::invalid_argument("n must be at least 1"));
            }
            ChaosMode::FailNext {
                remaining: n,
                status,
            }
        }
        ControlRequest::PermanentReject => ChaosMode::PermanentReject,
    };
    state.chaos.set(mode);
    Ok(Json(ControlResponse { ok: true }))
}

/// Only call this when test control is explicitly enabled (never in
/// production — see this module's doc comment).
pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/test/control", post(set_control))
}
