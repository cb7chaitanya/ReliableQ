//! Deterministic failure/latency injection for fake-charge (spec sec.
//! 12). Disabled by default; only mounted when
//! `FAKE_CHARGE_ENABLE_TEST_CONTROL` is explicitly set, which
//! production deployments must never set. The [`ChaosState`] itself
//! always exists on [`crate::AppState`] (so the charge handler has one
//! code path regardless), but with the control route unmounted it can
//! only ever be in [`ChaosMode::Normal`] and in-flight tracking is
//! harmless overhead (two atomics).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;

#[derive(Debug, Clone, Default)]
pub struct ChaosState {
    inner: Arc<Mutex<ChaosMode>>,
    inflight: Arc<AtomicUsize>,
    peak_inflight: Arc<AtomicUsize>,
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
    /// Persists (not consumed) until reset: every request sleeps this
    /// long before proceeding. Used to simulate a slow downstream so
    /// concurrent load is actually concurrent for long enough to
    /// measure (M6).
    DelayMs(u64),
}

/// Outcome of consulting chaos state for one request: either let the
/// request proceed normally, or short-circuit with this status.
pub enum ChaosDecision {
    Proceed,
    Reject(StatusCode),
}

impl ChaosState {
    /// Consumes one unit of injected failure, if any is configured.
    /// Also sleeps if `DelayMs` is configured (not consumed).
    pub async fn decide(&self) -> ChaosDecision {
        let delay = {
            let mode = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            match *mode {
                ChaosMode::DelayMs(ms) => Some(ms),
                _ => None,
            }
        };
        if let Some(ms) = delay {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }

        let mut mode = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match *mode {
            ChaosMode::Normal | ChaosMode::DelayMs(_) => ChaosDecision::Proceed,
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

    /// Marks the start of a request; returns a guard that marks its end
    /// on drop. Tracks the high-water mark of concurrent in-flight
    /// requests for M6's bounded-concurrency evidence.
    pub fn track_inflight(&self) -> InflightGuard {
        let current = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_inflight.fetch_max(current, Ordering::SeqCst);
        InflightGuard {
            counter: self.inflight.clone(),
        }
    }

    fn peak_inflight(&self) -> usize {
        self.peak_inflight.load(Ordering::SeqCst)
    }

    fn reset_peak_inflight(&self) {
        self.peak_inflight.store(0, Ordering::SeqCst);
    }
}

pub struct InflightGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum ControlRequest {
    Normal,
    FailNext { n: u32, status: u16 },
    PermanentReject,
    DelayMs { ms: u64 },
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
        ControlRequest::DelayMs { ms } => ChaosMode::DelayMs(ms),
    };
    state.chaos.set(mode);
    Ok(Json(ControlResponse { ok: true }))
}

#[derive(Debug, Serialize)]
struct InflightResponse {
    peak_inflight: usize,
}

async fn get_peak_inflight(State(state): State<AppState>) -> Json<InflightResponse> {
    Json(InflightResponse {
        peak_inflight: state.chaos.peak_inflight(),
    })
}

async fn reset_peak_inflight(State(state): State<AppState>) -> Json<ControlResponse> {
    state.chaos.reset_peak_inflight();
    Json(ControlResponse { ok: true })
}

/// Only call this when test control is explicitly enabled (never in
/// production — see this module's doc comment).
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/test/control", post(set_control))
        .route("/v1/test/inflight", get(get_peak_inflight))
        .route("/v1/test/inflight/reset", post(reset_peak_inflight))
}
