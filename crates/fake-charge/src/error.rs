//! Same stable error envelope as reliableq-api (spec sec. 8). Kept as a
//! small independent copy rather than a shared crate: SPEC.md sec. 6
//! fixes the five-crate layout, and this is the only place fake-charge
//! needs it.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Debug, Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
    request_id: String,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "INVALID_ARGUMENT", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: ErrorDetail {
                code: self.code,
                message: self.message,
                request_id: Uuid::new_v4().to_string(),
            },
        };
        (self.status, Json(body)).into_response()
    }
}

impl From<reliableq_db::jobs::RepoError> for ApiError {
    fn from(err: reliableq_db::jobs::RepoError) -> Self {
        tracing::error!(error = %err, "repository error");
        ApiError::internal("internal error")
    }
}

impl From<reliableq_core::validation::ValidationError> for ApiError {
    fn from(err: reliableq_core::validation::ValidationError) -> Self {
        ApiError::invalid_argument(err.to_string())
    }
}
