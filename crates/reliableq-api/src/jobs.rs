//! `POST /v1/jobs`, `GET /v1/jobs/{id}`, `GET /v1/jobs` (spec sec. 8.1,
//! 8.2). Only the "charge" job kind is validated/executed in this
//! project (see reliableq-core::validation), so submission validates the
//! payload against that shape regardless of the free-text `kind` field.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use reliableq_core::domain::JobStatus;
use reliableq_core::validation::{parse_charge_payload, validate_kind, validate_max_attempts};
use reliableq_db::attempts::{self, AttemptRow};
use reliableq_db::jobs::{self, JobRow};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/jobs", post(submit_job).get(list_jobs))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/jobs/{id}/retry", post(retry_job))
        .route("/v1/dead-jobs", get(list_dead_jobs))
}

const DEFAULT_MAX_ATTEMPTS: i32 = 5;
const DEFAULT_LIST_LIMIT: i64 = 50;
const MAX_LIST_LIMIT: i64 = 200;
/// Additional attempts granted by a bare `POST /v1/jobs/{id}/retry`
/// with no `max_attempts` override in the body.
const DEFAULT_RETRY_BUDGET_INCREMENT: i32 = 5;

fn default_max_attempts() -> i32 {
    DEFAULT_MAX_ATTEMPTS
}

#[derive(Debug, Deserialize)]
pub struct SubmitJobRequest {
    pub kind: String,
    pub payload: serde_json::Value,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
}

#[derive(Debug, Serialize)]
pub struct SubmitJobResponse {
    pub id: Uuid,
    pub status: JobStatus,
    pub attempts: i32,
    pub max_attempts: i32,
    pub created_at: DateTime<Utc>,
}

/// Never includes `lease_token`/`lease_expires_at` (spec sec. 8.2: "Do
/// not expose lease tokens").
#[derive(Debug, Serialize)]
pub struct JobSummary {
    pub id: Uuid,
    pub kind: String,
    pub payload: serde_json::Value,
    pub status: JobStatus,
    pub attempts: i32,
    pub max_attempts: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub version: i64,
}

impl TryFrom<JobRow> for JobSummary {
    type Error = ApiError;

    fn try_from(row: JobRow) -> Result<Self, ApiError> {
        let status = row.status()?;
        Ok(Self {
            id: row.id,
            kind: row.kind,
            payload: row.payload,
            status,
            attempts: row.attempts,
            max_attempts: row.max_attempts,
            next_attempt_at: row.next_attempt_at,
            last_error_code: row.last_error_code,
            last_error_message: row.last_error_message,
            created_at: row.created_at,
            updated_at: row.updated_at,
            started_at: row.started_at,
            finished_at: row.finished_at,
            version: row.version,
        })
    }
}

/// Never includes `lease_token` (spec sec. 8.2).
#[derive(Debug, Serialize)]
pub struct AttemptSummary {
    pub attempt_number: i32,
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub outcome: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub scheduled_delay_ms: Option<i64>,
    pub duration_ms: Option<i64>,
}

impl From<AttemptRow> for AttemptSummary {
    fn from(row: AttemptRow) -> Self {
        Self {
            attempt_number: row.attempt_number,
            worker_id: row.worker_id,
            started_at: row.started_at,
            finished_at: row.finished_at,
            outcome: row.outcome,
            error_code: row.error_code,
            error_message: row.error_message,
            scheduled_delay_ms: row.scheduled_delay_ms,
            duration_ms: row.duration_ms,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct JobDetail {
    #[serde(flatten)]
    pub job: JobSummary,
    pub attempts: Vec<AttemptSummary>,
}

async fn submit_job(
    State(state): State<AppState>,
    Json(body): Json<SubmitJobRequest>,
) -> Result<(StatusCode, Json<SubmitJobResponse>), ApiError> {
    validate_kind(&body.kind)?;
    validate_max_attempts(body.max_attempts)?;
    parse_charge_payload(&body.payload)?;

    let id = Uuid::new_v4();
    let row = jobs::insert_job(&state.db, id, &body.kind, &body.payload, body.max_attempts).await?;
    let status = row.status()?;
    metrics::counter!("reliableq_jobs_submitted_total", "kind" => row.kind.clone()).increment(1);

    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitJobResponse {
            id: row.id,
            status,
            attempts: row.attempts,
            max_attempts: row.max_attempts,
            created_at: row.created_at,
        }),
    ))
}

async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobDetail>, ApiError> {
    let row = jobs::get_job_by_id(&state.db, id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("job {id} not found")))?;
    let attempt_rows = attempts::list_for_job(&state.db, id).await?;

    let job = JobSummary::try_from(row)?;
    let attempts = attempt_rows.into_iter().map(AttemptSummary::from).collect();

    Ok(Json(JobDetail { job, attempts }))
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListJobsResponse {
    pub items: Vec<JobSummary>,
    pub next_cursor: Option<String>,
}

async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<ListJobsResponse>, ApiError> {
    let status = match &query.status {
        Some(raw) => Some(
            JobStatus::parse_db_str(raw)
                .ok_or_else(|| ApiError::invalid_argument(format!("invalid status {raw:?}")))?,
        ),
        None => None,
    };
    list_jobs_with_status(&state, status, &query).await
}

/// `GET /v1/dead-jobs` (spec sec. 8.2): "a convenience view equivalent
/// to filtering DEAD." Same pagination as `GET /v1/jobs`, status fixed.
async fn list_dead_jobs(
    State(state): State<AppState>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<ListJobsResponse>, ApiError> {
    list_jobs_with_status(&state, Some(JobStatus::Dead), &query).await
}

async fn list_jobs_with_status(
    state: &AppState,
    status: Option<JobStatus>,
    query: &ListJobsQuery,
) -> Result<Json<ListJobsResponse>, ApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
    if !(1..=MAX_LIST_LIMIT).contains(&limit) {
        return Err(ApiError::invalid_argument(format!(
            "limit must be between 1 and {MAX_LIST_LIMIT}"
        )));
    }

    let after = query.cursor.as_deref().map(decode_cursor).transpose()?;

    let rows = jobs::list_jobs(&state.db, status, limit, after).await?;
    let next_cursor = if rows.len() as i64 == limit {
        rows.last().map(|row| encode_cursor(row.created_at, row.id))
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(JobSummary::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(ListJobsResponse { items, next_cursor }))
}

#[derive(Debug, Deserialize)]
pub struct RetryJobRequest {
    pub max_attempts: Option<i32>,
}

async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    body: Option<Json<RetryJobRequest>>,
) -> Result<Json<JobSummary>, ApiError> {
    let existing = jobs::get_job_by_id(&state.db, id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("job {id} not found")))?;
    let existing_status = existing.status()?;
    if existing_status != JobStatus::Dead {
        return Err(ApiError::invalid_state(format!(
            "job {id} is {existing_status}, not DEAD; only a DEAD job can be retried"
        )));
    }

    let new_max_attempts = match body.and_then(|Json(b)| b.max_attempts) {
        Some(requested) => {
            validate_max_attempts(requested)?;
            if requested <= existing.attempts {
                return Err(ApiError::invalid_argument(format!(
                    "max_attempts ({requested}) must be greater than the job's existing attempts ({})",
                    existing.attempts
                )));
            }
            requested
        }
        None => existing.attempts + DEFAULT_RETRY_BUDGET_INCREMENT,
    };

    let row = jobs::retry_dead_job(&state.db, id, new_max_attempts)
        .await?
        .ok_or_else(|| {
            ApiError::invalid_state(format!(
                "job {id} was no longer DEAD by the time the retry was applied"
            ))
        })?;

    Ok(Json(JobSummary::try_from(row)?))
}

fn encode_cursor(created_at: DateTime<Utc>, id: Uuid) -> String {
    let raw = format!("{}|{}", created_at.to_rfc3339(), id);
    URL_SAFE_NO_PAD.encode(raw)
}

fn decode_cursor(cursor: &str) -> Result<(DateTime<Utc>, Uuid), ApiError> {
    let invalid = || ApiError::invalid_argument("invalid cursor");
    let raw = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| invalid())?;
    let raw = String::from_utf8(raw).map_err(|_| invalid())?;
    let (created_at_raw, id_raw) = raw.split_once('|').ok_or_else(invalid)?;
    let created_at = DateTime::parse_from_rfc3339(created_at_raw)
        .map_err(|_| invalid())?
        .with_timezone(&Utc);
    let id = Uuid::parse_str(id_raw).map_err(|_| invalid())?;
    Ok((created_at, id))
}
