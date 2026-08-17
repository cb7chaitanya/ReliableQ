//! Read-only access to the append-only `job_attempts` audit trail.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::jobs::RepoError;

#[derive(Debug, Clone, FromRow)]
pub struct AttemptRow {
    pub id: Uuid,
    pub job_id: Uuid,
    pub attempt_number: i32,
    pub worker_id: String,
    pub lease_token: Option<Uuid>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub outcome: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub scheduled_delay_ms: Option<i64>,
    pub duration_ms: Option<i64>,
}

pub async fn list_for_job(pool: &PgPool, job_id: Uuid) -> Result<Vec<AttemptRow>, RepoError> {
    let rows = sqlx::query_as::<_, AttemptRow>(
        "SELECT * FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number ASC",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
