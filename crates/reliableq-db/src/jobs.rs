//! Job and job_attempts repository functions.
//!
//! Claiming (`claim_pending_jobs`) covers both due `PENDING` jobs and
//! expired-lease `RUNNING` jobs (M2): abandoned attempts are closed out
//! as `LEASE_LOST`, exhausted-budget expired leases move straight to
//! `DEAD` instead of being stranded, and eligible rows are reclaimed
//! with a fresh lease token, still under `FOR UPDATE SKIP LOCKED` so
//! racing workers never double-claim. Retry scheduling (M4) and
//! dead-job replay (M5) are not implemented yet — see
//! `docs/failure-lab.md`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use reliableq_core::domain::JobStatus;
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("database state inconsistent with application invariants: {0}")]
    Inconsistent(String),
}

#[derive(Debug, Clone, FromRow)]
pub struct JobRow {
    pub id: Uuid,
    pub kind: String,
    pub payload: Value,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_token: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub version: i64,
}

impl JobRow {
    /// The `status` column is `text` with a CHECK constraint, not a
    /// native enum, so decoding into [`JobStatus`] can fail only if the
    /// database and this binary's domain model have drifted; callers
    /// must handle that as a real (if practically unreachable) error
    /// rather than a panic (SPEC.md sec. 19: no `unwrap`/`expect` in
    /// normal runtime paths).
    pub fn status(&self) -> Result<JobStatus, RepoError> {
        JobStatus::parse_db_str(&self.status).ok_or_else(|| {
            RepoError::Inconsistent(format!("unexpected job status {:?}", self.status))
        })
    }
}

/// A job row claimed by this worker, plus the attempt number just
/// recorded for it (equal to the job's post-claim `attempts` value).
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub job: JobRow,
    pub attempt_number: i32,
}

pub async fn insert_job(
    pool: &PgPool,
    id: Uuid,
    kind: &str,
    payload: &Value,
    max_attempts: i32,
) -> Result<JobRow, RepoError> {
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        INSERT INTO jobs (id, kind, payload, status, attempts, max_attempts, next_attempt_at)
        VALUES ($1, $2, $3, 'PENDING', 0, $4, now())
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(kind)
    .bind(payload)
    .bind(max_attempts)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub async fn get_job_by_id(pool: &PgPool, id: Uuid) -> Result<Option<JobRow>, RepoError> {
    let row = sqlx::query_as::<_, JobRow>("SELECT * FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

/// Stable cursor pagination ordered by `(created_at, id)` ascending,
/// matching `idx_jobs_status_created_at`. `after` is the `(created_at,
/// id)` of the last row of the previous page.
pub async fn list_jobs(
    pool: &PgPool,
    status: Option<JobStatus>,
    limit: i64,
    after: Option<(DateTime<Utc>, Uuid)>,
) -> Result<Vec<JobRow>, RepoError> {
    let status_str = status.map(JobStatus::as_db_str);
    let (after_created_at, after_id) = match after {
        Some((created_at, id)) => (Some(created_at), Some(id)),
        None => (None, None),
    };

    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        SELECT * FROM jobs
        WHERE ($1::text IS NULL OR status = $1)
          AND (
                $2::timestamptz IS NULL
                OR (created_at, id) > ($2, $3)
              )
        ORDER BY created_at ASC, id ASC
        LIMIT $4
        "#,
    )
    .bind(status_str)
    .bind(after_created_at)
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Atomically claims up to `limit` due `PENDING` jobs, transitions them
/// to `RUNNING` with a fresh lease, and records the attempt row for
/// each — all in one short transaction with no network call inside it
/// (invariant 13). There is no expired-`RUNNING` reclaim yet: a worker
/// that crashes after this commits leaves the job stranded until M2.
pub async fn claim_pending_jobs(
    pool: &PgPool,
    worker_id: &str,
    limit: i64,
    lease_duration: Duration,
) -> Result<Vec<ClaimedJob>, RepoError> {
    let mut tx = pool.begin().await?;

    // Step 1: close out any dangling attempt whose lease has expired
    // without finalization. This runs whether the job below turns out
    // to be reclaimable or exhausted, because from that attempt's own
    // point of view its lease was simply lost either way.
    sqlx::query(
        r#"
        UPDATE job_attempts
        SET outcome = 'LEASE_LOST', finished_at = now()
        WHERE outcome IS NULL
          AND job_id IN (
              SELECT id FROM jobs
              WHERE status = 'RUNNING' AND lease_expires_at <= now()
          )
        "#,
    )
    .execute(&mut *tx)
    .await?;

    // Step 2: a job whose lease expired *and* has no attempts left can
    // never be claimed again by step 3's guard below — without this it
    // would stay RUNNING with an expired lease forever. Move it to DEAD
    // now instead of leaving it stranded (spec sec. 9.1 point 2).
    sqlx::query(
        r#"
        WITH exhausted AS (
            SELECT id FROM jobs
            WHERE status = 'RUNNING'
              AND lease_expires_at <= now()
              AND attempts >= max_attempts
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs
        SET status = 'DEAD',
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error_code = 'LEASE_EXPIRED_BUDGET_EXHAUSTED',
            last_error_message = 'lease expired after exhausting max_attempts with no successful finalize',
            finished_at = now(),
            updated_at = now()
        FROM exhausted
        WHERE jobs.id = exhausted.id
        "#,
    )
    .execute(&mut *tx)
    .await?;

    // Step 3: claim due PENDING jobs and reclaim expired RUNNING jobs
    // that still have budget, in one guarded, row-locked statement so
    // two workers racing on the same row never both win.
    let claimed = sqlx::query_as::<_, JobRow>(
        r#"
        WITH due AS (
            SELECT id FROM jobs
            WHERE (
                    (status = 'PENDING' AND next_attempt_at <= now())
                 OR (status = 'RUNNING' AND lease_expires_at <= now())
                  )
              AND attempts < max_attempts
            ORDER BY next_attempt_at, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        )
        UPDATE jobs
        SET status = 'RUNNING',
            attempts = jobs.attempts + 1,
            lease_token = gen_random_uuid(),
            lease_expires_at = now() + make_interval(secs => $2),
            started_at = COALESCE(jobs.started_at, now()),
            updated_at = now()
        FROM due
        WHERE jobs.id = due.id
        RETURNING jobs.*
        "#,
    )
    .bind(limit)
    .bind(lease_duration.as_secs_f64())
    .fetch_all(&mut *tx)
    .await?;

    let mut results = Vec::with_capacity(claimed.len());
    for job in claimed {
        sqlx::query(
            r#"
            INSERT INTO job_attempts (job_id, attempt_number, worker_id, lease_token, started_at)
            VALUES ($1, $2, $3, $4, now())
            "#,
        )
        .bind(job.id)
        .bind(job.attempts)
        .bind(worker_id)
        .bind(job.lease_token)
        .execute(&mut *tx)
        .await?;

        results.push(ClaimedJob {
            attempt_number: job.attempts,
            job,
        });
    }

    tx.commit().await?;
    Ok(results)
}

/// Finalizes a successful attempt. Returns `false` if the guarded
/// update matched zero rows (the caller no longer holds the lease),
/// meaning the caller must not treat this as success.
pub async fn finalize_success(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    duration_ms: i64,
) -> Result<bool, RepoError> {
    let mut tx = pool.begin().await?;

    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'SUCCEEDED',
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error_code = NULL,
            last_error_message = NULL,
            finished_at = now(),
            updated_at = now()
        WHERE id = $1 AND status = 'RUNNING' AND lease_token = $2
        "#,
    )
    .bind(job_id)
    .bind(lease_token)
    .execute(&mut *tx)
    .await?;

    if result.rows_affected() == 0 {
        tx.rollback().await?;
        return Ok(false);
    }

    sqlx::query(
        r#"
        UPDATE job_attempts
        SET outcome = 'SUCCEEDED', finished_at = now(), duration_ms = $3
        WHERE job_id = $1 AND lease_token = $2
        "#,
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(duration_ms)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

/// Finalizes a job as permanently `DEAD`. M1 has no retry policy: every
/// execution failure lands here directly (see `docs/failure-lab.md` M1
/// entry). Returns `false` under the same fencing rule as
/// [`finalize_success`].
pub async fn finalize_dead(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    error_code: &str,
    error_message: &str,
    duration_ms: i64,
) -> Result<bool, RepoError> {
    let mut tx = pool.begin().await?;

    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'DEAD',
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error_code = $3,
            last_error_message = $4,
            finished_at = now(),
            updated_at = now()
        WHERE id = $1 AND status = 'RUNNING' AND lease_token = $2
        "#,
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(error_code)
    .bind(error_message)
    .execute(&mut *tx)
    .await?;

    if result.rows_affected() == 0 {
        tx.rollback().await?;
        return Ok(false);
    }

    sqlx::query(
        r#"
        UPDATE job_attempts
        SET outcome = 'DEAD', finished_at = now(), error_code = $3, error_message = $4, duration_ms = $5
        WHERE job_id = $1 AND lease_token = $2
        "#,
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(error_code)
    .bind(error_message)
    .bind(duration_ms)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}
