//! Job and job_attempts repository functions.
//!
//! Claiming (`claim_pending_jobs`) covers both due `PENDING` jobs and
//! expired-lease `RUNNING` jobs (M2): abandoned attempts are closed out
//! as `LEASE_LOST`, exhausted-budget expired leases move straight to
//! `DEAD` instead of being stranded, and eligible rows are reclaimed
//! with a fresh lease token, still under `FOR UPDATE SKIP LOCKED` so
//! racing workers never double-claim. Retryable failures are scheduled
//! back to `PENDING` with backoff (M4, `finalize_retry_scheduled`).
//! `retry_dead_job` (M5) is the only path back from `DEAD`, reserved
//! for explicit operator action — nothing here reclaims a `DEAD` job
//! automatically. See `docs/failure-lab.md`.

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
    /// `true` if this claim reclaimed an expired `RUNNING` lease (M2)
    /// rather than claiming a due `PENDING` job — lets the caller
    /// record `reliableq_lease_expirations_reclaimed_total` (spec sec.
    /// 13.2) without the repository layer taking on metrics recording
    /// itself.
    pub was_reclaimed: bool,
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
    // two workers racing on the same row never both win. `was_reclaimed`
    // records which branch a row came from, so the caller can count
    // reliableq_lease_expirations_reclaimed_total (spec sec. 13.2)
    // without the repository layer taking on metrics recording itself.
    let claimed = sqlx::query_as::<_, ClaimRow>(
        r#"
        WITH due AS (
            SELECT id, status FROM jobs
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
        RETURNING jobs.*, (due.status = 'RUNNING') AS was_reclaimed
        "#,
    )
    .bind(limit)
    .bind(lease_duration.as_secs_f64())
    .fetch_all(&mut *tx)
    .await?;

    let mut results = Vec::with_capacity(claimed.len());
    for claimed_row in claimed {
        let job = claimed_row.job;
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
            was_reclaimed: claimed_row.was_reclaimed,
            job,
        });
    }

    tx.commit().await?;
    Ok(results)
}

#[derive(Debug, FromRow)]
struct ClaimRow {
    #[sqlx(flatten)]
    job: JobRow,
    was_reclaimed: bool,
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

/// Schedules a retryable failure back onto the queue: `RUNNING ->
/// PENDING` with `next_attempt_at` computed from **database** time plus
/// `delay_seconds` (spec sec. 19: schedule against database time, not
/// worker wall-clock, so clock skew between hosts cannot matter), lease
/// cleared, sanitized error recorded. Returns `false` under the same
/// fencing rule as [`finalize_success`].
pub async fn finalize_retry_scheduled(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    delay_seconds: f64,
    error_code: &str,
    error_message: &str,
    duration_ms: i64,
) -> Result<bool, RepoError> {
    let mut tx = pool.begin().await?;
    let scheduled_delay_ms = (delay_seconds * 1000.0).round() as i64;

    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'PENDING',
            lease_token = NULL,
            lease_expires_at = NULL,
            next_attempt_at = now() + make_interval(secs => $3),
            last_error_code = $4,
            last_error_message = $5,
            updated_at = now()
        WHERE id = $1 AND status = 'RUNNING' AND lease_token = $2
        "#,
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(delay_seconds)
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
        SET outcome = 'RETRY_SCHEDULED',
            finished_at = now(),
            error_code = $3,
            error_message = $4,
            scheduled_delay_ms = $5,
            duration_ms = $6
        WHERE job_id = $1 AND lease_token = $2
        "#,
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(error_code)
    .bind(error_message)
    .bind(scheduled_delay_ms)
    .bind(duration_ms)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

/// Finalizes a job as permanently `DEAD`, whether from a permanent
/// failure or an exhausted retry budget. Returns `false` under the same
/// fencing rule as [`finalize_success`].
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

/// Explicit operator replay of a `DEAD` job (spec sec. 8.3): resets
/// status/scheduling/lease/error/terminal-timestamp, but preserves
/// `attempts` (and therefore the full historical `job_attempts` audit
/// trail with monotonic numbering — the next claim continues counting
/// from where it left off, not from zero) and reuses the same job ID
/// (and therefore the same downstream idempotency key). Guarded by
/// `WHERE status = 'DEAD'`; returns `None` if the job does not exist or
/// is not currently `DEAD` (the caller distinguishes 404 vs 409).
pub async fn retry_dead_job(
    pool: &PgPool,
    job_id: Uuid,
    new_max_attempts: i32,
) -> Result<Option<JobRow>, RepoError> {
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE jobs
        SET status = 'PENDING',
            next_attempt_at = now(),
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error_code = NULL,
            last_error_message = NULL,
            finished_at = NULL,
            max_attempts = $2,
            updated_at = now()
        WHERE id = $1 AND status = 'DEAD'
        RETURNING *
        "#,
    )
    .bind(job_id)
    .bind(new_max_attempts)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Renews a lease mid-execution (spec sec. 9.3: "Heartbeat every
/// one-third of the lease duration"), guarded by the same
/// `status = 'RUNNING' AND lease_token = $2` fencing predicate as
/// every other lease-touching statement. Returns `false` if the guard
/// matched zero rows — the caller has already lost ownership (someone
/// else reclaimed it after the lease expired) and must stop working.
pub async fn renew_lease(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    lease_duration: Duration,
) -> Result<bool, RepoError> {
    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET lease_expires_at = now() + make_interval(secs => $3),
            updated_at = now()
        WHERE id = $1 AND status = 'RUNNING' AND lease_token = $2
        "#,
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(lease_duration.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}
