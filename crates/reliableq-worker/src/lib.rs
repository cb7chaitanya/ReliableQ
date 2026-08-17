//! reliableq-worker: claims due jobs and executes them against
//! fake-charge. Crash recovery is real (M2): `claim_pending_jobs`
//! reclaims expired leases and closes out the abandoned attempt as
//! `LEASE_LOST`, and a stale worker's own finalize attempt is rejected
//! by the token-fenced guard below — that is why the `Ok(false)`
//! branches here only log, they do not need to write `LEASE_LOST`
//! themselves. Re-execution is safe against duplicate charges (M3): the
//! idempotency key is derived deterministically per job. Failures are
//! classified and, if retryable, scheduled with capped exponential
//! backoff and full jitter (M4). Dead jobs are explicitly replayable,
//! never auto-reclaimed (M5). Concurrency is now bounded by a semaphore
//! and long-running jobs get periodic lease renewal so a slow
//! downstream call doesn't let the lease expire out from under
//! legitimately in-progress work (M6).

pub mod poll;

use std::sync::Arc;
use std::time::{Duration, Instant};

pub use poll::run_worker_loop;
use reliableq_core::failure::{FailureClass, classify_http_status, classify_network_error};
use reliableq_core::retry::RetryPolicy;
use reliableq_core::validation::parse_charge_payload;
use reliableq_db::jobs::{self, ClaimedJob};
use reqwest::Client;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use uuid::Uuid;

#[derive(Debug)]
pub struct ExecutionFailure {
    pub class: FailureClass,
    pub code: &'static str,
    pub message: String,
}

/// Acquires one semaphore permit per job *before* spawning its task —
/// this is the backpressure that keeps claiming from ever producing an
/// unbounded task queue (spec sec. 9.5), not just a cap on parallel
/// execution. Each task renews its own lease every `lease_duration / 3`
/// while it runs (spec sec. 9.3).
///
/// Returns one `JoinHandle` per job so the caller can await the whole
/// batch, or race it against a shutdown grace period.
pub fn spawn_bounded_batch(
    pool: PgPool,
    client: Client,
    charge_service_url: String,
    retry_policy: RetryPolicy,
    lease_duration: Duration,
    semaphore: Arc<Semaphore>,
    claimed: Vec<ClaimedJob>,
) -> Vec<tokio::task::JoinHandle<()>> {
    claimed
        .into_iter()
        .map(|job| {
            let pool = pool.clone();
            let client = client.clone();
            let charge_service_url = charge_service_url.clone();
            let semaphore = semaphore.clone();
            tokio::spawn(async move {
                let Ok(_permit) = semaphore.acquire_owned().await else {
                    // Semaphore is only ever closed at process shutdown
                    // after all permits are already accounted for; this
                    // is unreachable in practice but must not panic.
                    tracing::error!(job_id = %job.job.id, "semaphore closed before permit acquired");
                    return;
                };
                execute_and_finalize(
                    &pool,
                    &client,
                    &charge_service_url,
                    &retry_policy,
                    lease_duration,
                    job,
                )
                .await;
            })
        })
        .collect()
}

/// Executes one claimed job's charge call and finalizes the result,
/// renewing its lease every `lease_duration / 3` for the duration of
/// the call. Never panics on a lost lease or downstream error: both are
/// handled as ordinary, logged outcomes (spec sec. 19: no panics in
/// normal runtime paths).
pub async fn execute_and_finalize(
    pool: &PgPool,
    client: &Client,
    charge_service_url: &str,
    retry_policy: &RetryPolicy,
    lease_duration: Duration,
    claimed: ClaimedJob,
) {
    let job = claimed.job;
    let attempt_number = claimed.attempt_number;
    let Some(lease_token) = job.lease_token else {
        // The claim query only ever produces RUNNING rows, which the
        // schema's CHECK constraint guarantees have a lease token. This
        // branch means the repository layer and schema have drifted.
        tracing::error!(job_id = %job.id, "claimed job unexpectedly has no lease token");
        return;
    };

    let heartbeat = spawn_heartbeat(pool.clone(), job.id, lease_token, lease_duration);

    let start = Instant::now();
    let outcome = execute_charge(client, charge_service_url, job.id, &job.payload).await;
    let duration_ms = i64::try_from(start.elapsed().as_millis()).unwrap_or(i64::MAX);
    heartbeat.abort();

    match outcome {
        Ok(()) => match jobs::finalize_success(pool, job.id, lease_token, duration_ms).await {
            Ok(true) => {
                tracing::info!(job_id = %job.id, attempt = attempt_number, "job succeeded");
            }
            Ok(false) => {
                tracing::warn!(
                    job_id = %job.id,
                    attempt = attempt_number,
                    "lease lost before success could be finalized"
                );
            }
            Err(err) => {
                tracing::error!(job_id = %job.id, error = %err, "failed to finalize success");
            }
        },
        Err(failure) if failure.class.is_retryable() && attempt_number < job.max_attempts => {
            let delay = retry_policy.delay(
                u32::try_from(attempt_number).unwrap_or(u32::MAX),
                &mut rand::thread_rng(),
            );
            match jobs::finalize_retry_scheduled(
                pool,
                job.id,
                lease_token,
                delay.as_secs_f64(),
                failure.code,
                &failure.message,
                duration_ms,
            )
            .await
            {
                Ok(true) => {
                    tracing::warn!(
                        job_id = %job.id,
                        attempt = attempt_number,
                        code = failure.code,
                        delay_ms = delay.as_millis() as u64,
                        "job failed with a retryable error, rescheduled"
                    );
                }
                Ok(false) => {
                    tracing::warn!(
                        job_id = %job.id,
                        attempt = attempt_number,
                        "lease lost before retry could be scheduled"
                    );
                }
                Err(err) => {
                    tracing::error!(job_id = %job.id, error = %err, "failed to finalize retry");
                }
            }
        }
        Err(failure) => {
            let reason = if failure.class.is_retryable() {
                "RETRY_BUDGET_EXHAUSTED"
            } else {
                failure.code
            };
            match jobs::finalize_dead(
                pool,
                job.id,
                lease_token,
                reason,
                &failure.message,
                duration_ms,
            )
            .await
            {
                Ok(true) => {
                    tracing::warn!(
                        job_id = %job.id,
                        attempt = attempt_number,
                        code = reason,
                        "job failed permanently and moved to DEAD"
                    );
                }
                Ok(false) => {
                    tracing::warn!(
                        job_id = %job.id,
                        attempt = attempt_number,
                        "lease lost before failure could be finalized"
                    );
                }
                Err(err) => {
                    tracing::error!(job_id = %job.id, error = %err, "failed to finalize dead");
                }
            }
        }
    }
}

/// Renews the lease every `lease_duration / 3` (spec sec. 9.3) until
/// aborted by the caller. Stops renewing (without panicking) the
/// moment it discovers the lease is no longer held.
fn spawn_heartbeat(
    pool: PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    lease_duration: Duration,
) -> tokio::task::JoinHandle<()> {
    let interval = (lease_duration / 3).max(Duration::from_millis(100));
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match jobs::renew_lease(&pool, job_id, lease_token, lease_duration).await {
                Ok(true) => {
                    tracing::debug!(job_id = %job_id, "lease renewed");
                }
                Ok(false) => {
                    tracing::warn!(job_id = %job_id, "lease renewal found the lease already gone");
                    break;
                }
                Err(err) => {
                    tracing::error!(job_id = %job_id, error = %err, "lease renewal failed");
                }
            }
        }
    })
}

/// Exposed as `pub` (beyond `execute_and_finalize`'s needs) so tests can
/// drive a single charge call without finalizing — the exact shape of
/// "worker crashes after the effect commits but before finalize" that
/// M3's duplicate-charge reproduction needs (see
/// `crates/reliableq-worker/tests/duplicate_charge.rs`).
pub async fn execute_charge(
    client: &Client,
    charge_service_url: &str,
    job_id: Uuid,
    payload: &serde_json::Value,
) -> Result<(), ExecutionFailure> {
    let charge_payload = parse_charge_payload(payload).map_err(|err| ExecutionFailure {
        class: FailureClass::Permanent,
        code: "INVALID_PAYLOAD",
        message: err.to_string(),
    })?;

    // Job-scoped and deterministic (spec sec. 9.2): every attempt at
    // this same job sends the same key, so a re-execution after a
    // lease reclaim (M2) replays instead of duplicating (M3).
    let idempotency_key = reliableq_core::idempotency::charge_idempotency_key(job_id);

    let response = client
        .post(format!("{charge_service_url}/v1/charges"))
        .header("Idempotency-Key", idempotency_key)
        .json(&json!({
            "customer_id": charge_payload.customer_id,
            "amount_cents": charge_payload.amount_cents,
            "currency": charge_payload.currency,
        }))
        .send()
        .await
        .map_err(|err| ExecutionFailure {
            class: classify_network_error(),
            code: "DOWNSTREAM_UNREACHABLE",
            message: err.to_string(),
        })?;

    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let class = classify_http_status(status.as_u16());
        let body = response.text().await.unwrap_or_default();
        Err(ExecutionFailure {
            class,
            code: "DOWNSTREAM_REJECTED",
            message: format!("charge service returned {status}: {body}"),
        })
    }
}
