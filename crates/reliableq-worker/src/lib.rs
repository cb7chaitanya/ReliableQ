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
//! never auto-reclaimed (M5). Concurrency is bounded by a semaphore and
//! long-running jobs get periodic lease renewal (M6). Every log line
//! that touches a lease logs its redacted hash, never the raw token
//! (spec sec. 13.1), and every terminal transition is reflected in a
//! Prometheus metric (spec sec. 13.2, M7).

pub mod failpoint;
pub mod metrics_server;
pub mod poll;

use std::sync::Arc;
use std::time::{Duration, Instant};

use failpoint::{Failpoints, NoopFailpoints};
pub use poll::run_worker_loop;
use reliableq_core::failure::{FailureClass, classify_http_status, classify_network_error};
use reliableq_core::redact::lease_token_hash;
use reliableq_core::retry::RetryPolicy;
use reliableq_core::validation::parse_charge_payload;
use reliableq_db::jobs::{self, ClaimedJob};
use reqwest::Client;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tracing::Instrument;
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
    // Spawned tasks don't inherit the caller's tracing span
    // automatically; capturing it here and instrumenting each task is
    // what keeps `worker_id` (attached to run_worker_loop's span) on
    // every log line the task emits (spec sec. 13.1).
    let span = tracing::Span::current();
    claimed
        .into_iter()
        .map(|job| {
            let pool = pool.clone();
            let client = client.clone();
            let charge_service_url = charge_service_url.clone();
            let semaphore = semaphore.clone();
            let span = span.clone();
            tokio::spawn(
                async move {
                    if job.was_reclaimed {
                        metrics::counter!("reliableq_lease_expirations_reclaimed_total")
                            .increment(1);
                    }
                    let Ok(_permit) = semaphore.acquire_owned().await else {
                        // Semaphore is only ever closed at process
                        // shutdown after all permits are already
                        // accounted for; unreachable in practice but
                        // must not panic.
                        tracing::error!(job_id = %job.job.id, "semaphore closed before permit acquired");
                        return;
                    };
                    metrics::gauge!("reliableq_inflight_jobs").increment(1.0);
                    execute_and_finalize(
                        &pool,
                        &client,
                        &charge_service_url,
                        &retry_policy,
                        lease_duration,
                        job,
                    )
                    .await;
                    metrics::gauge!("reliableq_inflight_jobs").decrement(1.0);
                }
                .instrument(span),
            )
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
    execute_and_finalize_with_failpoints(
        pool,
        client,
        charge_service_url,
        retry_policy,
        lease_duration,
        claimed,
        &NoopFailpoints,
    )
    .await;
}

/// Same as [`execute_and_finalize`] but checks `failpoints` at the
/// three named crash points from spec sec. 12
/// (`AfterClaimBeforeEffect`, `AfterEffectBeforeFinalize`,
/// `DuringFinalize`), simulating a worker crash by returning
/// immediately when one triggers — exactly what a real process death
/// would leave behind (a claimed row with a ticking lease, or a
/// committed charge with no finalize). Only the chaos suite calls this
/// directly; `execute_and_finalize` always passes [`NoopFailpoints`].
pub async fn execute_and_finalize_with_failpoints(
    pool: &PgPool,
    client: &Client,
    charge_service_url: &str,
    retry_policy: &RetryPolicy,
    lease_duration: Duration,
    claimed: ClaimedJob,
    failpoints: &dyn Failpoints,
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
    let token_hash = lease_token_hash(lease_token);

    if failpoints.should_trigger(failpoint::FailpointName::AfterClaimBeforeEffect, job.id) {
        tracing::warn!(job_id = %job.id, "chaos: simulated crash after claim, before effect");
        return;
    }

    let heartbeat = spawn_heartbeat(pool.clone(), job.id, lease_token, lease_duration);

    let start = Instant::now();
    let outcome = execute_charge(client, charge_service_url, job.id, &job.payload).await;
    let duration_ms = i64::try_from(start.elapsed().as_millis()).unwrap_or(i64::MAX);
    let duration_secs = start.elapsed().as_secs_f64();
    heartbeat.abort();

    if outcome.is_ok()
        && failpoints.should_trigger(failpoint::FailpointName::AfterEffectBeforeFinalize, job.id)
    {
        tracing::warn!(job_id = %job.id, "chaos: simulated crash after effect, before finalize");
        return;
    }

    if failpoints.should_trigger(failpoint::FailpointName::DuringFinalize, job.id) {
        // Still perform the finalize call itself — a real crash "during"
        // finalize most plausibly lands after the atomic DB transaction
        // has already committed one way or the other; what a real crash
        // prevents is the worker reacting to the result, not the
        // transaction itself completing. This proves DB state is
        // correct independent of whether the worker survives to see it.
        match &outcome {
            Ok(()) => {
                let _ = jobs::finalize_success(pool, job.id, lease_token, duration_ms).await;
            }
            Err(failure) => {
                let reason = if failure.class.is_retryable() {
                    "RETRY_BUDGET_EXHAUSTED"
                } else {
                    failure.code
                };
                let _ = jobs::finalize_dead(
                    pool,
                    job.id,
                    lease_token,
                    reason,
                    &failure.message,
                    duration_ms,
                )
                .await;
            }
        }
        tracing::warn!(job_id = %job.id, "chaos: simulated crash during finalize (after the DB call)");
        return;
    }

    match outcome {
        Ok(()) => match jobs::finalize_success(pool, job.id, lease_token, duration_ms).await {
            Ok(true) => {
                tracing::info!(
                    job_id = %job.id,
                    attempt = attempt_number,
                    lease_token_hash = %token_hash,
                    "job succeeded"
                );
                metrics::counter!("reliableq_job_attempts_total", "kind" => job.kind.clone(), "outcome" => "SUCCEEDED")
                    .increment(1);
                metrics::histogram!("reliableq_job_duration_seconds", "kind" => job.kind.clone(), "outcome" => "SUCCEEDED")
                    .record(duration_secs);
            }
            Ok(false) => {
                tracing::warn!(
                    job_id = %job.id,
                    attempt = attempt_number,
                    lease_token_hash = %token_hash,
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
                        lease_token_hash = %token_hash,
                        "job failed with a retryable error, rescheduled"
                    );
                    metrics::counter!("reliableq_job_attempts_total", "kind" => job.kind.clone(), "outcome" => "RETRY_SCHEDULED")
                        .increment(1);
                    metrics::counter!("reliableq_retries_scheduled_total", "reason" => failure.code)
                        .increment(1);
                    metrics::histogram!("reliableq_job_duration_seconds", "kind" => job.kind.clone(), "outcome" => "RETRY_SCHEDULED")
                        .record(duration_secs);
                }
                Ok(false) => {
                    tracing::warn!(
                        job_id = %job.id,
                        attempt = attempt_number,
                        lease_token_hash = %token_hash,
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
                        lease_token_hash = %token_hash,
                        "job failed permanently and moved to DEAD"
                    );
                    metrics::counter!("reliableq_job_attempts_total", "kind" => job.kind.clone(), "outcome" => "DEAD")
                        .increment(1);
                    metrics::counter!("reliableq_dead_jobs_total", "reason" => reason).increment(1);
                    metrics::histogram!("reliableq_job_duration_seconds", "kind" => job.kind.clone(), "outcome" => "DEAD")
                        .record(duration_secs);
                }
                Ok(false) => {
                    tracing::warn!(
                        job_id = %job.id,
                        attempt = attempt_number,
                        lease_token_hash = %token_hash,
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
    let token_hash = lease_token_hash(lease_token);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match jobs::renew_lease(&pool, job_id, lease_token, lease_duration).await {
                Ok(true) => {
                    tracing::debug!(job_id = %job_id, lease_token_hash = %token_hash, "lease renewed");
                    metrics::counter!("reliableq_lease_renewals_total", "result" => "ok")
                        .increment(1);
                }
                Ok(false) => {
                    tracing::warn!(
                        job_id = %job_id,
                        lease_token_hash = %token_hash,
                        "lease renewal found the lease already gone"
                    );
                    metrics::counter!("reliableq_lease_renewals_total", "result" => "lost")
                        .increment(1);
                    break;
                }
                Err(err) => {
                    tracing::error!(job_id = %job_id, error = %err, "lease renewal failed");
                    metrics::counter!("reliableq_lease_renewals_total", "result" => "error")
                        .increment(1);
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
    // Correlates this call with the job across the worker's own logs
    // and fake-charge's (spec sec. 13.3); not a true end-to-end trace
    // from the original API request, which would require a request_id
    // captured at submission time and stored on the job row.
    let request_id = format!("job:{job_id}");

    let response = client
        .post(format!("{charge_service_url}/v1/charges"))
        .header("Idempotency-Key", idempotency_key)
        .header("X-Request-Id", &request_id)
        .json(&json!({
            "customer_id": charge_payload.customer_id,
            "amount_cents": charge_payload.amount_cents,
            "currency": charge_payload.currency,
        }))
        .send()
        .await
        .map_err(|err| {
            metrics::counter!("reliableq_downstream_requests_total", "result" => "unreachable")
                .increment(1);
            ExecutionFailure {
                class: classify_network_error(),
                code: "DOWNSTREAM_UNREACHABLE",
                message: err.to_string(),
            }
        })?;

    if response.status().is_success() {
        metrics::counter!("reliableq_downstream_requests_total", "result" => "success")
            .increment(1);
        Ok(())
    } else {
        let status = response.status();
        let class = classify_http_status(status.as_u16());
        let result = match class {
            FailureClass::Transient => "transient",
            FailureClass::Permanent => "permanent",
            FailureClass::Ambiguous => "ambiguous",
        };
        metrics::counter!("reliableq_downstream_requests_total", "result" => result).increment(1);
        let body = response.text().await.unwrap_or_default();
        Err(ExecutionFailure {
            class,
            code: "DOWNSTREAM_REJECTED",
            message: format!("charge service returned {status}: {body}"),
        })
    }
}
