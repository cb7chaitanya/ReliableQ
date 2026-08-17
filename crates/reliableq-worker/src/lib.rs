//! reliableq-worker: claims due jobs and executes them against
//! fake-charge. Crash recovery is real (M2): `claim_pending_jobs`
//! reclaims expired leases and closes out the abandoned attempt as
//! `LEASE_LOST`, and a stale worker's own finalize attempt is rejected
//! by the token-fenced guard below — that is why the `Ok(false)`
//! branches here only log, they do not need to write `LEASE_LOST`
//! themselves. Re-execution is now safe against duplicate charges too
//! (M3): the idempotency key is derived deterministically per job
//! (`reliableq_core::idempotency::charge_idempotency_key`), and
//! fake-charge replays rather than re-inserting on a repeat key.
//! Remaining gaps, see `docs/failure-lab.md`:
//!
//! - any execution failure goes straight to `DEAD`, no retry (M4)
//! - jobs in a claimed batch are executed one at a time, unbounded
//!   only in the sense that there is no semaphore yet (M6)

use std::time::Instant;

use reliableq_core::validation::parse_charge_payload;
use reliableq_db::jobs::{self, ClaimedJob};
use reqwest::Client;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug)]
pub struct ExecutionFailure {
    pub code: &'static str,
    pub message: String,
}

/// Executes one claimed job's charge call and finalizes the result.
/// Never panics on a lost lease or downstream error: both are handled
/// as ordinary, logged outcomes (spec sec. 19: no panics in normal
/// runtime paths).
pub async fn execute_and_finalize(
    pool: &PgPool,
    client: &Client,
    charge_service_url: &str,
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

    let start = Instant::now();
    let outcome = execute_charge(client, charge_service_url, job.id, &job.payload).await;
    let duration_ms = i64::try_from(start.elapsed().as_millis()).unwrap_or(i64::MAX);

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
        Err(failure) => {
            match jobs::finalize_dead(
                pool,
                job.id,
                lease_token,
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
                        "job failed and moved to DEAD (M1 has no retry policy yet)"
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
            code: "DOWNSTREAM_UNREACHABLE",
            message: err.to_string(),
        })?;

    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(ExecutionFailure {
            code: "DOWNSTREAM_REJECTED",
            message: format!("charge service returned {status}: {body}"),
        })
    }
}
