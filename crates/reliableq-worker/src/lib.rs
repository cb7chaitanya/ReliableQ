//! reliableq-worker: claims due jobs and executes them against
//! fake-charge. Crash recovery is now real (M2): `claim_pending_jobs`
//! reclaims expired leases and closes out the abandoned attempt as
//! `LEASE_LOST`, and a stale worker's own finalize attempt is rejected
//! by the token-fenced guard below — that is why the `Ok(false)`
//! branches here only log, they do not need to write `LEASE_LOST`
//! themselves. Remaining gaps, see `docs/failure-lab.md`:
//!
//! - the charge idempotency key is scoped per *attempt*, not per job
//!   (M3 makes it job-scoped and adds graceful replay)
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

struct ExecutionFailure {
    code: &'static str,
    message: String,
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
    let outcome = execute_charge(
        client,
        charge_service_url,
        job.id,
        attempt_number,
        &job.payload,
    )
    .await;
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

async fn execute_charge(
    client: &Client,
    charge_service_url: &str,
    job_id: Uuid,
    attempt_number: i32,
    payload: &serde_json::Value,
) -> Result<(), ExecutionFailure> {
    let charge_payload = parse_charge_payload(payload).map_err(|err| ExecutionFailure {
        code: "INVALID_PAYLOAD",
        message: err.to_string(),
    })?;

    // Deliberately attempt-scoped, not job-scoped: see this module's
    // doc comment and docs/failure-lab.md M3. A second attempt at this
    // same job would send a different key, which is exactly the naive
    // gap M3's duplicate-charge reproduction relies on.
    let idempotency_key = format!("reliableq:charge:{job_id}:attempt:{attempt_number}");

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
