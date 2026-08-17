//! The claim/execute/shutdown poll loop, factored out of `main.rs` so
//! it can be driven by tests with a synthetic shutdown trigger instead
//! of real OS signals (spec sec. 17: chaos/property tests need
//! deterministic control over shutdown timing).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use reliableq_core::config::WorkerConfig;
use reliableq_db::jobs;
use reqwest::Client;
use sqlx::PgPool;
use tokio::sync::Semaphore;

use crate::spawn_bounded_batch;

/// Upper bound on one claim round-trip, independent of concurrency
/// (spec sec. 9.5: "a configurable batch maximum"). The actual claim
/// size is further capped by available semaphore permits.
const CLAIM_BATCH_MAX: i64 = 10;

/// Runs the claim/execute loop until `shutdown` resolves, then drains
/// in-flight work up to `worker_config.shutdown_grace` before
/// returning. Never claims more than the configured concurrency allows
/// (spec sec. 9.5) and never claims anything at all once `shutdown` has
/// fired.
///
/// `#[instrument]` attaches `worker_id` to every log event emitted from
/// this function and everything it calls (spec sec. 13.1: structured
/// logs must carry `worker_id`), without threading it through every
/// function signature by hand.
#[tracing::instrument(skip_all, fields(worker_id = %worker_id))]
pub async fn run_worker_loop(
    pool: &PgPool,
    client: &Client,
    worker_id: &str,
    worker_config: &WorkerConfig,
    shutdown: impl Future<Output = ()>,
) {
    let retry_policy = worker_config.retry_policy();
    let semaphore = Arc::new(Semaphore::new(worker_config.concurrency));
    let mut shutdown = std::pin::pin!(shutdown);

    'poll: loop {
        // Never claim more than we currently have permits for — claiming
        // ahead of capacity would just leave rows RUNNING with a
        // ticking lease while they wait for a permit, buying nothing.
        let available = semaphore.available_permits() as i64;
        let batch_size = available.min(CLAIM_BATCH_MAX);
        if batch_size == 0 {
            tokio::select! {
                _ = &mut shutdown => break 'poll,
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
            continue;
        }

        let claimed = match jobs::claim_pending_jobs(
            pool,
            worker_id,
            batch_size,
            worker_config.lease_duration,
        )
        .await
        {
            Ok(claimed) => claimed,
            Err(err) => {
                tracing::error!(error = %err, "failed to claim jobs, backing off");
                Vec::new()
            }
        };

        if claimed.is_empty() {
            // Full-jitter idle poll (spec sec. 9.5): avoids a
            // synchronized fleet of workers all polling in lockstep.
            let jitter_ms =
                rand::thread_rng().gen_range(0..=worker_config.poll_interval.as_millis() as u64);
            let wait = worker_config.poll_interval + Duration::from_millis(jitter_ms);
            tokio::select! {
                _ = &mut shutdown => break 'poll,
                _ = tokio::time::sleep(wait) => {}
            }
            continue;
        }

        let handles = spawn_bounded_batch(
            pool.clone(),
            client.clone(),
            worker_config.charge_service_url.clone(),
            retry_policy,
            worker_config.lease_duration,
            semaphore.clone(),
            claimed,
        );
        let mut batch = std::pin::pin!(await_all(handles));

        tokio::select! {
            _ = &mut batch => {}
            _ = &mut shutdown => {
                tracing::warn!(
                    grace = ?worker_config.shutdown_grace,
                    "shutdown signal received mid-batch; stopping new claims, \
                     waiting for in-flight work up to the grace period"
                );
                // Reuse the *same* in-flight batch future rather than
                // inferring completion from semaphore permit counts: a
                // freshly spawned task may not have been polled even
                // once yet (nothing decrements its permit until it
                // runs), so "permits == concurrency" is not a reliable
                // "nothing is running" signal at this exact instant.
                // Awaiting the real handles is.
                match tokio::time::timeout(worker_config.shutdown_grace, &mut batch).await {
                    Ok(()) => {
                        tracing::info!("in-flight batch finished within the grace period");
                    }
                    Err(_) => {
                        tracing::warn!(
                            "grace period elapsed with work still in flight; abandoning it \
                             (leases will expire and be reclaimed)"
                        );
                    }
                }
                break 'poll;
            }
        }
    }
}

async fn await_all(handles: Vec<tokio::task::JoinHandle<()>>) {
    for handle in handles {
        let _ = handle.await;
    }
}
