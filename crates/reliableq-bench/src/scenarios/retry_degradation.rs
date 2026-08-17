//! Scenario G — retry degradation under a persistent transient-failure
//! rate (docs/benchmarking/design.md sec. 4's `fail_rate` addition).
//! Measures retry amplification, eventual-success latency, and dead-job
//! count as the failure rate rises.

use std::time::Duration;

use serde_json::json;
use uuid::Uuid;

use crate::chaos_client::ChaosClient;
use crate::correctness::{GateInput, run_gate};
use crate::db::{end_to_end_latencies_ms, submit_many, wait_for_drain};
use crate::result::{ErrorCounts, RunResult, Throughput};
use crate::stats::{LatencyPercentiles, throughput_per_sec};

use super::{ScenarioCtx, TEARDOWN_GRACE, reset_database};

const LEASE_DURATION_SECS: u64 = 10;
const DRAIN_DEADLINE_SECS: u64 = 180;
const SEED: u64 = 20260818;

pub async fn run(ctx: &ScenarioCtx) -> anyhow::Result<()> {
    let Some(cfg) = ctx.config.retry_degradation.clone() else {
        return Ok(());
    };
    if !cfg.enabled {
        return Ok(());
    }

    for &rate in &cfg.failure_rates {
        for run_number in 1..=ctx.config.repeat {
            tracing::info!(rate, run_number, "retry_degradation: starting run");
            reset_database(&ctx.pool).await?;

            let mut fake_charge = crate::procs::spawn_fake_charge(
                &ctx.binary_dir,
                &ctx.database_url,
                &ctx.fake_charge_bind,
            )
            .await?;
            let mut api =
                crate::procs::spawn_api(&ctx.binary_dir, &ctx.database_url, &ctx.api_bind).await?;
            let chaos = ChaosClient::new(ctx.fake_charge_base());
            if rate > 0.0 {
                chaos
                    .set_fail_rate(rate, 503, SEED + run_number as u64)
                    .await?;
            } else {
                chaos.set_normal().await?;
            }

            let client = reqwest::Client::new();
            let outcomes = submit_many(
                &client,
                &ctx.api_base(),
                "retry-degradation",
                0,
                cfg.job_count,
                64,
                6,
            )
            .await;
            let ids: Vec<Uuid> = outcomes.iter().filter_map(|o| o.id).collect();

            let worker_spec = crate::procs::WorkerSpec {
                binary_dir: &ctx.binary_dir,
                database_url: &ctx.database_url,
                charge_service_url: &ctx.fake_charge_base(),
                concurrency: cfg.worker_concurrency,
                lease_duration_secs: LEASE_DURATION_SECS,
                poll_interval_ms: 100,
                metrics_bind_addr: ctx.worker_metrics_addr(0),
                // Faster than the production default so a `50%` failure
                // rate still converges within this scenario's drain
                // deadline instead of spending most of the run asleep in
                // backoff — retry *shape* (capped exponential + full
                // jitter) is unchanged, only its base delay is
                // compressed. Documented in the run's own record so this
                // is never confused with the shipped default.
                retry_base_delay_ms: Some(200),
            };
            let start = std::time::Instant::now();
            let mut worker = crate::procs::spawn_worker(worker_spec).await?;

            let drained = wait_for_drain(
                &ctx.pool,
                Duration::from_secs(DRAIN_DEADLINE_SECS),
                Duration::from_millis(200),
                |_, _| {},
            )
            .await?;
            let elapsed = start.elapsed().as_secs_f64();

            let total_attempts: i64 = if ids.is_empty() {
                0
            } else {
                sqlx::query_scalar("SELECT count(*) FROM job_attempts WHERE job_id = ANY($1)")
                    .bind(&ids)
                    .fetch_one(&ctx.pool)
                    .await
                    .unwrap_or(0)
            };
            let dead_count: i64 = if ids.is_empty() {
                0
            } else {
                sqlx::query_scalar(
                    "SELECT count(*) FROM jobs WHERE id = ANY($1) AND status = 'DEAD'",
                )
                .bind(&ids)
                .fetch_one(&ctx.pool)
                .await
                .unwrap_or(0)
            };
            let succeeded_count: i64 = if ids.is_empty() {
                0
            } else {
                sqlx::query_scalar(
                    "SELECT count(*) FROM jobs WHERE id = ANY($1) AND status = 'SUCCEEDED'",
                )
                .bind(&ids)
                .fetch_one(&ctx.pool)
                .await
                .unwrap_or(0)
            };
            let retry_delays_ms: Vec<(i64,)> = if ids.is_empty() {
                Vec::new()
            } else {
                sqlx::query_as(
                    "SELECT scheduled_delay_ms FROM job_attempts WHERE job_id = ANY($1) AND scheduled_delay_ms IS NOT NULL",
                )
                .bind(&ids)
                .fetch_all(&ctx.pool)
                .await
                .unwrap_or_default()
            };
            let retry_delays: Vec<f64> = retry_delays_ms.into_iter().map(|(v,)| v as f64).collect();

            let latencies = end_to_end_latencies_ms(&ctx.pool, &ids)
                .await
                .unwrap_or_default();

            let retry_amplification = if !ids.is_empty() {
                total_attempts as f64 / ids.len() as f64
            } else {
                0.0
            };

            let correctness = run_gate(GateInput {
                pool: &ctx.pool,
                tracked_ids: &ids,
                expect_all_terminal: drained,
                worker_capacity: Some(cfg.worker_concurrency as u64),
                observed_peak_inflight: chaos.peak_inflight().await.ok(),
            })
            .await?;

            let mut run = RunResult::base(
                &ctx.env,
                "retry_degradation",
                json!({"transient_failure_rate": rate}),
                run_number,
            );
            run.api_process_count = 1;
            run.worker_process_count = 1;
            run.worker_concurrency = cfg.worker_concurrency;
            run.lease_duration_secs = LEASE_DURATION_SECS as f64;
            run.fake_charge_failure_mode = format!("fail_rate={rate}");
            run.job_count = ids.len() as u64;
            run.measurement_duration_secs = elapsed;
            run.throughput = Throughput {
                unit: "jobs_per_sec".to_string(),
                value: throughput_per_sec(ids.len(), elapsed),
            };
            run.latency_percentiles = LatencyPercentiles::from_samples_ms(&latencies);
            run.error_counts = ErrorCounts::default();
            run.retry_configuration = json!({"base_delay_ms": 200, "multiplier": 2, "max_delay_secs": 60, "note": "compressed base_delay for this scenario only, see run_point doc comment"});
            run.correctness_results = correctness;
            run.extra = json!({
                "drained_within_deadline": drained,
                "retry_amplification": retry_amplification,
                "total_handler_attempts": total_attempts,
                "total_logical_jobs": ids.len(),
                "dead_job_count": dead_count,
                "succeeded_count": succeeded_count,
                "retry_delay_ms_distribution": LatencyPercentiles::from_samples_ms(&retry_delays),
            });
            ctx.write(&run)?;

            worker.graceful_stop(TEARDOWN_GRACE).await;
            api.graceful_stop(TEARDOWN_GRACE).await;
            fake_charge.graceful_stop(TEARDOWN_GRACE).await;
        }
    }
    Ok(())
}
