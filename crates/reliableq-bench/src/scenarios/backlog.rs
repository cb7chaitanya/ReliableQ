//! Scenario H — backlog draining. Preloads the full backlog with zero
//! workers running, then starts worker(s) and measures total drain time,
//! sustained throughput, queue depth over time, and tail latency
//! (docs/benchmarking/design.md sec. 1 Q8). The 100k-job point is
//! excluded from the quick profile by simply not listing it in
//! `benchmarks/config/quick.toml` (sec. 4 "Required scenarios" H).

use std::time::Duration;

use serde_json::json;
use uuid::Uuid;

use crate::correctness::{GateInput, run_gate};
use crate::db::{end_to_end_latencies_ms, submit_many, wait_for_drain};
use crate::result::{ErrorCounts, RunResult, Throughput};
use crate::stats::{LatencyPercentiles, throughput_per_sec};

use super::{ScenarioCtx, TEARDOWN_GRACE, reset_database};

const LEASE_DURATION_SECS: u64 = 15;
const DRAIN_DEADLINE_SECS: u64 = 900;

pub async fn run(ctx: &ScenarioCtx) -> anyhow::Result<()> {
    let Some(cfg) = ctx.config.backlog.clone() else {
        return Ok(());
    };
    if !cfg.enabled {
        return Ok(());
    }

    for &backlog in &cfg.backlogs {
        for run_number in 1..=ctx.config.repeat {
            tracing::info!(backlog, run_number, "backlog: starting run");
            reset_database(&ctx.pool).await?;

            let mut fake_charge = crate::procs::spawn_fake_charge(
                &ctx.binary_dir,
                &ctx.database_url,
                &ctx.fake_charge_bind,
            )
            .await?;
            let mut api =
                crate::procs::spawn_api(&ctx.binary_dir, &ctx.database_url, &ctx.api_bind).await?;

            let client = reqwest::Client::new();
            let preload_start = std::time::Instant::now();
            let outcomes =
                submit_many(&client, &ctx.api_base(), "backlog", 0, backlog, 128, 5).await;
            let preload_secs = preload_start.elapsed().as_secs_f64();
            let ids: Vec<Uuid> = outcomes.iter().filter_map(|o| o.id).collect();
            let submit_errors = outcomes.len() - ids.len();

            let worker_spec = crate::procs::WorkerSpec {
                binary_dir: &ctx.binary_dir,
                database_url: &ctx.database_url,
                charge_service_url: &ctx.fake_charge_base(),
                concurrency: cfg.worker_concurrency,
                lease_duration_secs: LEASE_DURATION_SECS,
                poll_interval_ms: 100,
                metrics_bind_addr: ctx.worker_metrics_addr(0),
                retry_base_delay_ms: None,
            };
            let start = std::time::Instant::now();
            let mut worker = crate::procs::spawn_worker(worker_spec).await?;

            let mut depth_samples: Vec<(f64, i64)> = Vec::new();
            let drained = wait_for_drain(
                &ctx.pool,
                Duration::from_secs(DRAIN_DEADLINE_SECS),
                Duration::from_millis(500),
                |t, depth| depth_samples.push((t, depth)),
            )
            .await?;
            let elapsed = start.elapsed().as_secs_f64();

            let latencies = end_to_end_latencies_ms(&ctx.pool, &ids)
                .await
                .unwrap_or_default();

            let correctness = run_gate(GateInput {
                pool: &ctx.pool,
                tracked_ids: &ids,
                expect_all_terminal: drained,
                worker_capacity: Some(cfg.worker_concurrency as u64),
                observed_peak_inflight: None,
                latency_samples_ms: &latencies,
                measurement_window_secs: elapsed,
            })
            .await?;

            let mut run = RunResult::base(
                &ctx.env,
                "backlog",
                json!({"backlog_size": backlog}),
                run_number,
            );
            run.api_process_count = 1;
            run.worker_process_count = 1;
            run.worker_concurrency = cfg.worker_concurrency;
            run.lease_duration_secs = LEASE_DURATION_SECS as f64;
            run.job_count = ids.len() as u64;
            run.warmup_duration_secs = Some(preload_secs);
            run.measurement_duration_secs = elapsed;
            run.throughput = Throughput {
                unit: "jobs_per_sec".to_string(),
                value: throughput_per_sec(ids.len(), elapsed),
            };
            run.latency_percentiles = LatencyPercentiles::from_samples_ms(&latencies);
            run.error_counts = ErrorCounts {
                http_errors: submit_errors as u64,
                timeouts: 0,
                other: 0,
            };
            run.correctness_results = correctness;
            run.extra = json!({
                "drained_within_deadline": drained,
                "preload_duration_secs": preload_secs,
                "queue_depth_over_time": depth_samples.iter().map(|(t, d)| json!({"t_secs": t, "depth": d})).collect::<Vec<_>>(),
            });
            ctx.write(&run)?;

            worker.graceful_stop(TEARDOWN_GRACE).await;
            api.graceful_stop(TEARDOWN_GRACE).await;
            fake_charge.graceful_stop(TEARDOWN_GRACE).await;
        }
    }
    Ok(())
}
