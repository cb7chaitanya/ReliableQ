//! Scenario A — ingestion baseline (docs/benchmarking/design.md sec. 1
//! Q1): `POST /v1/jobs` throughput/latency with zero workers running, so
//! only the API + PostgreSQL insert path is measured.

use serde_json::json;
use uuid::Uuid;

use crate::correctness::{GateInput, run_gate};
use crate::db::submit_many;
use crate::result::{ErrorCounts, RunResult, Throughput};
use crate::stats::{LatencyPercentiles, throughput_per_sec};

use super::{ScenarioCtx, TEARDOWN_GRACE, reset_database};

pub async fn run(ctx: &ScenarioCtx) -> anyhow::Result<()> {
    let Some(cfg) = ctx.config.ingestion.clone() else {
        return Ok(());
    };
    if !cfg.enabled {
        return Ok(());
    }

    for &concurrency in &cfg.concurrencies {
        for run_number in 1..=ctx.config.repeat {
            tracing::info!(concurrency, run_number, "ingestion: starting run");
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

            let warmup_start = std::time::Instant::now();
            let _ = submit_many(
                &client,
                &ctx.api_base(),
                "ingestion-warmup",
                0,
                cfg.warmup_requests,
                concurrency,
                5,
            )
            .await;
            let warmup_secs = warmup_start.elapsed().as_secs_f64();

            let start = std::time::Instant::now();
            let outcomes = submit_many(
                &client,
                &ctx.api_base(),
                "ingestion",
                cfg.warmup_requests,
                cfg.requests,
                concurrency,
                5,
            )
            .await;
            let elapsed = start.elapsed().as_secs_f64();

            let successes: Vec<Uuid> = outcomes.iter().filter_map(|o| o.id).collect();
            let error_count = outcomes.len() - successes.len();
            let latencies: Vec<f64> = outcomes
                .iter()
                .filter(|o| o.status == 202)
                .map(|o| o.elapsed_ms)
                .collect();

            let correctness = run_gate(GateInput {
                pool: &ctx.pool,
                tracked_ids: &successes,
                expect_all_terminal: false,
                worker_capacity: None,
                observed_peak_inflight: None,
            })
            .await?;

            let mut run = RunResult::base(
                &ctx.env,
                "ingestion",
                json!({"concurrency": concurrency}),
                run_number,
            );
            run.api_process_count = 1;
            run.worker_process_count = 0;
            run.job_count = outcomes.len() as u64;
            run.warmup_duration_secs = Some(warmup_secs);
            run.measurement_duration_secs = elapsed;
            run.throughput = Throughput {
                unit: "requests_per_sec".to_string(),
                value: throughput_per_sec(outcomes.len(), elapsed),
            };
            run.latency_percentiles = LatencyPercentiles::from_samples_ms(&latencies);
            run.error_counts = ErrorCounts {
                http_errors: error_count as u64,
                timeouts: 0,
                other: 0,
            };
            run.correctness_results = correctness;
            run.extra = json!({
                "committed_submissions_per_sec": throughput_per_sec(successes.len(), elapsed),
                "committed_submissions": successes.len(),
                "postgres_commit_latency_note": "not separately observable without API instrumentation this task does not add; response latency includes the commit",
                "connection_pool_note": "unavailable: the sqlx pool lives inside the api process, not this harness",
            });
            ctx.write(&run)?;

            api.graceful_stop(TEARDOWN_GRACE).await;
            fake_charge.graceful_stop(TEARDOWN_GRACE).await;
        }
    }
    Ok(())
}
