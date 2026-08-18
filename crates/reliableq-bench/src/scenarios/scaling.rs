//! Scenario D — multi-worker scaling. First holds total concurrency
//! constant while varying worker *process* count, then holds
//! per-worker concurrency constant while increasing process count
//! (docs/benchmarking/design.md sec. 1 Q4).

use std::collections::BTreeMap;
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

#[allow(clippy::too_many_arguments)]
async fn run_point(
    ctx: &ScenarioCtx,
    axis: &str,
    worker_count: u32,
    concurrency_per_worker: u32,
    job_count: u64,
    warmup_jobs: u64,
    latency_ms: u64,
    run_number: u32,
) -> anyhow::Result<()> {
    tracing::info!(
        axis,
        worker_count,
        concurrency_per_worker,
        run_number,
        "scaling: starting run"
    );
    reset_database(&ctx.pool).await?;

    let mut fake_charge =
        crate::procs::spawn_fake_charge(&ctx.binary_dir, &ctx.database_url, &ctx.fake_charge_bind)
            .await?;
    let mut api =
        crate::procs::spawn_api(&ctx.binary_dir, &ctx.database_url, &ctx.api_bind).await?;
    let chaos = ChaosClient::new(ctx.fake_charge_base());
    chaos.set_delay_ms(latency_ms).await?;

    let client = reqwest::Client::new();
    if warmup_jobs > 0 {
        let _ = submit_many(
            &client,
            &ctx.api_base(),
            "scaling-warmup",
            0,
            warmup_jobs,
            32,
            5,
        )
        .await;
    }

    let outcomes = submit_many(
        &client,
        &ctx.api_base(),
        "scaling",
        warmup_jobs,
        job_count,
        64,
        5,
    )
    .await;
    let ids: Vec<Uuid> = outcomes.iter().filter_map(|o| o.id).collect();
    let submit_errors = outcomes.len() - ids.len();

    chaos.reset_inflight().await?;

    let mut workers = Vec::new();
    for index in 0..worker_count {
        let spec = crate::procs::WorkerSpec {
            binary_dir: &ctx.binary_dir,
            database_url: &ctx.database_url,
            charge_service_url: &ctx.fake_charge_base(),
            concurrency: concurrency_per_worker,
            lease_duration_secs: LEASE_DURATION_SECS,
            poll_interval_ms: 100,
            metrics_bind_addr: ctx.worker_metrics_addr(index),
            retry_base_delay_ms: None,
        };
        workers.push(crate::procs::spawn_worker(spec).await?);
    }

    let start = std::time::Instant::now();
    let drained = wait_for_drain(
        &ctx.pool,
        Duration::from_secs(DRAIN_DEADLINE_SECS),
        Duration::from_millis(150),
        |_, _| {},
    )
    .await?;
    let elapsed = start.elapsed().as_secs_f64();

    let peak_inflight = chaos.peak_inflight().await.ok();
    let latencies = end_to_end_latencies_ms(&ctx.pool, &ids)
        .await
        .unwrap_or_default();

    // Per-worker fairness: jobs completed grouped by worker_id.
    let per_worker_counts: Vec<(String, i64)> = if ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query_as(
            r#"
            SELECT worker_id, count(*) FROM job_attempts
            WHERE job_id = ANY($1) AND outcome = 'SUCCEEDED'
            GROUP BY worker_id
            ORDER BY worker_id
            "#,
        )
        .bind(&ids)
        .fetch_all(&ctx.pool)
        .await
        .unwrap_or_default()
    };
    let fairness: BTreeMap<String, i64> = per_worker_counts.into_iter().collect();

    let correctness = run_gate(GateInput {
        pool: &ctx.pool,
        tracked_ids: &ids,
        expect_all_terminal: drained,
        worker_capacity: Some((worker_count * concurrency_per_worker) as u64),
        observed_peak_inflight: peak_inflight,
        latency_samples_ms: &latencies,
        measurement_window_secs: elapsed,
    })
    .await?;

    let mut run = RunResult::base(
        &ctx.env,
        "scaling",
        json!({
            "axis": axis,
            "worker_count": worker_count,
            "concurrency_per_worker": concurrency_per_worker,
            "total_concurrency": worker_count * concurrency_per_worker,
        }),
        run_number,
    );
    run.api_process_count = 1;
    run.worker_process_count = worker_count;
    run.worker_concurrency = concurrency_per_worker;
    run.lease_duration_secs = LEASE_DURATION_SECS as f64;
    run.fake_charge_latency_ms = latency_ms;
    run.job_count = ids.len() as u64;
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
        "per_worker_succeeded_counts": fairness,
    });
    ctx.write(&run)?;

    for worker in &mut workers {
        worker.graceful_stop(TEARDOWN_GRACE).await;
    }
    api.graceful_stop(TEARDOWN_GRACE).await;
    fake_charge.graceful_stop(TEARDOWN_GRACE).await;
    Ok(())
}

pub async fn run(ctx: &ScenarioCtx) -> anyhow::Result<()> {
    let Some(cfg) = ctx.config.scaling.clone() else {
        return Ok(());
    };
    if !cfg.enabled {
        return Ok(());
    }

    for &(workers, per_worker) in &cfg.fixed_total {
        for run_number in 1..=ctx.config.repeat {
            run_point(
                ctx,
                "fixed_total_concurrency",
                workers,
                per_worker,
                cfg.job_count,
                cfg.warmup_jobs,
                cfg.latency_ms,
                run_number,
            )
            .await?;
        }
    }
    for &(workers, per_worker) in &cfg.increasing {
        for run_number in 1..=ctx.config.repeat {
            run_point(
                ctx,
                "increasing_total_concurrency",
                workers,
                per_worker,
                cfg.job_count,
                cfg.warmup_jobs,
                cfg.latency_ms,
                run_number,
            )
            .await?;
        }
    }
    Ok(())
}
