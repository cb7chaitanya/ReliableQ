//! Scenarios B, C, F — execution overhead at a fixed downstream latency,
//! swept across worker concurrency (B: 0ms, C: 100ms — two points of the
//! same sweep) and across downstream latency at a fixed concurrency (F).
//! All three preload the full job count with zero workers running, then
//! start worker(s) and measure from that point to drain
//! (docs/benchmarking/design.md sec. 3: "preload ... then start real
//! worker process(es) and measure from that start until drain").

use std::time::Duration;

use serde_json::json;
use uuid::Uuid;

use crate::chaos_client::ChaosClient;
use crate::correctness::{GateInput, run_gate};
use crate::db::{end_to_end_latencies_ms, submit_many, wait_for_drain};
use crate::resource::{self, Sampler};
use crate::result::{ErrorCounts, ResourceSample, RunResult, Throughput};
use crate::stats::{LatencyPercentiles, throughput_per_sec};

use super::{ScenarioCtx, TEARDOWN_GRACE, reset_database};

const LEASE_DURATION_SECS: u64 = 10;
const DRAIN_DEADLINE_SECS: u64 = 180;
/// Matches `docker-compose.yml`'s fixed service/database name — this
/// project has no multi-database deployment mode, so hardcoding here
/// mirrors the same assumption `scripts/reliableq-demo.tape` already
/// makes (`docker exec reliableq-postgres-1 ...`).
const POSTGRES_CONTAINER: &str = "reliableq-postgres-1";
const POSTGRES_DB_NAME: &str = "reliableq";

#[allow(clippy::too_many_arguments)]
async fn run_point(
    ctx: &ScenarioCtx,
    scenario: &str,
    latency_ms: u64,
    concurrency: u32,
    job_count: u64,
    warmup_jobs: u64,
    run_number: u32,
) -> anyhow::Result<()> {
    tracing::info!(
        scenario,
        latency_ms,
        concurrency,
        run_number,
        "execution: starting run"
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

    // Warm-up: preload + drain a smaller batch through a throwaway worker,
    // discarded from measurement but left in the database (correctness
    // checks below only track the *measured* job IDs).
    if warmup_jobs > 0 {
        let _ = submit_many(
            &client,
            &ctx.api_base(),
            "exec-warmup",
            0,
            warmup_jobs,
            32,
            5,
        )
        .await;
        let warmup_spec = crate::procs::WorkerSpec {
            binary_dir: &ctx.binary_dir,
            database_url: &ctx.database_url,
            charge_service_url: &ctx.fake_charge_base(),
            concurrency,
            lease_duration_secs: LEASE_DURATION_SECS,
            poll_interval_ms: 100,
            metrics_bind_addr: ctx.worker_metrics_addr(0),
            retry_base_delay_ms: None,
        };
        let mut warmup_worker = crate::procs::spawn_worker(warmup_spec).await?;
        let _ = wait_for_drain(
            &ctx.pool,
            Duration::from_secs(DRAIN_DEADLINE_SECS),
            Duration::from_millis(200),
            |_, _| {},
        )
        .await;
        warmup_worker.graceful_stop(TEARDOWN_GRACE).await;
    }

    // Measured phase.
    let db_size_before = resource::database_size_bytes(&ctx.pool, POSTGRES_DB_NAME).await;
    let wal_lsn_before = resource::current_wal_lsn(&ctx.pool).await;

    let outcomes = submit_many(
        &client,
        &ctx.api_base(),
        scenario,
        warmup_jobs,
        job_count,
        64,
        5,
    )
    .await;
    let ids: Vec<Uuid> = outcomes.iter().filter_map(|o| o.id).collect();
    let submit_errors = outcomes.len() - ids.len();

    chaos.reset_inflight().await?;

    let worker_spec = crate::procs::WorkerSpec {
        binary_dir: &ctx.binary_dir,
        database_url: &ctx.database_url,
        charge_service_url: &ctx.fake_charge_base(),
        concurrency,
        lease_duration_secs: LEASE_DURATION_SECS,
        poll_interval_ms: 100,
        metrics_bind_addr: ctx.worker_metrics_addr(0),
        retry_base_delay_ms: None,
    };
    let start = std::time::Instant::now();
    let mut worker = crate::procs::spawn_worker(worker_spec).await?;

    let worker_sampler = worker
        .pid()
        .map(|pid| Sampler::start(pid, Duration::from_millis(500)));
    let api_sampler = api
        .pid()
        .map(|pid| Sampler::start(pid, Duration::from_millis(500)));
    let fc_sampler = fake_charge
        .pid()
        .map(|pid| Sampler::start(pid, Duration::from_millis(500)));

    let drained = wait_for_drain(
        &ctx.pool,
        Duration::from_secs(DRAIN_DEADLINE_SECS),
        Duration::from_millis(150),
        |_, _| {},
    )
    .await?;
    let elapsed = start.elapsed().as_secs_f64();

    let peak_inflight = chaos.peak_inflight().await.ok();

    let worker_resource = match worker_sampler {
        Some(s) => Some(s.stop_and_collect().await),
        None => None,
    };
    let api_resource = match api_sampler {
        Some(s) => Some(s.stop_and_collect().await),
        None => None,
    };
    let fc_resource = match fc_sampler {
        Some(s) => Some(s.stop_and_collect().await),
        None => None,
    };

    let db_size_after = resource::database_size_bytes(&ctx.pool, POSTGRES_DB_NAME).await;
    let wal_bytes = resource::wal_bytes_since(&ctx.pool, wal_lsn_before).await;
    let postgres_resource = resource::sample_postgres_container(POSTGRES_CONTAINER)
        .await
        .unwrap_or(ResourceSample {
            note: Some("docker stats unavailable for this container".to_string()),
            ..Default::default()
        });

    let latencies = end_to_end_latencies_ms(&ctx.pool, &ids)
        .await
        .unwrap_or_default();

    let correctness = run_gate(GateInput {
        pool: &ctx.pool,
        tracked_ids: &ids,
        expect_all_terminal: drained,
        worker_capacity: Some(concurrency as u64),
        observed_peak_inflight: peak_inflight,
    })
    .await?;

    let mut run = RunResult::base(
        &ctx.env,
        scenario,
        json!({"downstream_latency_ms": latency_ms, "worker_concurrency": concurrency}),
        run_number,
    );
    run.api_process_count = 1;
    run.worker_process_count = 1;
    run.worker_concurrency = concurrency;
    run.lease_duration_secs = LEASE_DURATION_SECS as f64;
    run.heartbeat_interval_secs = (LEASE_DURATION_SECS as f64) / 3.0;
    run.fake_charge_latency_ms = latency_ms;
    run.job_count = ids.len() as u64;
    run.warmup_duration_secs = Some(0.0);
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
    run.resource_measurements.worker = worker_resource;
    run.resource_measurements.api = api_resource;
    run.resource_measurements.fake_charge = fc_resource;
    run.resource_measurements.postgres = Some(postgres_resource);
    run.correctness_results = correctness;
    let theoretical_ceiling: Option<f64> = if latency_ms > 0 {
        Some(concurrency as f64 / (latency_ms as f64 / 1000.0))
    } else {
        None
    };
    run.extra = json!({
        "drained_within_deadline": drained,
        "theoretical_ceiling_jobs_per_sec_note":
            "concurrency / handler_latency, an approximation per docs/benchmarking/design.md sec. 1 Q3; null when latency is 0ms (undefined ceiling)",
        "theoretical_ceiling_jobs_per_sec": theoretical_ceiling,
        "database_size_bytes_before": db_size_before,
        "database_size_bytes_after": db_size_after,
        "wal_bytes_generated": wal_bytes,
    });
    ctx.write(&run)?;

    worker.graceful_stop(TEARDOWN_GRACE).await;
    api.graceful_stop(TEARDOWN_GRACE).await;
    fake_charge.graceful_stop(TEARDOWN_GRACE).await;
    Ok(())
}

pub async fn run(ctx: &ScenarioCtx) -> anyhow::Result<()> {
    if let Some(cfg) = ctx.config.execution.clone()
        && cfg.enabled
    {
        for &latency_ms in &cfg.latencies_ms {
            for &concurrency in &cfg.concurrencies {
                for run_number in 1..=ctx.config.repeat {
                    run_point(
                        ctx,
                        "execution",
                        latency_ms,
                        concurrency,
                        cfg.job_count,
                        cfg.warmup_jobs,
                        run_number,
                    )
                    .await?;
                }
            }
        }
    }

    if let Some(cfg) = ctx.config.downstream_latency.clone()
        && cfg.enabled
    {
        for &latency_ms in &cfg.latencies_ms {
            for run_number in 1..=ctx.config.repeat {
                run_point(
                    ctx,
                    "downstream_latency_sensitivity",
                    latency_ms,
                    cfg.worker_concurrency,
                    cfg.job_count,
                    cfg.job_count / 10,
                    run_number,
                )
                .await?;
            }
        }
    }

    Ok(())
}
