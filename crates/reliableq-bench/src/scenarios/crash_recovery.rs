//! Scenario I — worker-crash recovery. Two complementary drivers:
//!
//! 1. **Real process termination** (`run_real_kill`): a genuine
//!    `reliableq-worker` release binary, `kill -9`'d mid-flight against a
//!    slow downstream, recovered by a second real worker after lease
//!    expiry — the same mechanism `scripts/demo.sh` and
//!    `scripts/reliableq-demo.tape` demonstrate, measured here instead of
//!    just shown.
//! 2. **The three named failpoints** (`run_failpoint`), driven exactly
//!    the way `tests/chaos/seeded_chaos.rs` already does — directly
//!    against `reliableq_db::jobs` and
//!    `reliableq_worker::execute_and_finalize_with_failpoints` — because
//!    the compiled `reliableq-worker` binary has no flag to trigger them
//!    (deliberately, per SPEC.md sec. 12). See
//!    docs/benchmarking/design.md sec. 2.

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use reliableq_core::retry::RetryPolicy;
use reliableq_db::jobs;
use reliableq_worker::failpoint::{FailpointName, Failpoints};
use serde_json::json;
use uuid::Uuid;

use crate::chaos_client::ChaosClient;
use crate::correctness::{GateInput, run_gate};
use crate::db::charge_payload;
use crate::result::{ErrorCounts, RunResult, Throughput};
use crate::stats::LatencyPercentiles;

use super::{ScenarioCtx, TEARDOWN_GRACE, reset_database};

const FAILPOINT_LEASE: Duration = Duration::from_secs(2);
const QUIESCENCE_DEADLINE: Duration = Duration::from_secs(60);

/// Triggers exactly once per job, only for the named failpoint — the
/// first attempt "crashes" there; every later attempt (after lease
/// reclaim) runs normally.
struct CrashFirstAttempt {
    name: FailpointName,
    already_triggered: StdMutex<HashSet<Uuid>>,
}

impl Failpoints for CrashFirstAttempt {
    fn should_trigger(&self, name: FailpointName, job_id: Uuid) -> bool {
        if name != self.name {
            return false;
        }
        let mut seen = self
            .already_triggered
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        seen.insert(job_id)
    }
}

fn failpoint_label(name: FailpointName) -> &'static str {
    match name {
        FailpointName::AfterClaimBeforeEffect => "after_claim_before_effect",
        FailpointName::AfterEffectBeforeFinalize => "after_effect_before_finalize",
        FailpointName::DuringFinalize => "during_finalize",
    }
}

async fn run_failpoint(
    ctx: &ScenarioCtx,
    name: FailpointName,
    job_count: u64,
    run_number: u32,
) -> anyhow::Result<()> {
    let label = failpoint_label(name);
    tracing::info!(
        failpoint = label,
        run_number,
        "crash_recovery: failpoint run"
    );
    reset_database(&ctx.pool).await?;

    let mut fake_charge =
        crate::procs::spawn_fake_charge(&ctx.binary_dir, &ctx.database_url, &ctx.fake_charge_bind)
            .await?;
    let charge_url = ctx.fake_charge_base();
    let client = reqwest::Client::new();

    let mut ids = Vec::with_capacity(job_count as usize);
    for i in 0..job_count {
        let row = jobs::insert_job(
            &ctx.pool,
            Uuid::new_v4(),
            "charge",
            &charge_payload(&format!("crash-{label}"), i),
            6,
        )
        .await?;
        ids.push(row.id);
    }

    let failpoints = CrashFirstAttempt {
        name,
        already_triggered: StdMutex::new(HashSet::new()),
    };
    let retry_policy = RetryPolicy::DEFAULT;

    let start = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + QUIESCENCE_DEADLINE;
    let mut reached_quiescence = false;
    while tokio::time::Instant::now() < deadline {
        let claimed =
            jobs::claim_pending_jobs(&ctx.pool, "bench-crash-worker", 10, FAILPOINT_LEASE).await?;
        if claimed.is_empty() {
            let remaining: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM jobs WHERE id = ANY($1) AND status IN ('PENDING','RUNNING')",
            )
            .bind(&ids)
            .fetch_one(&ctx.pool)
            .await?;
            if remaining == 0 {
                reached_quiescence = true;
                break;
            }
            tokio::time::sleep(FAILPOINT_LEASE / 4).await;
            continue;
        }
        for job in claimed {
            reliableq_worker::execute_and_finalize_with_failpoints(
                &ctx.pool,
                &client,
                &charge_url,
                &retry_policy,
                FAILPOINT_LEASE,
                job,
                &failpoints,
            )
            .await;
        }
    }
    let elapsed = start.elapsed().as_secs_f64();

    // "Time from crash to reclaim": gap between attempt 1 (the one that
    // crashed) and attempt 2's start.
    let recovery_gaps_ms: Vec<(f64,)> = sqlx::query_as(
        r#"
        SELECT EXTRACT(EPOCH FROM (a2.started_at - a1.started_at))::float8 * 1000.0
        FROM job_attempts a1
        JOIN job_attempts a2 ON a2.job_id = a1.job_id AND a2.attempt_number = a1.attempt_number + 1
        WHERE a1.job_id = ANY($1) AND a1.attempt_number = 1
        "#,
    )
    .bind(&ids)
    .fetch_all(&ctx.pool)
    .await
    .unwrap_or_default();
    let recovery_gaps: Vec<f64> = recovery_gaps_ms.into_iter().map(|(v,)| v).collect();

    let lease_lost_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM job_attempts WHERE job_id = ANY($1) AND outcome = 'LEASE_LOST'",
    )
    .bind(&ids)
    .fetch_one(&ctx.pool)
    .await
    .unwrap_or(0);

    let correctness = run_gate(GateInput {
        pool: &ctx.pool,
        tracked_ids: &ids,
        expect_all_terminal: reached_quiescence,
        worker_capacity: None,
        observed_peak_inflight: None,
        latency_samples_ms: &recovery_gaps,
        measurement_window_secs: elapsed,
    })
    .await?;

    let mut run = RunResult::base(
        &ctx.env,
        "crash_recovery_failpoint",
        json!({"failpoint": label}),
        run_number,
    );
    run.lease_duration_secs = FAILPOINT_LEASE.as_secs_f64();
    run.job_count = ids.len() as u64;
    run.measurement_duration_secs = elapsed;
    run.throughput = Throughput {
        unit: "jobs_per_sec".to_string(),
        value: ids.len() as f64 / elapsed.max(1e-9),
    };
    run.latency_percentiles = LatencyPercentiles::from_samples_ms(&recovery_gaps);
    run.error_counts = ErrorCounts::default();
    run.correctness_results = correctness;
    run.extra = json!({
        "reached_quiescence": reached_quiescence,
        "jobs_that_crashed_and_recovered": recovery_gaps.len(),
        "lease_lost_attempts": lease_lost_count,
        "recovery_gap_ms_note": "attempt1.started_at -> attempt2.started_at, i.e. crash-to-reclaim wall time",
        "driver_note": "drives reliableq_worker::execute_and_finalize_with_failpoints directly; the compiled worker binary has no flag to enable failpoints (SPEC.md sec. 12)",
    });
    ctx.write(&run)?;

    fake_charge.graceful_stop(TEARDOWN_GRACE).await;
    Ok(())
}

/// Real `kill -9` of a live `reliableq-worker` process against a
/// downstream slower than the lease duration, recovered by a second real
/// worker process.
async fn run_real_kill(ctx: &ScenarioCtx, job_count: u64, run_number: u32) -> anyhow::Result<()> {
    tracing::info!(run_number, "crash_recovery: real kill -9 run");
    reset_database(&ctx.pool).await?;
    const LEASE_SECS: u64 = 5;

    let mut fake_charge =
        crate::procs::spawn_fake_charge(&ctx.binary_dir, &ctx.database_url, &ctx.fake_charge_bind)
            .await?;
    let mut api =
        crate::procs::spawn_api(&ctx.binary_dir, &ctx.database_url, &ctx.api_bind).await?;
    let chaos = ChaosClient::new(ctx.fake_charge_base());
    // Downstream latency well past the lease duration: any job claimed by
    // the worker we're about to kill is guaranteed still in flight (not
    // finalized) at the moment of the kill.
    chaos.set_delay_ms((LEASE_SECS + 5) * 1000).await?;

    let client = reqwest::Client::new();
    let outcomes = crate::db::submit_many(
        &client,
        &ctx.api_base(),
        "crash-real-kill",
        0,
        job_count,
        job_count as u32,
        5,
    )
    .await;
    let ids: Vec<Uuid> = outcomes.iter().filter_map(|o| o.id).collect();

    let worker1_spec = crate::procs::WorkerSpec {
        binary_dir: &ctx.binary_dir,
        database_url: &ctx.database_url,
        charge_service_url: &ctx.fake_charge_base(),
        concurrency: job_count.max(1) as u32,
        lease_duration_secs: LEASE_SECS,
        poll_interval_ms: 100,
        metrics_bind_addr: ctx.worker_metrics_addr(0),
        retry_base_delay_ms: None,
    };
    let kill_start = std::time::Instant::now();
    let mut worker1 = crate::procs::spawn_worker(worker1_spec).await?;
    // Give worker1 a moment to actually claim before killing it.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let claimed_before_kill: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE id = ANY($1) AND status = 'RUNNING'")
            .bind(&ids)
            .fetch_one(&ctx.pool)
            .await
            .unwrap_or(0);
    worker1.kill_now().await;
    let kill_elapsed = kill_start.elapsed().as_secs_f64();

    // Reset the downstream to normal latency once worker2 takes over, so
    // the reclaim actually succeeds quickly instead of also hitting the
    // long delay.
    chaos.set_delay_ms(0).await?;
    let worker2_spec = crate::procs::WorkerSpec {
        binary_dir: &ctx.binary_dir,
        database_url: &ctx.database_url,
        charge_service_url: &ctx.fake_charge_base(),
        concurrency: job_count.max(1) as u32,
        lease_duration_secs: LEASE_SECS,
        poll_interval_ms: 100,
        metrics_bind_addr: ctx.worker_metrics_addr(1),
        retry_base_delay_ms: None,
    };
    let mut worker2 = crate::procs::spawn_worker(worker2_spec).await?;

    let drained = crate::db::wait_for_drain(
        &ctx.pool,
        Duration::from_secs(120),
        Duration::from_millis(200),
        |_, _| {},
    )
    .await?;
    let total_elapsed = kill_start.elapsed().as_secs_f64();

    let latencies = crate::db::end_to_end_latencies_ms(&ctx.pool, &ids)
        .await
        .unwrap_or_default();

    let correctness = run_gate(GateInput {
        pool: &ctx.pool,
        tracked_ids: &ids,
        expect_all_terminal: drained,
        worker_capacity: None,
        observed_peak_inflight: None,
        latency_samples_ms: &latencies,
        measurement_window_secs: total_elapsed,
    })
    .await?;

    let mut run = RunResult::base(
        &ctx.env,
        "crash_recovery_real_kill",
        json!({"lease_duration_secs": LEASE_SECS}),
        run_number,
    );
    run.api_process_count = 1;
    run.worker_process_count = 2;
    run.lease_duration_secs = LEASE_SECS as f64;
    run.job_count = ids.len() as u64;
    run.measurement_duration_secs = total_elapsed;
    run.throughput = Throughput {
        unit: "jobs_per_sec".to_string(),
        value: ids.len() as f64 / total_elapsed.max(1e-9),
    };
    run.latency_percentiles = LatencyPercentiles::from_samples_ms(&latencies);
    run.error_counts = ErrorCounts::default();
    run.correctness_results = correctness;
    run.extra = json!({
        "drained_within_deadline": drained,
        "jobs_running_when_killed": claimed_before_kill,
        "time_to_kill_secs": kill_elapsed,
        "total_recovery_time_secs": total_elapsed,
        "method": "real SIGKILL (tokio::process::Child::start_kill) of a live reliableq-worker release binary",
    });
    ctx.write(&run)?;

    worker2.graceful_stop(TEARDOWN_GRACE).await;
    api.graceful_stop(TEARDOWN_GRACE).await;
    fake_charge.graceful_stop(TEARDOWN_GRACE).await;
    Ok(())
}

pub async fn run(ctx: &ScenarioCtx) -> anyhow::Result<()> {
    let Some(cfg) = ctx.config.crash_recovery.clone() else {
        return Ok(());
    };
    if !cfg.enabled {
        return Ok(());
    }

    for run_number in 1..=ctx.config.repeat {
        run_real_kill(ctx, 3, run_number).await?;
    }

    for name in [
        FailpointName::AfterClaimBeforeEffect,
        FailpointName::AfterEffectBeforeFinalize,
        FailpointName::DuringFinalize,
    ] {
        for run_number in 1..=ctx.config.repeat {
            run_failpoint(ctx, name, cfg.jobs_per_failpoint, run_number).await?;
        }
    }
    Ok(())
}
