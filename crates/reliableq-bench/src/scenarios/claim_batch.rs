//! Scenario E — claim-batch sensitivity. `crates/reliableq-worker/src/poll.rs`
//! hardcodes `CLAIM_BATCH_MAX = 10` and does not expose it as
//! configuration, so this scenario drives `reliableq_db::jobs::claim_pending_jobs`
//! and `reliableq_worker::execute_and_finalize` directly at a chosen
//! batch size — the same technique `tests/integration/happy_path.rs` and
//! `tests/chaos/seeded_chaos.rs` already use to run one controlled
//! claim/execute cycle without the compiled worker binary. See
//! docs/benchmarking/design.md sec. 2 for why this measures the claim
//! transaction and execution pipeline at a chosen batch size, not the
//! shipped worker binary's own fixed-at-10 batching.

use std::time::Duration;

use reliableq_core::retry::RetryPolicy;
use reliableq_db::jobs;
use serde_json::json;
use uuid::Uuid;

use crate::correctness::{GateInput, run_gate};
use crate::db::charge_payload;
use crate::result::{ErrorCounts, RunResult, Throughput};
use crate::stats::LatencyPercentiles;

use super::{ScenarioCtx, TEARDOWN_GRACE, reset_database};

const LEASE_DURATION: Duration = Duration::from_secs(10);

pub async fn run(ctx: &ScenarioCtx) -> anyhow::Result<()> {
    let Some(cfg) = ctx.config.claim_batch.clone() else {
        return Ok(());
    };
    if !cfg.enabled {
        return Ok(());
    }

    for &batch_size in &cfg.batch_sizes {
        for run_number in 1..=ctx.config.repeat {
            tracing::info!(batch_size, run_number, "claim_batch: starting run");
            reset_database(&ctx.pool).await?;

            let mut fake_charge = crate::procs::spawn_fake_charge(
                &ctx.binary_dir,
                &ctx.database_url,
                &ctx.fake_charge_bind,
            )
            .await?;
            let charge_url = ctx.fake_charge_base();
            let client = reqwest::Client::new();

            let mut ids = Vec::with_capacity(cfg.job_count as usize);
            for i in 0..cfg.job_count {
                let row = jobs::insert_job(
                    &ctx.pool,
                    Uuid::new_v4(),
                    "charge",
                    &charge_payload("claim-batch", i),
                    5,
                )
                .await?;
                ids.push(row.id);
            }

            let retry_policy = RetryPolicy::DEFAULT;
            let mut claim_latencies_ms = Vec::new();
            let mut rounds = 0u64;
            let start = std::time::Instant::now();
            loop {
                let claim_start = std::time::Instant::now();
                let claimed = jobs::claim_pending_jobs(
                    &ctx.pool,
                    "bench-claim-worker",
                    batch_size,
                    LEASE_DURATION,
                )
                .await?;
                claim_latencies_ms.push(claim_start.elapsed().as_secs_f64() * 1000.0);
                rounds += 1;
                if claimed.is_empty() {
                    break;
                }
                for job in claimed {
                    reliableq_worker::execute_and_finalize(
                        &ctx.pool,
                        &client,
                        &charge_url,
                        &retry_policy,
                        LEASE_DURATION,
                        job,
                    )
                    .await;
                }
            }
            let elapsed = start.elapsed().as_secs_f64();

            let queue_latencies_ms: Vec<(f64,)> = sqlx::query_as(
                r#"
                SELECT EXTRACT(EPOCH FROM (a.started_at - j.created_at))::float8 * 1000.0
                FROM jobs j JOIN job_attempts a ON a.job_id = j.id AND a.attempt_number = 1
                WHERE j.id = ANY($1)
                "#,
            )
            .bind(&ids)
            .fetch_all(&ctx.pool)
            .await
            .unwrap_or_default();
            let queue_latencies: Vec<f64> = queue_latencies_ms.into_iter().map(|(v,)| v).collect();

            let correctness = run_gate(GateInput {
                pool: &ctx.pool,
                tracked_ids: &ids,
                expect_all_terminal: true,
                worker_capacity: None,
                observed_peak_inflight: None,
            })
            .await?;

            let mut run = RunResult::base(
                &ctx.env,
                "claim_batch",
                json!({"batch_size": batch_size}),
                run_number,
            );
            run.claim_batch_size = batch_size;
            run.job_count = ids.len() as u64;
            run.measurement_duration_secs = elapsed;
            run.throughput = Throughput {
                unit: "jobs_per_sec".to_string(),
                value: ids.len() as f64 / elapsed.max(1e-9),
            };
            run.latency_percentiles = LatencyPercentiles::from_samples_ms(&queue_latencies);
            run.error_counts = ErrorCounts::default();
            run.correctness_results = correctness;
            run.extra = json!({
                "claim_transaction_latency_ms": LatencyPercentiles::from_samples_ms(&claim_latencies_ms),
                "claim_rounds": rounds,
                "claim_transactions_per_sec": rounds as f64 / elapsed.max(1e-9),
                "queue_latency_note": "created_at -> first job_attempts.started_at",
                "driver_note": "drives reliableq_db::jobs::claim_pending_jobs + reliableq_worker::execute_and_finalize directly, not the compiled worker binary (see file header doc comment)",
            });
            ctx.write(&run)?;

            fake_charge.graceful_stop(TEARDOWN_GRACE).await;
        }
    }
    Ok(())
}
