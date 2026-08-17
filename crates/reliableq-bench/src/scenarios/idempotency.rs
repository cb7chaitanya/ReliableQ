//! Scenario J — idempotency-key contention, driven directly against
//! `fake-charge`'s `POST /v1/charges` (docs/benchmarking/design.md sec.
//! 1 Q10). No API/worker involved — this is about the charge service's
//! own idempotency guarantee, not the job pipeline.

use serde_json::json;

use crate::result::{ErrorCounts, RunResult, Throughput};
use crate::stats::LatencyPercentiles;

use super::{ScenarioCtx, TEARDOWN_GRACE, reset_database};

async fn post_charge(
    client: &reqwest::Client,
    base: &str,
    key: &str,
    amount_cents: i64,
) -> (u16, f64) {
    let start = std::time::Instant::now();
    let result = client
        .post(format!("{base}/v1/charges"))
        .header("Idempotency-Key", key)
        .json(
            &json!({"customer_id": "bench-idem", "amount_cents": amount_cents, "currency": "INR"}),
        )
        .send()
        .await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    match result {
        Ok(resp) => (resp.status().as_u16(), elapsed_ms),
        Err(_) => (0, elapsed_ms),
    }
}

async fn charge_count(pool: &sqlx::PgPool, key: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM charges WHERE idempotency_key = $1")
        .bind(key)
        .fetch_one(pool)
        .await
        .unwrap_or(-1)
}

pub async fn run(ctx: &ScenarioCtx) -> anyhow::Result<()> {
    let Some(cfg) = ctx.config.idempotency.clone() else {
        return Ok(());
    };
    if !cfg.enabled {
        return Ok(());
    }

    for &concurrency in &cfg.concurrency_levels {
        for run_number in 1..=ctx.config.repeat {
            tracing::info!(concurrency, run_number, "idempotency: starting run");
            reset_database(&ctx.pool).await?;
            let mut fake_charge = crate::procs::spawn_fake_charge(
                &ctx.binary_dir,
                &ctx.database_url,
                &ctx.fake_charge_bind,
            )
            .await?;
            let base = ctx.fake_charge_base();
            let client = reqwest::Client::new();

            // --- Unique charge requests: `concurrency` distinct keys, no contention. ---
            let mut unique_set = tokio::task::JoinSet::new();
            for i in 0..concurrency {
                let client = client.clone();
                let base = base.clone();
                let key = format!("bench-unique-{run_number}-{i}");
                unique_set
                    .spawn(async move { post_charge(&client, &base, &key, 100 + i as i64).await });
            }
            let mut unique_latencies = Vec::new();
            while let Some(res) = unique_set.join_next().await {
                if let Ok((status, ms)) = res
                    && status == 201
                {
                    unique_latencies.push(ms);
                }
            }

            // --- Sequential replay: submit once, then again, same key. ---
            let replay_key = format!("bench-replay-{run_number}");
            let (first_status, _) = post_charge(&client, &base, &replay_key, 555).await;
            let (replay_status, _replay_ms) = post_charge(&client, &base, &replay_key, 555).await;
            let replay_ok = first_status == 201 && replay_status == 200;
            let replay_charge_count = charge_count(&ctx.pool, &replay_key).await;

            // --- Concurrent requests sharing one key: the primary
            // measured case (spec sec. 4 J: "requests = N, charges_created = 1"). ---
            let shared_key = format!("bench-shared-{run_number}");
            let start = std::time::Instant::now();
            let mut shared_set = tokio::task::JoinSet::new();
            for _ in 0..concurrency {
                let client = client.clone();
                let base = base.clone();
                let key = shared_key.clone();
                shared_set.spawn(async move { post_charge(&client, &base, &key, 4200).await });
            }
            let mut shared_latencies = Vec::new();
            let mut created = 0u32;
            let mut replayed = 0u32;
            let mut other = 0u32;
            while let Some(res) = shared_set.join_next().await {
                if let Ok((status, ms)) = res {
                    shared_latencies.push(ms);
                    match status {
                        201 => created += 1,
                        200 => replayed += 1,
                        _ => other += 1,
                    }
                }
            }
            let elapsed = start.elapsed().as_secs_f64();
            let shared_charge_count = charge_count(&ctx.pool, &shared_key).await;
            let shared_invariant_holds = shared_charge_count == 1 && created == 1;

            // --- Same key, conflicting payload: must not silently succeed. ---
            let conflict_key = format!("bench-conflict-{run_number}");
            let (first_status, _) = post_charge(&client, &base, &conflict_key, 111).await;
            let (conflict_status, _) = post_charge(&client, &base, &conflict_key, 222).await;
            let conflict_correctly_rejected = first_status == 201 && conflict_status == 409;
            let conflict_charge_count = charge_count(&ctx.pool, &conflict_key).await;

            let passed = shared_invariant_holds
                && replay_ok
                && conflict_correctly_rejected
                && conflict_charge_count == 1
                && replay_charge_count == 1;

            let mut run = RunResult::base(
                &ctx.env,
                "idempotency_contention",
                json!({"concurrency": concurrency}),
                run_number,
            );
            run.job_count = concurrency as u64;
            run.measurement_duration_secs = elapsed;
            run.throughput = Throughput {
                unit: "requests_per_sec".to_string(),
                value: concurrency as f64 / elapsed.max(1e-9),
            };
            run.latency_percentiles = LatencyPercentiles::from_samples_ms(&shared_latencies);
            run.error_counts = ErrorCounts {
                http_errors: other as u64,
                timeouts: 0,
                other: 0,
            };
            run.correctness_results = crate::result::CorrectnessResults {
                passed,
                checks: json!({
                    "unique_requests_all_created": unique_latencies.len() == concurrency as usize,
                    "sequential_replay_returns_200_with_one_row": replay_ok && replay_charge_count == 1,
                    "concurrent_shared_key_creates_exactly_one_charge": shared_invariant_holds,
                    "conflicting_payload_rejected_not_silently_succeeded": conflict_correctly_rejected && conflict_charge_count == 1,
                }),
                failures: if passed {
                    Vec::new()
                } else {
                    vec!["one or more idempotency invariants failed; see checks".to_string()]
                },
            };
            run.extra = json!({
                "unique_requests": concurrency,
                "unique_created_count": unique_latencies.len(),
                "unique_latency_ms": LatencyPercentiles::from_samples_ms(&unique_latencies),
                "shared_key_requests": concurrency,
                "shared_key_created": created,
                "shared_key_replayed": replayed,
                "shared_key_charges_in_db": shared_charge_count,
                "conflict_charges_in_db": conflict_charge_count,
            });
            ctx.write(&run)?;

            fake_charge.graceful_stop(TEARDOWN_GRACE).await;
        }
    }
    Ok(())
}
