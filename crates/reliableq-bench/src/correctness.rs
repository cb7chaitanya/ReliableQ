//! The correctness gate (docs/benchmarking/design.md sec. 7), run after
//! every scenario directly against PostgreSQL. A scenario's timing
//! numbers are irrelevant if this fails — `run.rs` marks the whole
//! `RunResult` `correctness_results.passed = false` and `report.rs`
//! excludes such runs from any published throughput/latency number.

use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::result::CorrectnessResults;

pub struct GateInput<'a> {
    pub pool: &'a PgPool,
    /// Every job ID this scenario believes it submitted.
    pub tracked_ids: &'a [Uuid],
    /// If `true`, every tracked job must be terminal (SUCCEEDED/DEAD) by
    /// the time the gate runs — appropriate for scenarios that ran to
    /// drain; scenarios that intentionally stop mid-flight (e.g. an
    /// ingestion-only run with zero workers) pass `false`.
    pub expect_all_terminal: bool,
    /// `worker_process_count * worker_concurrency`, if workers ran.
    pub worker_capacity: Option<u64>,
    /// `peak_inflight` observed via fake-charge's `/v1/test/inflight`
    /// (reset before the scenario started).
    pub observed_peak_inflight: Option<u64>,
    /// Wall-clock-derived latency samples (created_at/finished_at,
    /// job_attempts.started_at, etc.) this run collected, if any, paired
    /// with `measurement_window_secs` — the same scenario's own
    /// `Instant`-based elapsed time. `Instant` is monotonic and unaffected
    /// by the host sleeping mid-run; PostgreSQL timestamps are wall-clock
    /// and are not — a host sleep/wake spanning a job's lifetime produces
    /// an impossible multi-hour latency sample with a perfectly normal
    /// `measurement_duration_secs`. Caught by actually running the quick
    /// profile on a laptop that went to sleep mid-run, not by inspection —
    /// see docs/benchmarking/design.md sec. 8.
    pub latency_samples_ms: &'a [f64],
    pub measurement_window_secs: f64,
}

pub async fn run_gate(input: GateInput<'_>) -> anyhow::Result<CorrectnessResults> {
    let mut checks: BTreeMap<String, Value> = BTreeMap::new();
    let mut failures = Vec::new();
    let pool = input.pool;
    let ids = input.tracked_ids;

    // 1. every tracked job exists with a known status.
    let found: Vec<(Uuid, String, i32, i32)> = if ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query_as("SELECT id, status, attempts, max_attempts FROM jobs WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(pool)
            .await?
    };
    let all_found = found.len() == ids.len();
    checks.insert("all_tracked_jobs_found".into(), Value::Bool(all_found));
    if !all_found {
        failures.push(format!(
            "expected {} tracked jobs, found {} in the database",
            ids.len(),
            found.len()
        ));
    }

    // 2. terminal-state consistency.
    let terminal_count = found
        .iter()
        .filter(|(_, status, _, _)| status == "SUCCEEDED" || status == "DEAD")
        .count();
    let non_terminal_count = found.len() - terminal_count;
    if input.expect_all_terminal {
        let all_terminal = non_terminal_count == 0;
        checks.insert(
            "all_tracked_jobs_terminal".into(),
            Value::Bool(all_terminal),
        );
        if !all_terminal {
            failures.push(format!(
                "{non_terminal_count} tracked jobs are still PENDING/RUNNING, expected 0"
            ));
        }
    }

    // 3. no job exceeds its attempt budget.
    let over_budget: Vec<&(Uuid, String, i32, i32)> =
        found.iter().filter(|(_, _, a, m)| a > m).collect();
    let no_over_budget = over_budget.is_empty();
    checks.insert(
        "no_job_over_attempt_budget".into(),
        Value::Bool(no_over_budget),
    );
    if !no_over_budget {
        failures.push(format!(
            "{} jobs exceeded their attempt budget",
            over_budget.len()
        ));
    }

    // 4. at most one charge per idempotency key, scoped to this
    // scenario's own jobs (other scenarios' charges live in the same
    // long-lived database and must not be misattributed).
    let duplicate_keys: i64 = if ids.is_empty() {
        0
    } else {
        let keys: Vec<String> = ids
            .iter()
            .map(|id| format!("reliableq:charge:{id}"))
            .collect();
        sqlx::query_scalar(
            r#"
            SELECT count(*) FROM (
                SELECT idempotency_key FROM charges
                WHERE idempotency_key = ANY($1)
                GROUP BY idempotency_key
                HAVING count(*) > 1
            ) dup
            "#,
        )
        .bind(&keys)
        .fetch_one(pool)
        .await?
    };
    let no_duplicate_charges = duplicate_keys == 0;
    checks.insert(
        "at_most_one_charge_per_idempotency_key".into(),
        Value::Bool(no_duplicate_charges),
    );
    if !no_duplicate_charges {
        failures.push(format!(
            "{duplicate_keys} idempotency keys back more than one charge row"
        ));
    }

    // 5. at most one SUCCEEDED job_attempts row per job (fencing
    // evidence: a stale, reclaimed owner must never have also
    // finalized a success).
    let jobs_with_multiple_success: i64 = if ids.is_empty() {
        0
    } else {
        sqlx::query_scalar(
            r#"
            SELECT count(*) FROM (
                SELECT job_id FROM job_attempts
                WHERE job_id = ANY($1) AND outcome = 'SUCCEEDED'
                GROUP BY job_id
                HAVING count(*) > 1
            ) dup
            "#,
        )
        .bind(ids)
        .fetch_one(pool)
        .await?
    };
    let no_stale_finalization = jobs_with_multiple_success == 0;
    checks.insert(
        "no_job_finalized_successful_more_than_once".into(),
        Value::Bool(no_stale_finalization),
    );
    if !no_stale_finalization {
        failures.push(format!(
            "{jobs_with_multiple_success} jobs have more than one SUCCEEDED attempt row \
             (a stale/reclaimed owner may have finalized after losing its lease)"
        ));
    }

    // 6. measured in-flight work never exceeded configured capacity.
    if let (Some(capacity), Some(peak)) = (input.worker_capacity, input.observed_peak_inflight) {
        let within_capacity = peak <= capacity;
        checks.insert(
            "peak_inflight_within_capacity".into(),
            Value::Bool(within_capacity),
        );
        checks.insert("observed_peak_inflight".into(), Value::from(peak));
        checks.insert("configured_capacity".into(), Value::from(capacity));
        if !within_capacity {
            failures.push(format!(
                "observed peak_inflight {peak} exceeded configured capacity {capacity}"
            ));
        }
    }

    // 7. wall-clock latency samples must be consistent with this run's
    // own monotonic elapsed time (generous 2x + 30s slack for submission
    // time, warmup, and poll-interval overhead outside the strict
    // measurement window) — catches a host sleep/wake spanning the run.
    if !input.latency_samples_ms.is_empty() && input.measurement_window_secs > 0.0 {
        let threshold_ms = input.measurement_window_secs * 1000.0 * 2.0 + 30_000.0;
        let max_sample = input
            .latency_samples_ms
            .iter()
            .cloned()
            .fold(0.0_f64, f64::max);
        let within_window = max_sample <= threshold_ms;
        checks.insert(
            "latency_samples_within_measurement_window".into(),
            Value::Bool(within_window),
        );
        if !within_window {
            failures.push(format!(
                "a latency sample of {max_sample:.0}ms far exceeds this run's own \
                 measurement window ({threshold_ms:.0}ms threshold) — likely the host \
                 slept/woke mid-run, corrupting wall-clock (not monotonic) timestamps"
            ));
        }
    }

    let passed = failures.is_empty();
    Ok(CorrectnessResults {
        passed,
        checks: Value::Object(checks.into_iter().collect()),
        failures,
    })
}
