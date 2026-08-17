//! Preload/query helpers shared by scenarios: submitting jobs through the
//! real `POST /v1/jobs` contract, and polling PostgreSQL directly for
//! queue depth / drain / per-job timing — never through the lagging
//! (5s-refresh) `/metrics` gauges, per docs/benchmarking/design.md sec. 8.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::Semaphore;
use uuid::Uuid;

pub fn charge_payload(scenario: &str, i: u64) -> Value {
    json!({
        "customer_id": format!("bench-{scenario}-{i}"),
        "amount_cents": 100 + (i % 5000) as i64,
        "currency": "INR",
    })
}

#[derive(Debug, Clone)]
pub struct SubmitOutcome {
    pub elapsed_ms: f64,
    pub status: u16,
    pub id: Option<Uuid>,
}

pub async fn submit_job(
    client: &reqwest::Client,
    api_base: &str,
    scenario: &str,
    i: u64,
    max_attempts: i32,
) -> SubmitOutcome {
    let body = json!({
        "kind": "charge",
        "payload": charge_payload(scenario, i),
        "max_attempts": max_attempts,
    });
    let start = Instant::now();
    let outcome = client
        .post(format!("{api_base}/v1/jobs"))
        .json(&body)
        .send()
        .await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    match outcome {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let id = if status == 202 {
                resp.json::<Value>()
                    .await
                    .ok()
                    .and_then(|v| v["id"].as_str().and_then(|s| Uuid::parse_str(s).ok()))
            } else {
                None
            };
            SubmitOutcome {
                elapsed_ms,
                status,
                id,
            }
        }
        Err(_) => SubmitOutcome {
            elapsed_ms,
            status: 0,
            id: None,
        },
    }
}

/// Submits `count` jobs at the given client-side concurrency, counting
/// only requests that returned `202` as durable submissions (SPEC-BENCH
/// sec. "Submission metrics": "a request counts as successful only after
/// the API returns 202").
pub async fn submit_many(
    client: &reqwest::Client,
    api_base: &str,
    scenario: &str,
    start_index: u64,
    count: u64,
    concurrency: u32,
    max_attempts: i32,
) -> Vec<SubmitOutcome> {
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1) as usize));
    let mut set = tokio::task::JoinSet::new();
    for i in 0..count {
        let client = client.clone();
        let api_base = api_base.to_string();
        let scenario = scenario.to_string();
        let semaphore = semaphore.clone();
        set.spawn(async move {
            let _permit = semaphore.acquire_owned().await.expect("semaphore open");
            submit_job(&client, &api_base, &scenario, start_index + i, max_attempts).await
        });
    }
    let mut results = Vec::with_capacity(count as usize);
    while let Some(res) = set.join_next().await {
        if let Ok(outcome) = res {
            results.push(outcome);
        }
    }
    results
}

pub async fn queue_depth(pool: &PgPool) -> anyhow::Result<i64> {
    let depth: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status IN ('PENDING', 'RUNNING')")
            .fetch_one(pool)
            .await?;
    Ok(depth)
}

/// Polls queue depth until zero or `deadline` elapses. Returns `true` if
/// drained. Samples are pushed to `depth_samples` (queue depth, elapsed
/// seconds) for the backlog scenario's drain curve.
pub async fn wait_for_drain(
    pool: &PgPool,
    deadline: Duration,
    poll_interval: Duration,
    mut on_sample: impl FnMut(f64, i64),
) -> anyhow::Result<bool> {
    let start = Instant::now();
    loop {
        let depth = queue_depth(pool).await?;
        on_sample(start.elapsed().as_secs_f64(), depth);
        if depth == 0 {
            return Ok(true);
        }
        if start.elapsed() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// End-to-end latency (created_at -> finished_at) in ms for the given job
/// IDs that have reached a terminal state. Jobs still non-terminal are
/// skipped (the caller decides whether that's acceptable for the
/// scenario).
pub async fn end_to_end_latencies_ms(pool: &PgPool, ids: &[Uuid]) -> anyhow::Result<Vec<f64>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(f64,)> = sqlx::query_as(
        r#"
        SELECT EXTRACT(EPOCH FROM (finished_at - created_at))::float8 * 1000.0
        FROM jobs
        WHERE id = ANY($1) AND finished_at IS NOT NULL
        "#,
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(v,)| v).collect())
}
