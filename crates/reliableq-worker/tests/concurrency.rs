//! M6: bounded concurrency, measured against a real fake-charge whose
//! response is artificially delayed (spec sec. 12 chaos injection) so
//! that concurrent load actually overlaps for long enough to measure.
//! See docs/failure-lab.md M6 for the unbounded-vs-bounded evidence.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use reliableq_core::retry::RetryPolicy;
use reliableq_db::jobs;
use serde_json::json;
use sqlx::Executor;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::Semaphore;
use uuid::Uuid;

struct Harness {
    pool: PgPool,
    admin_pool: PgPool,
    schema: String,
    charge_url: String,
    client: reqwest::Client,
}

impl Harness {
    async fn new() -> Self {
        let database_url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must point at a reachable postgres instance");
        let schema = format!("reliableq_test_{}", Uuid::new_v4().simple());

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect admin pool");
        admin_pool
            .execute(format!(r#"CREATE SCHEMA "{schema}""#).as_str())
            .await
            .expect("create isolated test schema");

        let schema_for_hook = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .after_connect(move |conn, _meta| {
                let schema = schema_for_hook.clone();
                Box::pin(async move {
                    conn.execute(format!(r#"SET search_path TO "{schema}""#).as_str())
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .expect("connect scoped pool");

        reliableq_db::run_migrations(&pool)
            .await
            .expect("migrations should apply cleanly");

        let charge_addr = spawn_app(fake_charge::build_app(
            fake_charge::AppState {
                db: pool.clone(),
                chaos: fake_charge::chaos::ChaosState::default(),
            },
            64 * 1024,
            Duration::from_secs(10),
            true,
        ))
        .await;

        Self {
            pool,
            admin_pool,
            schema,
            charge_url: format!("http://{charge_addr}"),
            client: reqwest::Client::new(),
        }
    }

    async fn set_delay(&self, ms: u64) {
        self.client
            .post(format!("{}/v1/test/control", self.charge_url))
            .json(&json!({ "mode": "delay_ms", "ms": ms }))
            .send()
            .await
            .expect("set delay")
            .error_for_status()
            .expect("control endpoint should accept the request");
    }

    async fn reset_peak_inflight(&self) {
        self.client
            .post(format!("{}/v1/test/inflight/reset", self.charge_url))
            .send()
            .await
            .expect("reset peak inflight")
            .error_for_status()
            .expect("reset should succeed");
    }

    async fn peak_inflight(&self) -> usize {
        let body: serde_json::Value = self
            .client
            .get(format!("{}/v1/test/inflight", self.charge_url))
            .send()
            .await
            .expect("get peak inflight")
            .json()
            .await
            .expect("valid json");
        body["peak_inflight"].as_u64().expect("peak_inflight field") as usize
    }

    async fn submit_n_jobs(&self, n: usize) {
        for i in 0..n {
            let id = Uuid::new_v4();
            jobs::insert_job(
                &self.pool,
                id,
                "charge",
                &json!({ "customer_id": format!("c-{i}"), "amount_cents": 100, "currency": "INR" }),
                5,
            )
            .await
            .expect("insert");
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let admin_pool = self.admin_pool.clone();
        let schema = self.schema.clone();
        tokio::spawn(async move {
            let _ = admin_pool
                .execute(format!(r#"DROP SCHEMA "{schema}" CASCADE"#).as_str())
                .await;
        });
    }
}

async fn spawn_app(app: axum::Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should not fail");
    });
    addr
}

const JOB_COUNT: usize = 20;
const DELAY_MS: u64 = 150;

/// Reproduction: claiming a batch and spawning every job's execution
/// with no concurrency bound at all lets every one of them be in
/// flight against the (artificially slow) downstream simultaneously.
#[tokio::test]
async fn unbounded_spawning_lets_every_claimed_job_run_concurrently() {
    let harness = Harness::new().await;
    harness.set_delay(DELAY_MS).await;
    harness.submit_n_jobs(JOB_COUNT).await;
    harness.reset_peak_inflight().await;

    let claimed = jobs::claim_pending_jobs(&harness.pool, "worker-a", 50, Duration::from_secs(30))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), JOB_COUNT);

    // Deliberately naive: no semaphore, spawn everything immediately.
    let retry_policy = RetryPolicy::DEFAULT;
    let mut handles = Vec::new();
    for job in claimed {
        let pool = harness.pool.clone();
        let client = harness.client.clone();
        let url = harness.charge_url.clone();
        handles.push(tokio::spawn(async move {
            reliableq_worker::execute_and_finalize(
                &pool,
                &client,
                &url,
                &retry_policy,
                Duration::from_secs(30),
                job,
            )
            .await;
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }

    let peak = harness.peak_inflight().await;
    assert_eq!(
        peak, JOB_COUNT,
        "with no bound at all, every claimed job runs concurrently"
    );
}

/// The fix: spawn_bounded_batch never lets more than `concurrency`
/// jobs be in flight, regardless of how many were claimed.
#[tokio::test]
async fn bounded_batch_never_exceeds_configured_concurrency() {
    let harness = Harness::new().await;
    harness.set_delay(DELAY_MS).await;
    harness.submit_n_jobs(JOB_COUNT).await;
    harness.reset_peak_inflight().await;

    let claimed = jobs::claim_pending_jobs(&harness.pool, "worker-a", 50, Duration::from_secs(30))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), JOB_COUNT);

    const CONCURRENCY: usize = 4;
    let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
    let handles = reliableq_worker::spawn_bounded_batch(
        harness.pool.clone(),
        harness.client.clone(),
        harness.charge_url.clone(),
        RetryPolicy::DEFAULT,
        Duration::from_secs(30),
        semaphore,
        claimed,
    );
    for handle in handles {
        let _ = handle.await;
    }

    let peak = harness.peak_inflight().await;
    assert!(
        peak <= CONCURRENCY,
        "peak in-flight ({peak}) must never exceed the configured bound ({CONCURRENCY})"
    );
    assert!(
        peak > 1,
        "sanity: the delay must be long enough to observe real overlap"
    );

    let succeeded: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED'")
        .fetch_one(&harness.pool)
        .await
        .expect("count succeeded");
    assert_eq!(
        succeeded, JOB_COUNT as i64,
        "bounding concurrency must not drop or fail any job"
    );
}
