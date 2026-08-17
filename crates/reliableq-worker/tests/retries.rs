//! M4: failure classification and retry scheduling, driven against a
//! real fake-charge with deterministic chaos injection (spec sec. 12).
//! See docs/failure-lab.md M4 for the tight-retry-then-backoff evidence.

use std::net::SocketAddr;
use std::time::Duration;

use reliableq_core::retry::RetryPolicy;
use reliableq_db::jobs;
use serde_json::json;
use sqlx::Executor;
use sqlx::postgres::{PgPool, PgPoolOptions};
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
            .max_connections(5)
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

    async fn set_chaos_fail_next(&self, n: u32, status: u16) {
        self.client
            .post(format!("{}/v1/test/control", self.charge_url))
            .json(&json!({ "mode": "fail_next", "n": n, "status": status }))
            .send()
            .await
            .expect("set chaos control")
            .error_for_status()
            .expect("control endpoint should accept the request");
    }

    async fn set_chaos_permanent_reject(&self) {
        self.client
            .post(format!("{}/v1/test/control", self.charge_url))
            .json(&json!({ "mode": "permanent_reject" }))
            .send()
            .await
            .expect("set chaos control")
            .error_for_status()
            .expect("control endpoint should accept the request");
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

fn charge_payload() -> serde_json::Value {
    json!({ "customer_id": "c-retry", "amount_cents": 250, "currency": "INR" })
}

async fn run_one_cycle(harness: &Harness, retry_policy: &RetryPolicy) -> usize {
    let claimed =
        jobs::claim_pending_jobs(&harness.pool, "test-worker", 10, Duration::from_secs(30))
            .await
            .expect("claim");
    let count = claimed.len();
    for job in claimed {
        reliableq_worker::execute_and_finalize(
            &harness.pool,
            &harness.client,
            &harness.charge_url,
            retry_policy,
            job,
        )
        .await;
    }
    count
}

/// A transient failure (503) with retry budget remaining must go back
/// to PENDING with a future next_attempt_at, not straight to DEAD.
#[tokio::test]
async fn transient_failure_reschedules_instead_of_dying() {
    let harness = Harness::new().await;
    harness.set_chaos_fail_next(1, 503).await;

    let id = Uuid::new_v4();
    jobs::insert_job(&harness.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");

    let claimed_count = run_one_cycle(&harness, &RetryPolicy::DEFAULT).await;
    assert_eq!(claimed_count, 1);

    let job = jobs::get_job_by_id(&harness.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(
        job.status().unwrap(),
        reliableq_core::domain::JobStatus::Pending,
        "a transient failure with budget left must reschedule, not die"
    );
    assert!(
        job.next_attempt_at > job.created_at,
        "next_attempt_at must be pushed into the future"
    );
    assert_eq!(job.last_error_code.as_deref(), Some("DOWNSTREAM_REJECTED"));
}

/// A permanent rejection (422) must go straight to DEAD after exactly
/// one attempt, regardless of remaining budget.
#[tokio::test]
async fn permanent_failure_goes_dead_after_one_attempt() {
    let harness = Harness::new().await;
    harness.set_chaos_permanent_reject().await;

    let id = Uuid::new_v4();
    jobs::insert_job(&harness.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");

    run_one_cycle(&harness, &RetryPolicy::DEFAULT).await;

    let job = jobs::get_job_by_id(&harness.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(
        job.status().unwrap(),
        reliableq_core::domain::JobStatus::Dead
    );
    assert_eq!(job.attempts, 1, "a permanent failure must not retry");
}

/// Exhausting the retry budget on repeated transient failures must
/// eventually land on DEAD with a budget-exhaustion reason, not retry
/// forever.
#[tokio::test]
async fn transient_failures_exhausting_budget_eventually_die() {
    let harness = Harness::new().await;
    harness.set_chaos_fail_next(10, 503).await; // more than max_attempts

    let id = Uuid::new_v4();
    jobs::insert_job(&harness.pool, id, "charge", &charge_payload(), 3)
        .await
        .expect("insert");

    for _ in 0..3 {
        sqlx::query("UPDATE jobs SET next_attempt_at = now() WHERE id = $1")
            .bind(id)
            .execute(&harness.pool)
            .await
            .expect("force job due");
        run_one_cycle(&harness, &RetryPolicy::DEFAULT).await;
    }

    let job = jobs::get_job_by_id(&harness.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(
        job.status().unwrap(),
        reliableq_core::domain::JobStatus::Dead
    );
    assert_eq!(job.attempts, 3);
    assert_eq!(
        job.last_error_code.as_deref(),
        Some("RETRY_BUDGET_EXHAUSTED")
    );
}
