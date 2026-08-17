//! End-to-end happy path (spec sec. 17 "End-to-end tests"): submit a
//! charge job through the real API, drive one worker poll cycle against
//! the real fake-charge service, and assert the job reaches SUCCEEDED
//! with exactly one persisted charge.
//!
//! api and fake-charge run in-process on OS-assigned ports; the worker
//! poll cycle is driven directly (`reliableq_db::jobs::claim_pending_jobs`
//! plus `reliableq_worker::execute_and_finalize`) instead of spawning
//! the worker binary, so the test has no sleep-based polling for work
//! to appear — it drives exactly one cycle and asserts on the result.

use std::net::SocketAddr;
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::Executor;
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;

struct Harness {
    pool: PgPool,
    admin_pool: PgPool,
    schema: String,
    api_addr: SocketAddr,
    charge_addr: SocketAddr,
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
            .max_connections(10)
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
            false,
        ))
        .await;

        let api_addr = spawn_app(reliableq_api::build_app(
            reliableq_api::AppState { db: pool.clone() },
            64 * 1024,
            Duration::from_secs(10),
        ))
        .await;

        Self {
            pool,
            admin_pool,
            schema,
            api_addr,
            charge_addr,
            client: reqwest::Client::new(),
        }
    }

    async fn submit_job(&self, payload: Value) -> Value {
        self.client
            .post(format!("http://{}/v1/jobs", self.api_addr))
            .json(&payload)
            .send()
            .await
            .expect("submit request")
            .json::<Value>()
            .await
            .expect("submit response should be json")
    }

    async fn get_job(&self, id: &str) -> Value {
        self.client
            .get(format!("http://{}/v1/jobs/{id}", self.api_addr))
            .send()
            .await
            .expect("get request")
            .json::<Value>()
            .await
            .expect("get response should be json")
    }

    /// Drives exactly one worker poll cycle in-process: claim, then
    /// execute+finalize every claimed job.
    async fn run_one_worker_cycle(&self) -> usize {
        let claimed = reliableq_db::jobs::claim_pending_jobs(
            &self.pool,
            "test-worker",
            10,
            Duration::from_secs(30),
        )
        .await
        .expect("claim");
        let count = claimed.len();
        let charge_url = format!("http://{}", self.charge_addr);
        let retry_policy = reliableq_core::retry::RetryPolicy::DEFAULT;
        for job in claimed {
            reliableq_worker::execute_and_finalize(
                &self.pool,
                &self.client,
                &charge_url,
                &retry_policy,
                job,
            )
            .await;
        }
        count
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

fn charge_job(customer_id: &str, amount_cents: i64, currency: &str) -> Value {
    json!({
        "kind": "charge",
        "payload": { "customer_id": customer_id, "amount_cents": amount_cents, "currency": currency },
        "max_attempts": 5,
    })
}

#[tokio::test]
async fn submitted_job_succeeds_and_persists_exactly_one_charge() {
    let harness = Harness::new().await;

    let submitted = harness.submit_job(charge_job("c-happy", 4200, "USD")).await;
    let id = submitted["id"].as_str().expect("id present").to_string();
    assert_eq!(submitted["status"], "PENDING");

    let claimed_count = harness.run_one_worker_cycle().await;
    assert_eq!(claimed_count, 1, "the one submitted job should be claimed");

    let job = harness.get_job(&id).await;
    assert_eq!(job["status"], "SUCCEEDED");
    assert_eq!(job["attempts"][0]["outcome"], "SUCCEEDED");
    assert_eq!(job["attempts"].as_array().unwrap().len(), 1);

    let charge_count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges")
        .fetch_one(&harness.pool)
        .await
        .expect("count charges");
    assert_eq!(
        charge_count, 1,
        "exactly one charge must exist for the one succeeded job"
    );
}
