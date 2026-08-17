//! M5: a dead job that already charged before dying must not
//! double-charge when an operator retries it — a direct consequence of
//! ADR 0004's job-scoped (not attempt-scoped) idempotency key, since
//! `POST /v1/jobs/{id}/retry` reuses the same job ID.

use std::net::SocketAddr;
use std::time::Duration;

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
            false,
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
    json!({ "customer_id": "c-replay", "amount_cents": 777, "currency": "INR" })
}

#[tokio::test]
async fn retrying_a_dead_job_that_already_charged_replays_not_duplicates() {
    let harness = Harness::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&harness.pool, id, "charge", &charge_payload(), 1)
        .await
        .expect("insert");

    // Attempt 1 charges successfully...
    let outcome = reliableq_worker::execute_charge(
        &harness.client,
        &harness.charge_url,
        id,
        &charge_payload(),
    )
    .await;
    assert!(outcome.is_ok());

    // ...but the job is forced DEAD anyway (simulating some unrelated
    // permanent failure discovered after the charge already committed
    // — e.g. a downstream validation step that runs after payment).
    sqlx::query(
        "UPDATE jobs SET status = 'DEAD', finished_at = now(), \
         last_error_code = 'PERMANENT', last_error_message = 'simulated' WHERE id = $1",
    )
    .bind(id)
    .execute(&harness.pool)
    .await
    .expect("force dead");

    let retried = jobs::retry_dead_job(&harness.pool, id, 5)
        .await
        .expect("retry")
        .expect("job was dead, retry should apply");
    assert_eq!(
        retried.status().unwrap(),
        reliableq_core::domain::JobStatus::Pending
    );

    // The operator's retry causes re-execution — same job ID, same
    // deterministic idempotency key.
    let outcome2 = reliableq_worker::execute_charge(
        &harness.client,
        &harness.charge_url,
        id,
        &charge_payload(),
    )
    .await;
    assert!(
        outcome2.is_ok(),
        "replay must still report success to the caller"
    );

    let charge_count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges")
        .fetch_one(&harness.pool)
        .await
        .expect("count charges");
    assert_eq!(
        charge_count, 1,
        "manual retry of an already-charged dead job must not double-charge"
    );
}
