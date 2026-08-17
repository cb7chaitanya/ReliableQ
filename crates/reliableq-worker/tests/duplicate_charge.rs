//! M3: reproduce the duplicate-charge window left open by M2's ADR
//! 0003 ("What this still cannot guarantee"), then (after the fix
//! lands) prove re-execution replays instead of duplicating. See
//! docs/failure-lab.md M3 for the before/after evidence.
//!
//! fake-charge runs in-process on an OS-assigned port; the two charge
//! attempts are driven directly through `reliableq_worker::execute_charge`
//! so the test controls exactly when each attempt happens, with no
//! sleep-based waiting for anything to happen.

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
            fake_charge::AppState { db: pool.clone() },
            64 * 1024,
            Duration::from_secs(10),
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
    json!({ "customer_id": "c-dup", "amount_cents": 900, "currency": "INR" })
}

/// The scenario ADR 0003 named as still-open: attempt 1's charge call
/// commits, but the worker "crashes" before finalizing (we simply never
/// call finalize). The lease expires, a second worker reclaims the job
/// and retries the charge call. Post-fix, this must produce exactly one
/// charge row (a replay), not two.
#[tokio::test]
async fn crash_after_charge_before_finalize_then_retry_produces_one_charge() {
    let harness = Harness::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&harness.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");

    // Attempt 1: claim with a near-instant lease, execute the charge
    // call, then do NOT finalize — simulating a crash right after the
    // effect committed.
    let claimed1 =
        jobs::claim_pending_jobs(&harness.pool, "worker-a", 10, Duration::from_millis(1))
            .await
            .expect("worker a claim");
    assert_eq!(claimed1.len(), 1);
    let outcome1 = reliableq_worker::execute_charge(
        &harness.client,
        &harness.charge_url,
        id,
        &claimed1[0].job.payload,
    )
    .await;
    assert!(outcome1.is_ok(), "first charge attempt should succeed");

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Attempt 2: a new worker reclaims the abandoned job and retries.
    let claimed2 = jobs::claim_pending_jobs(&harness.pool, "worker-b", 10, Duration::from_secs(30))
        .await
        .expect("worker b claim");
    assert_eq!(claimed2.len(), 1, "expired lease must be reclaimable (M2)");
    let outcome2 = reliableq_worker::execute_charge(
        &harness.client,
        &harness.charge_url,
        id,
        &claimed2[0].job.payload,
    )
    .await;
    assert!(
        outcome2.is_ok(),
        "second charge attempt must also succeed — as a replay, not a new charge"
    );

    let charge_count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges")
        .fetch_one(&harness.pool)
        .await
        .expect("count charges");
    assert_eq!(
        charge_count, 1,
        "re-executing the same job must produce exactly one charge (invariant 12)"
    );
}
