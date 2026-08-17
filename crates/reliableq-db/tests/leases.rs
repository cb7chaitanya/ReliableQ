//! M2: expired-lease reclaim and fencing, against a live isolated
//! PostgreSQL schema. See docs/failure-lab.md M2 for the reproduction
//! evidence (the first test below failed before this milestone's fix).

use std::time::Duration;

use reliableq_core::domain::JobStatus;
use reliableq_db::jobs;
use serde_json::json;
use sqlx::Executor;
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;

struct TestDb {
    pool: PgPool,
    admin_pool: PgPool,
    schema: String,
}

impl TestDb {
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

        Self {
            pool,
            admin_pool,
            schema,
        }
    }
}

impl Drop for TestDb {
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

fn charge_payload() -> serde_json::Value {
    json!({ "customer_id": "c1", "amount_cents": 500, "currency": "INR" })
}

/// Reproduction: a worker claims a job, then vanishes (crash) before
/// finalizing. Before this milestone's fix, claim_pending_jobs only
/// ever matched `status = 'PENDING'`, so an expired `RUNNING` lease was
/// invisible to every future claim — the job was stranded forever.
#[tokio::test]
async fn expired_lease_is_reclaimable_by_a_new_worker() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");

    // Worker A claims with a lease so short it is already expired by
    // the time we check.
    let claimed_a = jobs::claim_pending_jobs(&db.pool, "worker-a", 10, Duration::from_millis(1))
        .await
        .expect("worker a claim");
    assert_eq!(claimed_a.len(), 1);
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Worker A crashed here, forever. Worker B polls and must be able
    // to reclaim the abandoned job now that its lease has expired.
    let claimed_b = jobs::claim_pending_jobs(&db.pool, "worker-b", 10, Duration::from_secs(30))
        .await
        .expect("worker b claim");
    assert_eq!(
        claimed_b.len(),
        1,
        "an expired RUNNING lease must be reclaimable by a new worker"
    );
    assert_eq!(claimed_b[0].job.id, id);
    assert_eq!(
        claimed_b[0].attempt_number, 2,
        "reclaiming must record a new, incremented attempt"
    );
    assert_ne!(
        claimed_b[0].job.lease_token, claimed_a[0].job.lease_token,
        "reclaim must issue a fresh lease token"
    );
}

/// The abandoned attempt's row must be closed out as LEASE_LOST, not
/// left with outcome = NULL forever.
#[tokio::test]
async fn reclaim_marks_the_abandoned_attempt_as_lease_lost() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    jobs::claim_pending_jobs(&db.pool, "worker-a", 10, Duration::from_millis(1))
        .await
        .expect("worker a claim");
    tokio::time::sleep(Duration::from_millis(20)).await;
    jobs::claim_pending_jobs(&db.pool, "worker-b", 10, Duration::from_secs(30))
        .await
        .expect("worker b claim");

    let (attempt_number, outcome): (i32, Option<String>) = sqlx::query_as(
        "SELECT attempt_number, outcome FROM job_attempts WHERE job_id = $1 AND attempt_number = 1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .expect("fetch attempt 1");
    assert_eq!(attempt_number, 1);
    assert_eq!(outcome.as_deref(), Some("LEASE_LOST"));
}

/// Fencing: the crashed worker's stale token must not be able to
/// finalize a job that a new owner has since reclaimed.
#[tokio::test]
async fn stale_worker_cannot_finalize_after_reclaim() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    let claimed_a = jobs::claim_pending_jobs(&db.pool, "worker-a", 10, Duration::from_millis(1))
        .await
        .expect("worker a claim");
    let stale_token = claimed_a[0].job.lease_token.expect("lease token");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let claimed_b = jobs::claim_pending_jobs(&db.pool, "worker-b", 10, Duration::from_secs(30))
        .await
        .expect("worker b claim");
    let current_token = claimed_b[0].job.lease_token.expect("lease token");
    assert_ne!(stale_token, current_token);

    let stale_ok = jobs::finalize_success(&db.pool, id, stale_token, 5)
        .await
        .expect("finalize call");
    assert!(
        !stale_ok,
        "worker A's stale lease token must not be able to finalize"
    );

    let current_ok = jobs::finalize_success(&db.pool, id, current_token, 5)
        .await
        .expect("finalize call");
    assert!(
        current_ok,
        "worker B, the current owner, must be able to finalize"
    );

    let job = jobs::get_job_by_id(&db.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(job.status().unwrap(), JobStatus::Succeeded);
}

/// Two workers racing to reclaim the same expired lease must not both
/// win — SKIP LOCKED must apply to the reclaim branch exactly as it
/// does to the PENDING branch.
#[tokio::test]
async fn two_workers_racing_to_reclaim_dont_double_claim() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    jobs::claim_pending_jobs(&db.pool, "worker-a", 10, Duration::from_millis(1))
        .await
        .expect("worker a claim");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let (b, c) = tokio::join!(
        jobs::claim_pending_jobs(&db.pool, "worker-b", 10, Duration::from_secs(30)),
        jobs::claim_pending_jobs(&db.pool, "worker-c", 10, Duration::from_secs(30)),
    );
    let b = b.expect("worker b claim");
    let c = c.expect("worker c claim");
    assert_eq!(
        b.len() + c.len(),
        1,
        "exactly one of two racing reclaimers should win"
    );
}

/// A job that has already exhausted its retry budget when its lease
/// expires must not be left stranded RUNNING forever just because it
/// can no longer be reclaimed: it must transition to DEAD.
#[tokio::test]
async fn expired_lease_with_exhausted_budget_becomes_dead_not_stranded() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    // max_attempts = 1: the single claim below already consumes the
    // entire budget.
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 1)
        .await
        .expect("insert");
    jobs::claim_pending_jobs(&db.pool, "worker-a", 10, Duration::from_millis(1))
        .await
        .expect("worker a claim");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let claimed = jobs::claim_pending_jobs(&db.pool, "worker-b", 10, Duration::from_secs(30))
        .await
        .expect("claim after exhaustion");
    assert!(
        claimed.is_empty(),
        "an exhausted job must not be claimable again"
    );

    let job = jobs::get_job_by_id(&db.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(
        job.status().unwrap(),
        JobStatus::Dead,
        "it must be DEAD, not left stranded RUNNING with an expired lease"
    );
    assert!(job.lease_token.is_none());
    assert!(job.finished_at.is_some());
}
