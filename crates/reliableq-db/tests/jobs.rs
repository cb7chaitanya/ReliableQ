//! Repository tests against a live, isolated PostgreSQL schema.
//!
//! Each test owns a fresh schema (spec sec. 19: "tests own isolated
//! database schemas/databases and clean up safely") so concurrent test
//! runs and claim-locking behavior cannot interfere with each other.

use std::time::Duration;

use reliableq_core::domain::JobStatus;
use reliableq_db::jobs::{self, RepoError};
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
        // Best-effort cleanup: spawn so Drop stays sync. Test process
        // exit also reclaims the schema; this just keeps a long-lived
        // dev database tidy between runs.
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

#[tokio::test]
async fn insert_and_get_round_trip() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    let inserted = jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");

    assert_eq!(inserted.id, id);
    assert_eq!(inserted.status().unwrap(), JobStatus::Pending);
    assert_eq!(inserted.attempts, 0);
    assert_eq!(inserted.max_attempts, 5);
    assert!(inserted.lease_token.is_none());
    assert!(inserted.finished_at.is_none());

    let fetched = jobs::get_job_by_id(&db.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(fetched.id, id);
    assert_eq!(fetched.kind, "charge");
}

#[tokio::test]
async fn get_missing_job_returns_none() {
    let db = TestDb::new().await;
    let missing = jobs::get_job_by_id(&db.pool, Uuid::new_v4())
        .await
        .expect("query should succeed");
    assert!(missing.is_none());
}

#[tokio::test]
async fn list_jobs_paginates_in_created_order() {
    let db = TestDb::new().await;
    let mut ids = Vec::new();
    for _ in 0..5 {
        let id = Uuid::new_v4();
        jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
            .await
            .expect("insert");
        ids.push(id);
        // Ensure distinct created_at ordering even at high insert rates.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let first_page = jobs::list_jobs(&db.pool, Some(JobStatus::Pending), 2, None)
        .await
        .expect("list page 1");
    assert_eq!(first_page.len(), 2);
    assert_eq!(first_page[0].id, ids[0]);
    assert_eq!(first_page[1].id, ids[1]);

    let cursor = (first_page[1].created_at, first_page[1].id);
    let second_page = jobs::list_jobs(&db.pool, Some(JobStatus::Pending), 2, Some(cursor))
        .await
        .expect("list page 2");
    assert_eq!(second_page.len(), 2);
    assert_eq!(second_page[0].id, ids[2]);
    assert_eq!(second_page[1].id, ids[3]);
}

#[tokio::test]
async fn claim_only_returns_due_pending_jobs() {
    let db = TestDb::new().await;
    let due_id = Uuid::new_v4();
    jobs::insert_job(&db.pool, due_id, "charge", &charge_payload(), 5)
        .await
        .expect("insert due job");

    let future_id = Uuid::new_v4();
    jobs::insert_job(&db.pool, future_id, "charge", &charge_payload(), 5)
        .await
        .expect("insert future job");
    sqlx::query("UPDATE jobs SET next_attempt_at = now() + interval '1 hour' WHERE id = $1")
        .bind(future_id)
        .execute(&db.pool)
        .await
        .expect("push future job's next_attempt_at out");

    let claimed = jobs::claim_pending_jobs(&db.pool, "worker-1", 10, Duration::from_secs(30))
        .await
        .expect("claim");

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, due_id);
    assert_eq!(claimed[0].attempt_number, 1);
    assert_eq!(claimed[0].job.status().unwrap(), JobStatus::Running);
    assert!(claimed[0].job.lease_token.is_some());
    assert!(claimed[0].job.lease_expires_at.is_some());
    assert!(claimed[0].job.started_at.is_some());
}

#[tokio::test]
async fn claim_never_returns_the_same_job_to_two_concurrent_claimants() {
    let db = TestDb::new().await;
    for _ in 0..20 {
        let id = Uuid::new_v4();
        jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
            .await
            .expect("insert");
    }

    let (a, b) = tokio::join!(
        jobs::claim_pending_jobs(&db.pool, "worker-a", 15, Duration::from_secs(30)),
        jobs::claim_pending_jobs(&db.pool, "worker-b", 15, Duration::from_secs(30)),
    );
    let a = a.expect("claim a");
    let b = b.expect("claim b");

    let a_ids: std::collections::HashSet<_> = a.iter().map(|c| c.job.id).collect();
    let b_ids: std::collections::HashSet<_> = b.iter().map(|c| c.job.id).collect();
    assert!(
        a_ids.is_disjoint(&b_ids),
        "SKIP LOCKED must prevent two workers from claiming the same job"
    );
    assert_eq!(
        a.len() + b.len(),
        20,
        "every job must be claimed exactly once"
    );
}

#[tokio::test]
async fn claim_creates_a_matching_attempt_row() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");

    let claimed = jobs::claim_pending_jobs(&db.pool, "worker-1", 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);

    let attempt_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM job_attempts WHERE job_id = $1")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("count attempts");
    assert_eq!(attempt_count, 1);

    let (attempt_number, worker_id, outcome): (i32, String, Option<String>) = sqlx::query_as(
        "SELECT attempt_number, worker_id, outcome FROM job_attempts WHERE job_id = $1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .expect("fetch attempt");
    assert_eq!(attempt_number, 1);
    assert_eq!(worker_id, "worker-1");
    assert_eq!(outcome, None, "outcome is set only at finalization");
}

#[tokio::test]
async fn finalize_success_transitions_job_and_attempt() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    let claimed = jobs::claim_pending_jobs(&db.pool, "worker-1", 10, Duration::from_secs(30))
        .await
        .expect("claim");
    let lease_token = claimed[0].job.lease_token.expect("lease token present");

    let ok = jobs::finalize_success(&db.pool, id, lease_token, 42)
        .await
        .expect("finalize");
    assert!(ok);

    let job = jobs::get_job_by_id(&db.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(job.status().unwrap(), JobStatus::Succeeded);
    assert!(job.lease_token.is_none());
    assert!(job.finished_at.is_some());

    let (outcome, duration_ms): (Option<String>, Option<i64>) =
        sqlx::query_as("SELECT outcome, duration_ms FROM job_attempts WHERE job_id = $1")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("fetch attempt");
    assert_eq!(outcome.as_deref(), Some("SUCCEEDED"));
    assert_eq!(duration_ms, Some(42));
}

#[tokio::test]
async fn finalize_dead_transitions_job_and_records_error() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    let claimed = jobs::claim_pending_jobs(&db.pool, "worker-1", 10, Duration::from_secs(30))
        .await
        .expect("claim");
    let lease_token = claimed[0].job.lease_token.expect("lease token present");

    let ok = jobs::finalize_dead(&db.pool, id, lease_token, "PERMANENT", "rejected", 7)
        .await
        .expect("finalize");
    assert!(ok);

    let job = jobs::get_job_by_id(&db.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(job.status().unwrap(), JobStatus::Dead);
    assert_eq!(job.last_error_code.as_deref(), Some("PERMANENT"));
    assert!(job.lease_token.is_none());
    assert!(job.finished_at.is_some());
}

#[tokio::test]
async fn stale_lease_token_cannot_finalize() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    jobs::claim_pending_jobs(&db.pool, "worker-1", 10, Duration::from_secs(30))
        .await
        .expect("claim");

    let stale_token = Uuid::new_v4();
    let ok = jobs::finalize_success(&db.pool, id, stale_token, 1)
        .await
        .expect("finalize call should succeed even though it matches nothing");
    assert!(
        !ok,
        "a stale/wrong lease token must not be able to finalize"
    );

    let job = jobs::get_job_by_id(&db.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(
        job.status().unwrap(),
        JobStatus::Running,
        "job must remain untouched by the rejected finalize"
    );
}

#[tokio::test]
async fn dead_jobs_are_not_claimable() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 1)
        .await
        .expect("insert");
    let claimed = jobs::claim_pending_jobs(&db.pool, "worker-1", 10, Duration::from_secs(30))
        .await
        .expect("claim");
    let lease_token = claimed[0].job.lease_token.expect("lease token present");
    jobs::finalize_dead(&db.pool, id, lease_token, "PERMANENT", "no", 1)
        .await
        .expect("finalize dead");

    let reclaimed = jobs::claim_pending_jobs(&db.pool, "worker-2", 10, Duration::from_secs(30))
        .await
        .expect("claim after dead");
    assert!(
        reclaimed.iter().all(|c| c.job.id != id),
        "a DEAD job must never be claimable"
    );
}

#[tokio::test]
async fn status_parses_via_typed_error_not_panic() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    let row = jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    match row.status() {
        Ok(JobStatus::Pending) => {}
        other => panic!("expected Ok(Pending), got {other:?}"),
    }
}

#[allow(unused)]
fn assert_repo_error_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RepoError>();
}

#[tokio::test]
async fn finalize_retry_scheduled_reschedules_and_clears_lease() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    let claimed = jobs::claim_pending_jobs(&db.pool, "worker-1", 10, Duration::from_secs(30))
        .await
        .expect("claim");
    let lease_token = claimed[0].job.lease_token.expect("lease token");

    let ok = jobs::finalize_retry_scheduled(
        &db.pool,
        id,
        lease_token,
        30.0,
        "TRANSIENT",
        "simulated 503",
        5,
    )
    .await
    .expect("finalize retry");
    assert!(ok);

    let job = jobs::get_job_by_id(&db.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(job.status().unwrap(), JobStatus::Pending);
    assert!(job.lease_token.is_none());
    assert_eq!(job.last_error_code.as_deref(), Some("TRANSIENT"));
    assert!(
        job.next_attempt_at > job.created_at,
        "next_attempt_at must be pushed into the future by the scheduled delay"
    );

    let (outcome, scheduled_delay_ms): (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT outcome, scheduled_delay_ms FROM job_attempts WHERE job_id = $1 AND attempt_number = 1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .expect("fetch attempt");
    assert_eq!(outcome.as_deref(), Some("RETRY_SCHEDULED"));
    assert_eq!(scheduled_delay_ms, Some(30_000));
}

/// Demonstrates the exact danger a "tight" (zero-delay) retry policy
/// creates: nothing in the repository layer stops a delay of zero from
/// making a job immediately due again. This is real behavior of a real
/// primitive, not a hypothetical — it's why reliableq-worker's actual
/// policy (reliableq_core::retry::RetryPolicy::DEFAULT) always has a
/// non-zero base_delay, and why full jitter's *cap* per attempt, not
/// the delay floor, is what bounds retry load. See docs/failure-lab.md
/// M4.
#[tokio::test]
async fn zero_delay_retry_scheduling_makes_a_job_immediately_reclaimable() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    jobs::insert_job(&db.pool, id, "charge", &charge_payload(), 5)
        .await
        .expect("insert");
    let claimed = jobs::claim_pending_jobs(&db.pool, "worker-a", 10, Duration::from_secs(30))
        .await
        .expect("claim");
    let lease_token = claimed[0].job.lease_token.expect("lease token");

    jobs::finalize_retry_scheduled(&db.pool, id, lease_token, 0.0, "TRANSIENT", "simulated", 1)
        .await
        .expect("finalize retry with zero delay");

    // No sleep at all: if a zero-delay retry is claimable immediately,
    // that is exactly the thundering-herd risk a real backoff policy
    // must prevent by never producing a zero (or near-zero) delay.
    let reclaimed = jobs::claim_pending_jobs(&db.pool, "worker-b", 10, Duration::from_secs(30))
        .await
        .expect("claim immediately after a zero-delay retry");
    assert_eq!(
        reclaimed.len(),
        1,
        "a zero-delay retry is due again with no backoff at all"
    );
}
