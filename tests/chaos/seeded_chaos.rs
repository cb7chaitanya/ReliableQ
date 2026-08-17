//! Seeded chaos suite (spec sec. 17): >=100 jobs, multiple concurrent
//! simulated workers, randomized crash-point injection at all three
//! named failpoints, plus a burst of real transient downstream
//! failures — then, at quiescence, every invariant is checked directly
//! against PostgreSQL. See docs/failure-lab.md M8 and
//! docs/blog/05-chaos-results.md for the evidence this test produced.
//!
//! No sleep-based guessing for "is it done yet": claim/execute runs in
//! short deterministic rounds with a short lease duration, and
//! quiescence is polled with a bounded deadline. The seed is always
//! printed so a failure is reproducible.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reliableq_core::retry::RetryPolicy;
use reliableq_db::jobs;
use reliableq_worker::failpoint::{FailpointName, Failpoints};
use serde_json::json;
use sqlx::Executor;
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;

const SEED: u64 = 20260817;
const JOB_COUNT: usize = 120;
const WORKER_COUNT: usize = 3;
const LEASE_DURATION: Duration = Duration::from_millis(250);
const FAILPOINT_TRIGGER_PROBABILITY: f64 = 0.12;
const MAX_ROUNDS: usize = 200;
const QUIESCENCE_DEADLINE: Duration = Duration::from_secs(60);

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
        let schema = format!("reliableq_chaos_{}", Uuid::new_v4().simple());

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
            .max_connections(30)
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

    async fn reset_chaos(&self) {
        self.client
            .post(format!("{}/v1/test/control", self.charge_url))
            .json(&json!({ "mode": "normal" }))
            .send()
            .await
            .expect("reset chaos control")
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

fn charge_payload(i: usize) -> serde_json::Value {
    json!({ "customer_id": format!("chaos-c-{i}"), "amount_cents": 100 + (i as i64 % 500), "currency": "INR" })
}

/// Seeded, deterministic given the same seed: each call draws from a
/// shared RNG protected by a mutex (simulated workers run
/// concurrently). Triggers at all three named failpoints with equal
/// probability — this is exactly the "lease expires while the old
/// worker is paused" scenario when it hits AfterEffectBeforeFinalize:
/// the simulated worker "dies" right after the charge commits, the
/// short LEASE_DURATION expires, and a later round's claim reclaims
/// and retries — fencing (M2) plus the job-scoped idempotency key
/// (M3/ADR 0004) are what keep that retry from double-charging.
struct SeededChaosFailpoints {
    rng: StdMutex<StdRng>,
    probability: f64,
}

impl Failpoints for SeededChaosFailpoints {
    fn should_trigger(&self, _name: FailpointName, _job_id: Uuid) -> bool {
        let mut rng = self.rng.lock().unwrap_or_else(|e| e.into_inner());
        rng.gen_bool(self.probability)
    }
}

/// Runs one claim+execute round for one simulated worker: claims due
/// work, then executes each claimed job's charge call and finalize
/// with the shared chaos failpoints.
async fn run_one_round(
    pool: PgPool,
    client: reqwest::Client,
    charge_url: String,
    worker_id: String,
    failpoints: Arc<SeededChaosFailpoints>,
) -> usize {
    let claimed = jobs::claim_pending_jobs(&pool, &worker_id, 10, LEASE_DURATION)
        .await
        .expect("claim");
    let count = claimed.len();
    for job in claimed {
        reliableq_worker::execute_and_finalize_with_failpoints(
            &pool,
            &client,
            &charge_url,
            &RetryPolicy::DEFAULT,
            LEASE_DURATION,
            job,
            failpoints.as_ref(),
        )
        .await;
    }
    count
}

async fn is_quiescent(pool: &PgPool) -> bool {
    let non_terminal: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status IN ('PENDING', 'RUNNING')")
            .fetch_one(pool)
            .await
            .expect("count non-terminal jobs");
    non_terminal == 0
}

#[tokio::test]
async fn seeded_chaos_suite_converges_and_holds_every_invariant() {
    eprintln!("seeded_chaos_suite: SEED={SEED} JOB_COUNT={JOB_COUNT} WORKER_COUNT={WORKER_COUNT}");

    let harness = Harness::new().await;
    let failpoints = Arc::new(SeededChaosFailpoints {
        rng: StdMutex::new(StdRng::seed_from_u64(SEED)),
        probability: FAILPOINT_TRIGGER_PROBABILITY,
    });

    for i in 0..JOB_COUNT {
        let id = Uuid::new_v4();
        jobs::insert_job(&harness.pool, id, "charge", &charge_payload(i), 6)
            .await
            .expect("insert");
    }

    // A burst of real transient downstream failures early on, so some
    // jobs must genuinely retry against a degraded dependency (spec
    // sec. 17: "transient failures" as one of the injected conditions),
    // not just simulated worker crashes. Reset afterward so the run can
    // actually converge instead of exhausting everyone's retry budget.
    harness.set_chaos_fail_next(40, 503).await;

    let deadline = tokio::time::Instant::now() + QUIESCENCE_DEADLINE;
    let mut round = 0;
    let mut reached_quiescence = false;
    while round < MAX_ROUNDS && tokio::time::Instant::now() < deadline {
        round += 1;
        if round == 15 {
            // Let the queue drain of the injected transient failures
            // instead of fighting chaos forever.
            harness.reset_chaos().await;
        }

        let handles: Vec<_> = (0..WORKER_COUNT)
            .map(|w| {
                tokio::spawn(run_one_round(
                    harness.pool.clone(),
                    harness.client.clone(),
                    harness.charge_url.clone(),
                    format!("chaos-worker-{w}"),
                    failpoints.clone(),
                ))
            })
            .collect();
        let mut claimed_this_round = 0;
        for handle in handles {
            claimed_this_round += handle.await.expect("worker round task should not panic");
        }

        if claimed_this_round == 0 {
            if is_quiescent(&harness.pool).await {
                reached_quiescence = true;
                break;
            }
            // Nothing claimable right now but not yet quiescent: leases
            // are still ticking down. Wait a fraction of the lease
            // duration rather than busy-looping.
            tokio::time::sleep(LEASE_DURATION / 4).await;
        }
    }

    eprintln!(
        "seeded_chaos_suite: stopped after {round} rounds, reached_quiescence={reached_quiescence}"
    );
    assert!(
        reached_quiescence,
        "SEED={SEED}: did not reach quiescence within {MAX_ROUNDS} rounds / {QUIESCENCE_DEADLINE:?} — \
         some job is stuck non-terminal; rerun with this seed to reproduce"
    );

    // --- Invariant checks, straight against PostgreSQL ---

    let non_terminal: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status IN ('PENDING', 'RUNNING')")
            .fetch_one(&harness.pool)
            .await
            .expect("count non-terminal");
    assert_eq!(
        non_terminal, 0,
        "SEED={SEED}: every job must be terminal (SUCCEEDED or DEAD) at quiescence"
    );

    let total_jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&harness.pool)
        .await
        .expect("count total");
    assert_eq!(
        total_jobs, JOB_COUNT as i64,
        "no job was lost or duplicated"
    );

    let over_budget: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE attempts > max_attempts")
            .fetch_one(&harness.pool)
            .await
            .expect("count over-budget jobs");
    assert_eq!(
        over_budget, 0,
        "SEED={SEED}: no job may exceed its configured retry budget without manual replay"
    );

    let duplicate_charge_keys: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT idempotency_key FROM charges GROUP BY idempotency_key HAVING count(*) > 1) dup",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("count duplicate idempotency keys");
    assert_eq!(
        duplicate_charge_keys, 0,
        "SEED={SEED}: no idempotency key may back more than one charge row — a violation here \
         means fencing + idempotency failed to protect a lease-expiry-while-paused overlap"
    );

    let dead_count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status = 'DEAD'")
        .fetch_one(&harness.pool)
        .await
        .expect("count dead");
    let succeeded_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED'")
            .fetch_one(&harness.pool)
            .await
            .expect("count succeeded");
    eprintln!(
        "seeded_chaos_suite: SEED={SEED} rounds={round} succeeded={succeeded_count} dead={dead_count} \
         total={total_jobs}"
    );
    assert_eq!(succeeded_count + dead_count, total_jobs as i64);
}
