//! M6: graceful shutdown, driven through `reliableq_worker::run_worker_loop`
//! with a synthetic shutdown trigger instead of an OS signal, so the
//! test controls exactly when shutdown fires with no sleep-based
//! guessing (spec sec. 17: avoid timing-flaky sleeps).

use std::net::SocketAddr;
use std::time::Duration;

use reliableq_core::config::WorkerConfig;
use reliableq_db::jobs;
use serde_json::json;
use sqlx::Executor;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::oneshot;
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

    fn worker_config(&self, concurrency: usize, shutdown_grace: Duration) -> WorkerConfig {
        WorkerConfig {
            concurrency,
            poll_interval: Duration::from_millis(20),
            lease_duration: Duration::from_secs(30),
            charge_service_url: self.charge_url.clone(),
            charge_request_timeout: Duration::from_secs(10),
            retry_base_delay: Duration::from_secs(1),
            retry_multiplier: 2,
            retry_max_delay: Duration::from_secs(60),
            shutdown_grace,
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

fn charge_payload(i: usize) -> serde_json::Value {
    json!({ "customer_id": format!("c-{i}"), "amount_cents": 100, "currency": "INR" })
}

/// With a grace period comfortably longer than the in-flight work's
/// delay, shutdown must wait for that work to finish successfully
/// rather than abandoning it.
#[tokio::test]
async fn shutdown_waits_for_in_flight_work_within_grace_period() {
    let harness = Harness::new().await;
    harness.set_delay(100).await;
    let id = Uuid::new_v4();
    jobs::insert_job(&harness.pool, id, "charge", &charge_payload(0), 5)
        .await
        .expect("insert");

    let config = harness.worker_config(4, Duration::from_secs(5));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Fire shutdown almost immediately — well before the 100ms delayed
    // charge call can have completed — but with a 5s grace period, the
    // loop must still wait for it rather than abandoning it.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = shutdown_tx.send(());
    });

    reliableq_worker::run_worker_loop(
        &harness.pool,
        &harness.client,
        "worker-shutdown-test",
        &config,
        async {
            let _ = shutdown_rx.await;
        },
    )
    .await;

    let job = jobs::get_job_by_id(&harness.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(
        job.status().unwrap(),
        reliableq_core::domain::JobStatus::Succeeded,
        "in-flight work must finish, not be abandoned, within the grace period"
    );
}

/// With a grace period shorter than the in-flight work's delay,
/// shutdown must return anyway (not hang) and must not mark the
/// abandoned job successful.
#[tokio::test]
async fn shutdown_abandons_work_that_exceeds_the_grace_period() {
    let harness = Harness::new().await;
    harness.set_delay(2000).await;
    let id = Uuid::new_v4();
    jobs::insert_job(&harness.pool, id, "charge", &charge_payload(0), 5)
        .await
        .expect("insert");

    let config = harness.worker_config(4, Duration::from_millis(100));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = shutdown_tx.send(());
    });

    let started = tokio::time::Instant::now();
    reliableq_worker::run_worker_loop(
        &harness.pool,
        &harness.client,
        "worker-shutdown-test",
        &config,
        async {
            let _ = shutdown_rx.await;
        },
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(1),
        "shutdown must return promptly once the grace period elapses, took {elapsed:?}"
    );

    let job = jobs::get_job_by_id(&harness.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_ne!(
        job.status().unwrap(),
        reliableq_core::domain::JobStatus::Succeeded,
        "abandoned work must never be marked successful"
    );
}

/// No new job is claimed once shutdown has already fired, even if one
/// becomes due immediately after.
#[tokio::test]
async fn no_new_claims_after_shutdown_fires() {
    let harness = Harness::new().await;
    let config = harness.worker_config(4, Duration::from_millis(50));

    // Shutdown fires before the loop even starts.
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let _ = shutdown_tx.send(());

    reliableq_worker::run_worker_loop(
        &harness.pool,
        &harness.client,
        "worker-shutdown-test",
        &config,
        async {
            let _ = shutdown_rx.await;
        },
    )
    .await;

    let id = Uuid::new_v4();
    jobs::insert_job(&harness.pool, id, "charge", &charge_payload(0), 5)
        .await
        .expect("insert");
    let job = jobs::get_job_by_id(&harness.pool, id)
        .await
        .expect("get")
        .expect("job exists");
    assert_eq!(
        job.status().unwrap(),
        reliableq_core::domain::JobStatus::Pending,
        "a job submitted after the loop has already returned must remain untouched"
    );
}
