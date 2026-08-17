//! `POST /v1/charges` against a live, isolated PostgreSQL schema.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use fake_charge::{AppState, build_app};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::Executor;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tower::ServiceExt;
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

    fn app(&self) -> Router {
        build_app(
            AppState {
                db: self.pool.clone(),
                chaos: fake_charge::chaos::ChaosState::default(),
            },
            64 * 1024,
            Duration::from_secs(10),
            true,
        )
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

fn valid_payload() -> Value {
    json!({ "customer_id": "c1", "amount_cents": 500, "currency": "INR" })
}

async fn post_charge(
    app: Router,
    idempotency_key: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/charges")
        .header("content-type", "application/json");
    if let Some(key) = idempotency_key {
        builder = builder.header("Idempotency-Key", key);
    }
    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response body should be valid json")
    };
    (status, json)
}

#[tokio::test]
async fn first_charge_returns_201() {
    let db = TestDb::new().await;
    let (status, body) = post_charge(db.app(), Some("key-1"), valid_payload()).await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["customer_id"], "c1");
    assert_eq!(body["amount_cents"], 500);
    assert_eq!(body["currency"], "INR");
    assert_eq!(body["idempotency_key"], "key-1");
    assert_eq!(body["replayed"], false);
    assert!(body["id"].is_string());
}

#[tokio::test]
async fn missing_idempotency_key_is_rejected() {
    let db = TestDb::new().await;
    let (status, body) = post_charge(db.app(), None, valid_payload()).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "INVALID_ARGUMENT");
}

#[tokio::test]
async fn invalid_payload_is_rejected() {
    let db = TestDb::new().await;
    let mut payload = valid_payload();
    payload["amount_cents"] = json!(-5);
    let (status, body) = post_charge(db.app(), Some("key-2"), payload).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "INVALID_ARGUMENT");
}

/// M3 fix: reusing an idempotency key with the identical payload
/// replays the original charge (see docs/failure-lab.md M3).
#[tokio::test]
async fn reused_idempotency_key_with_same_payload_replays() {
    let db = TestDb::new().await;
    let (first_status, first_body) = post_charge(db.app(), Some("dup"), valid_payload()).await;
    assert_eq!(first_status, StatusCode::CREATED);
    assert_eq!(first_body["replayed"], false);

    let (second_status, second_body) = post_charge(db.app(), Some("dup"), valid_payload()).await;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second_body["replayed"], true);
    assert_eq!(
        second_body["id"], first_body["id"],
        "a replay must return the original charge's id"
    );

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges WHERE idempotency_key = $1")
        .bind("dup")
        .fetch_one(&db.pool)
        .await
        .expect("count");
    assert_eq!(count, 1);
}

/// Reusing a key with a different payload must not silently succeed —
/// it must surface as a distinguishable conflict.
#[tokio::test]
async fn reused_idempotency_key_with_different_payload_is_a_conflict() {
    let db = TestDb::new().await;
    let (first_status, _) = post_charge(db.app(), Some("conflict"), valid_payload()).await;
    assert_eq!(first_status, StatusCode::CREATED);

    let mut different = valid_payload();
    different["amount_cents"] = json!(999);
    let (second_status, second_body) = post_charge(db.app(), Some("conflict"), different).await;
    assert_eq!(second_status, StatusCode::CONFLICT);
    assert_eq!(second_body["error"]["code"], "IDEMPOTENCY_CONFLICT");

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges WHERE idempotency_key = $1")
        .bind("conflict")
        .fetch_one(&db.pool)
        .await
        .expect("count");
    assert_eq!(
        count, 1,
        "a conflicting request must not create a second row"
    );
}

/// Concurrent duplicate HTTP requests must produce one charge row
/// (spec sec. 8.5).
#[tokio::test]
async fn concurrent_duplicate_requests_produce_one_charge_row() {
    let db = TestDb::new().await;
    let app = db.app();
    let (a, b) = tokio::join!(
        post_charge(app.clone(), Some("race"), valid_payload()),
        post_charge(app.clone(), Some("race"), valid_payload()),
    );
    let statuses = [a.0, b.0];
    assert!(
        statuses.contains(&StatusCode::CREATED) && statuses.contains(&StatusCode::OK),
        "expected one 201 and one 200 (replay), got {statuses:?}"
    );

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges WHERE idempotency_key = $1")
        .bind("race")
        .fetch_one(&db.pool)
        .await
        .expect("count");
    assert_eq!(count, 1);
}
