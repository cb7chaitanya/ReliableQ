//! `POST /v1/jobs`, `GET /v1/jobs/{id}`, `GET /v1/jobs` against a live,
//! isolated PostgreSQL schema.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use reliableq_api::{AppState, build_app};
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
            },
            64 * 1024,
            Duration::from_secs(10),
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

fn valid_submission() -> Value {
    json!({
        "kind": "charge",
        "payload": { "customer_id": "c1", "amount_cents": 5000, "currency": "INR" },
        "max_attempts": 5,
    })
}

async fn post_json(app: Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
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

async fn get_json(app: Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
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
async fn submit_returns_202_with_pending_job() {
    let db = TestDb::new().await;
    let (status, body) = post_json(db.app(), "/v1/jobs", valid_submission()).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["status"], "PENDING");
    assert_eq!(body["attempts"], 0);
    assert_eq!(body["max_attempts"], 5);
    assert!(body["id"].is_string());
    assert!(body["created_at"].is_string());
}

#[tokio::test]
async fn submit_is_visible_immediately_after_response() {
    let db = TestDb::new().await;
    let (_, submitted) = post_json(db.app(), "/v1/jobs", valid_submission()).await;
    let id = submitted["id"].as_str().unwrap();

    let (status, fetched) = get_json(db.app(), &format!("/v1/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["id"], id);
    assert_eq!(fetched["status"], "PENDING");
    assert!(fetched["attempts"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn submit_rejects_empty_kind() {
    let db = TestDb::new().await;
    let mut body = valid_submission();
    body["kind"] = json!("");
    let (status, response) = post_json(db.app(), "/v1/jobs", body).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "INVALID_ARGUMENT");
    assert!(response["error"]["request_id"].is_string());
}

#[tokio::test]
async fn submit_rejects_non_positive_amount() {
    let db = TestDb::new().await;
    let mut body = valid_submission();
    body["payload"]["amount_cents"] = json!(0);
    let (status, response) = post_json(db.app(), "/v1/jobs", body).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "INVALID_ARGUMENT");
}

#[tokio::test]
async fn submit_rejects_invalid_currency() {
    let db = TestDb::new().await;
    let mut body = valid_submission();
    body["payload"]["currency"] = json!("inr");
    let (status, _response) = post_json(db.app(), "/v1/jobs", body).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn submit_rejects_max_attempts_out_of_range() {
    let db = TestDb::new().await;
    let mut body = valid_submission();
    body["max_attempts"] = json!(0);
    let (status, _response) = post_json(db.app(), "/v1/jobs", body).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_missing_job_returns_404() {
    let db = TestDb::new().await;
    let (status, body) = get_json(db.app(), &format!("/v1/jobs/{}", Uuid::new_v4())).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
}

#[tokio::test]
async fn list_filters_by_status_and_paginates() {
    let db = TestDb::new().await;
    for _ in 0..3 {
        post_json(db.app(), "/v1/jobs", valid_submission()).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let (status, page1) = get_json(db.app(), "/v1/jobs?status=PENDING&limit=2").await;
    assert_eq!(status, StatusCode::OK);
    let items = page1["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let cursor = page1["next_cursor"].as_str().expect("more pages remain");

    let (status, page2) = get_json(
        db.app(),
        &format!("/v1/jobs?status=PENDING&limit=2&cursor={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items2 = page2["items"].as_array().unwrap();
    assert_eq!(items2.len(), 1);
    assert!(page2["next_cursor"].is_null());
}

#[tokio::test]
async fn list_rejects_invalid_status() {
    let db = TestDb::new().await;
    let (status, response) = get_json(db.app(), "/v1/jobs?status=NOT_A_STATUS").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "INVALID_ARGUMENT");
}

#[tokio::test]
async fn list_rejects_limit_over_max() {
    let db = TestDb::new().await;
    let (status, _response) = get_json(db.app(), "/v1/jobs?limit=500").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}
