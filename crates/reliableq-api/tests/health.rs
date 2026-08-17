//! `/health/live` must never require the database; `/health/ready` must.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use reliableq_api::{AppState, build_app};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn test_app(db: sqlx::PgPool) -> Router {
    build_app(AppState::new(db), 64 * 1024, Duration::from_secs(10))
}

#[tokio::test]
async fn live_does_not_require_database() {
    let db = PgPoolOptions::new()
        .connect_lazy("postgres://unreachable.invalid/db")
        .expect("lazy pool construction does not dial the database");
    let app = test_app(db);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health/live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn ready_reports_unavailable_when_database_unreachable() {
    let db = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(300))
        .connect_lazy("postgres://127.0.0.1:1/reliableq_unreachable")
        .expect("lazy pool construction does not dial the database");
    let app = test_app(db);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn ready_reports_ok_when_database_reachable() {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping ready_reports_ok_when_database_reachable: DATABASE_URL not set");
        return;
    };
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect to DATABASE_URL");
    let app = test_app(db);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
