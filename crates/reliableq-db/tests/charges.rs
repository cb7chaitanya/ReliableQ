//! Charges repository tests against a live, isolated PostgreSQL schema.

use reliableq_db::charges;
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

#[tokio::test]
async fn insert_and_find_round_trip() {
    let db = TestDb::new().await;
    let id = Uuid::new_v4();
    charges::insert_or_get_charge(&db.pool, id, "key-1", "c1", 500, "INR")
        .await
        .expect("insert");

    let found = charges::find_by_idempotency_key(&db.pool, "key-1")
        .await
        .expect("find")
        .expect("charge exists");
    assert_eq!(found.id, id);
    assert_eq!(found.customer_id, "c1");
    assert_eq!(found.amount_cents, 500);
    assert_eq!(found.currency, "INR");
}

#[tokio::test]
async fn find_missing_key_returns_none() {
    let db = TestDb::new().await;
    let found = charges::find_by_idempotency_key(&db.pool, "does-not-exist")
        .await
        .expect("query should succeed");
    assert!(found.is_none());
}

#[tokio::test]
async fn first_insert_for_a_key_is_created() {
    let db = TestDb::new().await;
    let outcome =
        charges::insert_or_get_charge(&db.pool, Uuid::new_v4(), "key-a", "c1", 500, "INR")
            .await
            .expect("insert");
    assert!(matches!(outcome, charges::InsertChargeOutcome::Created(_)));
}

/// The M3 fix: reusing an idempotency key with the identical semantic
/// payload replays the original charge instead of erroring or
/// duplicating (see docs/failure-lab.md M3).
#[tokio::test]
async fn reusing_a_key_with_the_same_payload_replays() {
    let db = TestDb::new().await;
    let first =
        charges::insert_or_get_charge(&db.pool, Uuid::new_v4(), "dup-key", "c1", 500, "INR")
            .await
            .expect("first insert");
    let first_row = match first {
        charges::InsertChargeOutcome::Created(row) => row,
        other => panic!("expected Created, got {other:?}"),
    };

    let second =
        charges::insert_or_get_charge(&db.pool, Uuid::new_v4(), "dup-key", "c1", 500, "INR")
            .await
            .expect("second insert");
    let replayed_row = match second {
        charges::InsertChargeOutcome::Replayed(row) => row,
        other => panic!("expected Replayed, got {other:?}"),
    };
    assert_eq!(
        replayed_row.id, first_row.id,
        "a replay must return the original charge's id, not mint a new one"
    );

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges WHERE idempotency_key = $1")
        .bind("dup-key")
        .fetch_one(&db.pool)
        .await
        .expect("count");
    assert_eq!(
        count, 1,
        "exactly one charge row must exist per idempotency key"
    );
}

/// Reusing a key with a *different* payload is a genuine conflict, not
/// a replay — silently returning the original charge would let a
/// caller believe a different amount/customer was charged.
#[tokio::test]
async fn reusing_a_key_with_a_different_payload_is_a_conflict() {
    let db = TestDb::new().await;
    charges::insert_or_get_charge(&db.pool, Uuid::new_v4(), "conflict-key", "c1", 500, "INR")
        .await
        .expect("first insert");

    let second =
        charges::insert_or_get_charge(&db.pool, Uuid::new_v4(), "conflict-key", "c1", 999, "INR")
            .await
            .expect("second insert call");
    assert!(matches!(second, charges::InsertChargeOutcome::Conflict(_)));

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges WHERE idempotency_key = $1")
        .bind("conflict-key")
        .fetch_one(&db.pool)
        .await
        .expect("count");
    assert_eq!(
        count, 1,
        "a conflicting request must not create a second row"
    );
}

/// Concurrent duplicate requests must produce one charge row (spec sec.
/// 8.5), not a race where both see "no existing row" and both insert.
#[tokio::test]
async fn concurrent_inserts_with_the_same_key_produce_one_row() {
    let db = TestDb::new().await;
    let (a, b) = tokio::join!(
        charges::insert_or_get_charge(&db.pool, Uuid::new_v4(), "race-key", "c1", 500, "INR"),
        charges::insert_or_get_charge(&db.pool, Uuid::new_v4(), "race-key", "c1", 500, "INR"),
    );
    let a = a.expect("insert a");
    let b = b.expect("insert b");

    let created_count = [&a, &b]
        .iter()
        .filter(|o| matches!(o, charges::InsertChargeOutcome::Created(_)))
        .count();
    assert_eq!(
        created_count, 1,
        "exactly one of the two racing requests should create the row"
    );

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges WHERE idempotency_key = $1")
        .bind("race-key")
        .fetch_one(&db.pool)
        .await
        .expect("count");
    assert_eq!(count, 1);
}
