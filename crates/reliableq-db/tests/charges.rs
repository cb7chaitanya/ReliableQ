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
    charges::insert_charge(&db.pool, id, "key-1", "c1", 500, "INR")
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

/// M1's naive `insert_charge` has no dedup check: the database's unique
/// constraint on `idempotency_key` is the only thing standing between a
/// key reuse and a silent duplicate row, and it turns the reuse into an
/// error rather than a graceful replay. This is the exact gap M3 closes
/// (see docs/failure-lab.md).
#[tokio::test]
async fn reusing_an_idempotency_key_is_rejected_by_the_unique_constraint_not_replayed() {
    let db = TestDb::new().await;
    charges::insert_charge(&db.pool, Uuid::new_v4(), "dup-key", "c1", 500, "INR")
        .await
        .expect("first insert succeeds");

    let second =
        charges::insert_charge(&db.pool, Uuid::new_v4(), "dup-key", "c1", 500, "INR").await;
    assert!(
        second.is_err(),
        "naive insert must not silently succeed on a reused key"
    );

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges WHERE idempotency_key = $1")
        .bind("dup-key")
        .fetch_one(&db.pool)
        .await
        .expect("count");
    assert_eq!(
        count, 1,
        "the unique constraint must still cap it at one row"
    );
}
