//! M0 exit criterion: a clean checkout can migrate an empty database.
//!
//! This test owns an isolated PostgreSQL schema (spec sec. 19: "tests own
//! isolated database schemas/databases and clean up safely") rather than
//! sharing the default schema with other tests, and drops it afterward
//! regardless of outcome.

use sqlx::Executor;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn migrations_apply_cleanly_to_empty_schema() {
    let database_url = std::env::var("DATABASE_URL").expect(
        "DATABASE_URL must point at a reachable postgres instance for reliableq-db tests \
         (see docker-compose.yml)",
    );

    let schema = format!("reliableq_test_{}", uuid::Uuid::new_v4().simple());

    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect to admin schema");
    admin_pool
        .execute(format!(r#"CREATE SCHEMA "{schema}""#).as_str())
        .await
        .expect("create isolated test schema");

    let result = run_migrations_in_schema(&database_url, &schema).await;

    admin_pool
        .execute(format!(r#"DROP SCHEMA "{schema}" CASCADE"#).as_str())
        .await
        .expect("drop isolated test schema");
    admin_pool.close().await;

    let tables = result.expect("migrations should apply cleanly to an empty schema");
    assert_eq!(
        tables,
        vec!["_sqlx_migrations", "charges", "job_attempts", "jobs"],
        "expected exactly the tables defined by migrations/, no more, no less"
    );
}

async fn run_migrations_in_schema(
    database_url: &str,
    schema: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let schema_for_hook = schema.to_string();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |conn, _meta| {
            let schema = schema_for_hook.clone();
            Box::pin(async move {
                conn.execute(format!(r#"SET search_path TO "{schema}""#).as_str())
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await?;

    reliableq_db::run_migrations(&pool)
        .await
        .map_err(|err| sqlx::Error::Migrate(Box::new(err)))?;

    let tables: Vec<String> = sqlx::query_scalar(
        "select table_name from information_schema.tables \
         where table_schema = $1 order by table_name",
    )
    .bind(schema)
    .fetch_all(&pool)
    .await?;

    pool.close().await;
    Ok(tables)
}
