//! Connection pool creation and migration runner shared by every binary
//! that talks to PostgreSQL. Query repository functions land here in
//! later milestones as the job/attempt/charge tables gain behavior.

use reliableq_core::config::DatabaseConfig;
use sqlx::postgres::{PgPool, PgPoolOptions};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Creates a bounded connection pool from validated configuration.
pub async fn create_pool(config: &DatabaseConfig) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.connect_timeout)
        .connect(&config.url)
        .await
}

/// Applies all pending migrations. Idempotent: already-applied migrations
/// are skipped via sqlx's tracking table.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}
