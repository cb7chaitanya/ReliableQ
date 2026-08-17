//! Standalone migration runner: `cargo run -p reliableq-db --bin migrate`.
//! Also used by `make migrate` for a documented, scriptable setup path.

use reliableq_core::config::DatabaseConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = DatabaseConfig::from_env()?;
    let pool = reliableq_db::create_pool(&config).await?;
    reliableq_db::run_migrations(&pool).await?;
    println!("migrations applied");
    Ok(())
}
