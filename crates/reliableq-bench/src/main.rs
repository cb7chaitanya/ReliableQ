mod chaos_client;
mod config;
mod correctness;
mod db;
mod env_info;
mod procs;
mod report;
mod resource;
mod result;
mod scenarios;
mod stats;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use config::BenchConfig;
use env_info::EnvInfo;
use reliableq_core::config::DatabaseConfig;
use scenarios::ScenarioCtx;
use sqlx::postgres::PgPoolOptions;

#[derive(Parser)]
#[command(name = "reliableq-bench")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runs one scenario (or `all`) from a config file against the real,
    /// release-mode binaries under `--binary-dir`.
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value = "all")]
        scenario: String,
        #[arg(long, default_value = "benchmarks/results")]
        out: PathBuf,
    },
    /// Regenerates docs/benchmarking/results.md and benchmarks/reports/*.svg
    /// from raw results already on disk.
    Report {
        #[arg(long, default_value = "benchmarks/results")]
        results: PathBuf,
        #[arg(long, default_value = "docs/benchmarking/results.md")]
        out: PathBuf,
        #[arg(long, default_value = "benchmarks/reports")]
        charts: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Run {
            config,
            scenario,
            out,
        } => run_command(config, scenario, out).await,
        Command::Report {
            results,
            out,
            charts,
        } => {
            report::generate(&results, &out, &charts)?;
            tracing::info!(out = %out.display(), "report generated");
            Ok(())
        }
    }
}

async fn run_command(config_path: PathBuf, scenario: String, out: PathBuf) -> anyhow::Result<()> {
    let config = BenchConfig::load(&config_path)?;
    std::fs::create_dir_all(&out)?;

    let db_config = DatabaseConfig {
        url: config.environment.database_url.clone(),
        max_connections: DatabaseConfig::DEFAULT_MAX_CONNECTIONS,
        connect_timeout: std::time::Duration::from_secs(5),
    };
    let pool = PgPoolOptions::new()
        .max_connections(db_config.max_connections)
        .acquire_timeout(db_config.connect_timeout)
        .connect(&db_config.url)
        .await?;
    reliableq_db::run_migrations(&pool).await?;

    let env = EnvInfo::capture(Some(&pool)).await;
    if env.git_dirty {
        tracing::warn!(
            "worktree is dirty: runs will be recorded with git_dirty=true and excluded from \
             the published report by `reliableq-bench report`"
        );
    }
    tracing::info!(commit = %env.git_commit, dirty = env.git_dirty, "environment captured");

    let ctx = ScenarioCtx {
        api_bind: config.environment.api_bind.clone(),
        fake_charge_bind: config.environment.fake_charge_bind.clone(),
        worker_metrics_base_port: config.environment.worker_metrics_bind_base_port,
        binary_dir: config.environment.binary_dir.clone(),
        database_url: config.environment.database_url.clone(),
        out_dir: out,
        config,
        env,
        pool,
    };

    let run_all = scenario == "all";
    macro_rules! maybe_run {
        ($name:literal, $module:path) => {
            if run_all || scenario == $name {
                tracing::info!(scenario = $name, "=== starting scenario ===");
                $module(&ctx).await?;
            }
        };
    }

    maybe_run!("ingestion", scenarios::ingestion::run);
    maybe_run!("execution", scenarios::execution::run);
    maybe_run!("scaling", scenarios::scaling::run);
    maybe_run!("claim_batch", scenarios::claim_batch::run);
    maybe_run!("retry_degradation", scenarios::retry_degradation::run);
    maybe_run!("backlog", scenarios::backlog::run);
    maybe_run!("crash_recovery", scenarios::crash_recovery::run);
    maybe_run!("idempotency", scenarios::idempotency::run);

    tracing::info!("all requested scenarios complete");
    Ok(())
}
