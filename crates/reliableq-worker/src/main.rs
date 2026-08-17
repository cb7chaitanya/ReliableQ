use reliableq_core::config::{DatabaseConfig, LogFormat, WorkerConfig};
use reliableq_db::{create_pool, run_migrations};
use reliableq_worker::run_worker_loop;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_format = LogFormat::from_env().unwrap_or_else(|err| {
        eprintln!("invalid LOG_FORMAT configuration: {err}");
        std::process::exit(1);
    });
    init_tracing(log_format);

    let db_config = DatabaseConfig::from_env().unwrap_or_else(|err| {
        tracing::error!(error = %err, "invalid database configuration");
        std::process::exit(1);
    });
    let worker_config = WorkerConfig::from_env().unwrap_or_else(|err| {
        tracing::error!(error = %err, "invalid worker configuration");
        std::process::exit(1);
    });

    let db = create_pool(&db_config).await?;
    run_migrations(&db).await?;

    let client = reqwest::Client::builder()
        .timeout(worker_config.charge_request_timeout)
        .connect_timeout(worker_config.charge_request_timeout)
        .build()?;

    let worker_id = format!("worker-{}", Uuid::new_v4());
    tracing::info!(
        worker_id = %worker_id,
        concurrency = worker_config.concurrency,
        "starting reliableq-worker"
    );

    run_worker_loop(&db, &client, &worker_id, &worker_config, shutdown_signal()).await;

    tracing::info!("worker shutting down");
    Ok(())
}

fn init_tracing(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init(),
        LogFormat::Pretty => tracing_subscriber::fmt().with_env_filter(filter).init(),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl_c");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
