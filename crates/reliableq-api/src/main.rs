use reliableq_api::{AppState, build_app};
use reliableq_core::config::{DatabaseConfig, HttpConfig, LogFormat};
use reliableq_db::{create_pool, run_migrations};
use tracing_subscriber::EnvFilter;

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
    let http_config = HttpConfig::from_env("API", "0.0.0.0:8080").unwrap_or_else(|err| {
        tracing::error!(error = %err, "invalid http configuration");
        std::process::exit(1);
    });

    let db = create_pool(&db_config).await?;
    run_migrations(&db).await?;

    let app = build_app(
        AppState { db },
        http_config.max_body_bytes,
        http_config.request_timeout,
    );

    tracing::info!(addr = %http_config.bind_addr, "starting reliableq-api");
    let listener = tokio::net::TcpListener::bind(http_config.bind_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

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
