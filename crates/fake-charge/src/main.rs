use fake_charge::{AppState, build_app};
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
    let http_config = HttpConfig::from_env("FAKE_CHARGE", "0.0.0.0:8081").unwrap_or_else(|err| {
        tracing::error!(error = %err, "invalid http configuration");
        std::process::exit(1);
    });

    let db = create_pool(&db_config).await?;
    run_migrations(&db).await?;

    // Never true unless explicitly set (spec sec. 12: "safe from
    // accidental production activation"). Deployments must not set
    // this.
    let enable_test_control = std::env::var("FAKE_CHARGE_ENABLE_TEST_CONTROL")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    if enable_test_control {
        tracing::warn!("FAKE_CHARGE_ENABLE_TEST_CONTROL is set: chaos control endpoint is live");
    }

    let app = build_app(
        AppState {
            db,
            chaos: fake_charge::chaos::ChaosState::default(),
        },
        http_config.max_body_bytes,
        http_config.request_timeout,
        enable_test_control,
    );

    tracing::info!(addr = %http_config.bind_addr, "starting fake-charge");
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
