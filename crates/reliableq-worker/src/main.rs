use std::time::Duration;

use rand::Rng;
use reliableq_core::config::{DatabaseConfig, LogFormat, WorkerConfig};
use reliableq_db::{create_pool, jobs, run_migrations};
use reliableq_worker::execute_and_finalize;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Claimed per poll cycle. Not yet tied to `WorkerConfig::concurrency`
/// (that bound lands in M6); this just caps one round-trip's work.
const CLAIM_BATCH_SIZE: i64 = 10;

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
    tracing::info!(worker_id = %worker_id, "starting reliableq-worker");

    let mut shutdown = std::pin::pin!(shutdown_signal());
    loop {
        let claimed = match jobs::claim_pending_jobs(
            &db,
            &worker_id,
            CLAIM_BATCH_SIZE,
            worker_config.lease_duration,
        )
        .await
        {
            Ok(claimed) => claimed,
            Err(err) => {
                tracing::error!(error = %err, "failed to claim jobs, backing off");
                Vec::new()
            }
        };

        if claimed.is_empty() {
            // Full-jitter idle poll (spec sec. 9.5): avoids a
            // synchronized fleet of workers all polling in lockstep.
            // Only interruptible here, between cycles — M1 does not
            // interrupt in-flight execution for shutdown (that grace
            // period lands in M6).
            let jitter_ms =
                rand::thread_rng().gen_range(0..=worker_config.poll_interval.as_millis() as u64);
            let wait = worker_config.poll_interval + Duration::from_millis(jitter_ms);
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tokio::time::sleep(wait) => {}
            }
            continue;
        }

        for claimed_job in claimed {
            execute_and_finalize(&db, &client, &worker_config.charge_service_url, claimed_job)
                .await;
        }
    }

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
