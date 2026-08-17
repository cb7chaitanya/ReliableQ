//! Periodic `ps`-based CPU/RSS sampling for bench-owned child processes,
//! and `docker stats` for the PostgreSQL container. Coarse (~1 sample/sec,
//! OS accounting granularity) — treated as directional, not
//! profiler-grade (docs/benchmarking/design.md sec. 8).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::process::Command;
use tokio::sync::Mutex;

use crate::result::ResourceSample;

#[derive(Default)]
struct Accumulator {
    cpu_samples: Vec<f64>,
    rss_peak: u64,
}

pub struct Sampler {
    stop: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
    acc: Arc<Mutex<Accumulator>>,
}

/// `ps -o %cpu=,rss= -p <pid>` — macOS and Linux both support this
/// invocation; `rss` is reported in KiB on both platforms.
async fn sample_pid(pid: u32) -> Option<(f64, u64)> {
    let out = Command::new("ps")
        .args(["-o", "%cpu=,rss=", "-p", &pid.to_string()])
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parts = text.split_whitespace();
    let cpu: f64 = parts.next()?.parse().ok()?;
    let rss_kb: u64 = parts.next()?.parse().ok()?;
    Some((cpu, rss_kb * 1024))
}

impl Sampler {
    /// Starts sampling `pid` every `interval` until [`Sampler::stop_and_collect`].
    pub fn start(pid: u32, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acc = Arc::new(Mutex::new(Accumulator::default()));
        let stop_clone = stop.clone();
        let acc_clone = acc.clone();
        let handle = tokio::spawn(async move {
            while !stop_clone.load(Ordering::Relaxed) {
                if let Some((cpu, rss)) = sample_pid(pid).await {
                    let mut acc = acc_clone.lock().await;
                    acc.cpu_samples.push(cpu);
                    acc.rss_peak = acc.rss_peak.max(rss);
                }
                tokio::time::sleep(interval).await;
            }
        });
        Self {
            stop,
            handle: Some(handle),
            acc,
        }
    }

    pub async fn stop_and_collect(mut self) -> ResourceSample {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
        let acc = self.acc.lock().await;
        if acc.cpu_samples.is_empty() {
            return ResourceSample {
                note: Some("no samples collected (process may have exited too quickly)".into()),
                ..Default::default()
            };
        }
        let avg = acc.cpu_samples.iter().sum::<f64>() / acc.cpu_samples.len() as f64;
        let peak = acc.cpu_samples.iter().cloned().fold(0.0_f64, f64::max);
        ResourceSample {
            cpu_pct_avg: Some(avg),
            cpu_pct_peak: Some(peak),
            rss_bytes_peak: Some(acc.rss_peak),
            note: None,
        }
    }
}

/// One-shot `docker stats` sample for the given container name. Returns
/// `None` (recorded as `unavailable` upstream) if docker isn't reachable
/// or the container name doesn't match.
pub async fn sample_postgres_container(container_name: &str) -> Option<ResourceSample> {
    let out = Command::new("docker")
        .args([
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}}\t{{.MemUsage}}",
            container_name,
        ])
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parts = text.trim().split('\t');
    let cpu_pct = parts.next()?.trim_end_matches('%').parse::<f64>().ok();
    Some(ResourceSample {
        cpu_pct_avg: cpu_pct,
        cpu_pct_peak: cpu_pct,
        rss_bytes_peak: None,
        note: Some("single docker-stats sample, not a time series".into()),
    })
}

pub async fn database_size_bytes(pool: &sqlx::PgPool, db_name: &str) -> Option<i64> {
    sqlx::query_scalar("SELECT pg_database_size($1)")
        .bind(db_name)
        .fetch_one(pool)
        .await
        .ok()
}

pub async fn wal_bytes_since(pool: &sqlx::PgPool, since_lsn: Option<String>) -> Option<i64> {
    let current: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(pool)
        .await
        .ok()?;
    let since = since_lsn?;
    let diff: Option<i64> =
        sqlx::query_scalar("SELECT (pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn))::bigint")
            .bind(&current)
            .bind(&since)
            .fetch_one(pool)
            .await
            .ok();
    diff
}

pub async fn current_wal_lsn(pool: &sqlx::PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(pool)
        .await
        .ok()
}
