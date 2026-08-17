//! Spawns and tears down the *real* `reliableq-api`, `reliableq-worker`,
//! and `fake-charge` release binaries as child processes — every scenario
//! except the claim-batch driver and the crash-recovery failpoint driver
//! (docs/benchmarking/design.md sec. 2, sec. 5) runs the actual compiled
//! binaries, not an in-process reimplementation.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use tokio::process::{Child, Command};

pub struct ManagedProcess {
    pub label: String,
    pub child: Child,
}

impl ManagedProcess {
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Real `kill -9` — used for scenario I's genuine crash simulation
    /// and for ordinary teardown when a graceful stop is not needed.
    pub async fn kill_now(&mut self) {
        tracing::debug!(label = %self.label, pid = ?self.pid(), "kill -9");
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    /// SIGTERM, then a bounded wait, then a hard kill if it didn't exit.
    /// Used for routine end-of-scenario teardown so a worker's own
    /// graceful-shutdown path (spec sec. 9.5) runs rather than being
    /// skipped every single time.
    pub async fn graceful_stop(&mut self, grace: Duration) {
        tracing::debug!(label = %self.label, pid = ?self.pid(), "graceful stop (SIGTERM)");
        if let Some(pid) = self.pid() {
            let _ = Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .await;
        }
        if tokio::time::timeout(grace, self.child.wait())
            .await
            .is_err()
        {
            self.kill_now().await;
        }
    }
}

fn binary_path(binary_dir: &str, name: &str) -> String {
    Path::new(binary_dir)
        .join(name)
        .to_string_lossy()
        .into_owned()
}

async fn wait_for_http_ok(url: &str, timeout: Duration) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(resp) = client.get(url).send().await
            && resp.status().is_success()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for {url} to become ready");
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

pub async fn spawn_fake_charge(
    binary_dir: &str,
    database_url: &str,
    bind_addr: &str,
) -> anyhow::Result<ManagedProcess> {
    let mut envs: HashMap<&str, String> = HashMap::new();
    envs.insert("DATABASE_URL", database_url.to_string());
    envs.insert("FAKE_CHARGE_BIND_ADDR", bind_addr.to_string());
    envs.insert("FAKE_CHARGE_ENABLE_TEST_CONTROL", "true".to_string());
    envs.insert("LOG_FORMAT", "pretty".to_string());
    envs.insert("RUST_LOG", "warn".to_string());

    let child = Command::new(binary_path(binary_dir, "fake-charge"))
        .envs(&envs)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawning fake-charge: {e}"))?;

    wait_for_http_ok(
        &format!("http://{bind_addr}/v1/test/inflight"),
        Duration::from_secs(15),
    )
    .await?;

    Ok(ManagedProcess {
        label: "fake_charge".to_string(),
        child,
    })
}

pub async fn spawn_api(
    binary_dir: &str,
    database_url: &str,
    bind_addr: &str,
) -> anyhow::Result<ManagedProcess> {
    let mut envs: HashMap<&str, String> = HashMap::new();
    envs.insert("DATABASE_URL", database_url.to_string());
    envs.insert("API_BIND_ADDR", bind_addr.to_string());
    envs.insert("LOG_FORMAT", "pretty".to_string());
    envs.insert("RUST_LOG", "warn".to_string());

    let child = Command::new(binary_path(binary_dir, "reliableq-api"))
        .envs(&envs)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawning reliableq-api: {e}"))?;

    wait_for_http_ok(
        &format!("http://{bind_addr}/health/ready"),
        Duration::from_secs(15),
    )
    .await?;

    Ok(ManagedProcess {
        label: "api".to_string(),
        child,
    })
}

#[allow(clippy::too_many_arguments)]
pub struct WorkerSpec<'a> {
    pub binary_dir: &'a str,
    pub database_url: &'a str,
    pub charge_service_url: &'a str,
    pub concurrency: u32,
    pub lease_duration_secs: u64,
    pub poll_interval_ms: u64,
    pub metrics_bind_addr: String,
    /// `None` keeps `reliableq-worker`'s own default retry policy.
    pub retry_base_delay_ms: Option<u64>,
}

pub async fn spawn_worker(spec: WorkerSpec<'_>) -> anyhow::Result<ManagedProcess> {
    let mut envs: HashMap<&str, String> = HashMap::new();
    envs.insert("DATABASE_URL", spec.database_url.to_string());
    envs.insert(
        "WORKER_CHARGE_SERVICE_URL",
        spec.charge_service_url.to_string(),
    );
    envs.insert("WORKER_CONCURRENCY", spec.concurrency.to_string());
    envs.insert(
        "WORKER_LEASE_DURATION_SECS",
        spec.lease_duration_secs.to_string(),
    );
    envs.insert("WORKER_POLL_INTERVAL_MS", spec.poll_interval_ms.to_string());
    envs.insert("WORKER_METRICS_BIND_ADDR", spec.metrics_bind_addr.clone());
    envs.insert("LOG_FORMAT", "pretty".to_string());
    envs.insert("RUST_LOG", "warn".to_string());
    if let Some(base) = spec.retry_base_delay_ms {
        envs.insert("WORKER_RETRY_BASE_DELAY_MS", base.to_string());
    }

    let child = Command::new(binary_path(spec.binary_dir, "reliableq-worker"))
        .envs(&envs)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawning reliableq-worker: {e}"))?;

    wait_for_http_ok(
        &format!("http://{}/metrics", spec.metrics_bind_addr),
        Duration::from_secs(15),
    )
    .await?;

    Ok(ManagedProcess {
        label: "worker".to_string(),
        child,
    })
}
