pub mod backlog;
pub mod claim_batch;
pub mod crash_recovery;
pub mod execution;
pub mod idempotency;
pub mod ingestion;
pub mod retry_degradation;
pub mod scaling;

use std::path::PathBuf;
use std::time::Duration;

use sqlx::PgPool;

use crate::config::BenchConfig;
use crate::env_info::EnvInfo;

pub struct ScenarioCtx {
    pub config: BenchConfig,
    pub env: EnvInfo,
    pub pool: PgPool,
    pub out_dir: PathBuf,
    pub database_url: String,
    pub binary_dir: String,
    pub api_bind: String,
    pub fake_charge_bind: String,
    pub worker_metrics_base_port: u16,
}

impl ScenarioCtx {
    pub fn api_base(&self) -> String {
        format!("http://{}", self.api_bind)
    }

    pub fn fake_charge_base(&self) -> String {
        format!("http://{}", self.fake_charge_bind)
    }

    pub fn worker_metrics_addr(&self, index: u32) -> String {
        format!("127.0.0.1:{}", self.worker_metrics_base_port + index as u16)
    }

    pub fn write(&self, run: &crate::result::RunResult) -> anyhow::Result<PathBuf> {
        run.write(&self.out_dir)
    }
}

pub const TEARDOWN_GRACE: Duration = Duration::from_secs(5);

/// Truncates `jobs`, `job_attempts`, and `charges` so each scenario/repeat
/// starts from a known, empty database state (docs/benchmarking/design.md
/// sec. 5: "start from a known database state"). Only ever called against
/// the bench harness's own configured `DATABASE_URL`, never a database it
/// did not itself own for the duration of the run.
pub async fn reset_database(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query("TRUNCATE TABLE job_attempts, charges, jobs")
        .execute(pool)
        .await?;
    Ok(())
}
