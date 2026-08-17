//! `BenchConfig`: deserialized from `benchmarks/config/quick.toml` or
//! `benchmarks/config/full.toml`. One section per scenario (SPEC-BENCH
//! sec. 4); each carries its own `enabled` flag so a profile can exclude
//! a scenario entirely (e.g. the quick profile excludes the 100k-job
//! point of the backlog scenario by simply not listing it).

use serde::Deserialize;

fn default_repeat() -> u32 {
    3
}

#[derive(Debug, Clone, Deserialize)]
pub struct BenchConfig {
    pub environment: EnvironmentConfig,
    #[serde(default = "default_repeat")]
    pub repeat: u32,
    pub ingestion: Option<IngestionConfig>,
    pub execution: Option<ExecutionConfig>,
    pub scaling: Option<ScalingConfig>,
    pub claim_batch: Option<ClaimBatchConfig>,
    pub downstream_latency: Option<DownstreamLatencyConfig>,
    pub retry_degradation: Option<RetryDegradationConfig>,
    pub backlog: Option<BacklogConfig>,
    pub crash_recovery: Option<CrashRecoveryConfig>,
    pub idempotency: Option<IdempotencyConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnvironmentConfig {
    pub api_bind: String,
    pub worker_metrics_bind_base_port: u16,
    pub fake_charge_bind: String,
    pub database_url: String,
    pub binary_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestionConfig {
    pub enabled: bool,
    pub requests: u64,
    pub warmup_requests: u64,
    pub concurrencies: Vec<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    pub enabled: bool,
    pub job_count: u64,
    pub warmup_jobs: u64,
    pub concurrencies: Vec<u32>,
    /// Point B is `0`, point C is `100`; any additional values are
    /// reported as extra execution-sweep points.
    pub latencies_ms: Vec<u64>,
}

fn default_scaling_latency_ms() -> u64 {
    20
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScalingConfig {
    pub enabled: bool,
    pub job_count: u64,
    pub warmup_jobs: u64,
    /// `(worker_count, concurrency_per_worker)` pairs holding total
    /// concurrency constant.
    pub fixed_total: Vec<(u32, u32)>,
    /// `(worker_count, concurrency_per_worker)` pairs with increasing
    /// total concurrency.
    pub increasing: Vec<(u32, u32)>,
    /// A nonzero downstream latency so total concurrency actually gates
    /// throughput (at 0ms, DB round-trip cost dominates and scaling
    /// worker *count* changes little — not what this scenario tests).
    #[serde(default = "default_scaling_latency_ms")]
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClaimBatchConfig {
    pub enabled: bool,
    pub job_count: u64,
    pub batch_sizes: Vec<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownstreamLatencyConfig {
    pub enabled: bool,
    pub job_count: u64,
    pub worker_concurrency: u32,
    pub latencies_ms: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryDegradationConfig {
    pub enabled: bool,
    pub job_count: u64,
    pub worker_concurrency: u32,
    pub failure_rates: Vec<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BacklogConfig {
    pub enabled: bool,
    pub backlogs: Vec<u64>,
    pub worker_concurrency: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CrashRecoveryConfig {
    pub enabled: bool,
    pub jobs_per_failpoint: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdempotencyConfig {
    pub enabled: bool,
    pub concurrency_levels: Vec<u32>,
}

impl BenchConfig {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        let config: BenchConfig =
            toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
        Ok(config)
    }
}
