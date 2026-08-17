//! The `RunResult` schema (docs/benchmarking/design.md sec. 6): one JSON
//! object per benchmark run. Every field is always present; anything the
//! environment cannot supply is `null`, never a fabricated number — see
//! [`Unavailable`].

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::env_info::EnvInfo;
use crate::stats::LatencyPercentiles;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Throughput {
    pub unit: String,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ErrorCounts {
    pub http_errors: u64,
    pub timeouts: u64,
    #[serde(default)]
    pub other: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceSample {
    pub cpu_pct_avg: Option<f64>,
    pub cpu_pct_peak: Option<f64>,
    pub rss_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceMeasurements {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<ResourceSample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<ResourceSample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fake_charge: Option<ResourceSample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres: Option<ResourceSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrectnessResults {
    pub passed: bool,
    pub checks: Value,
    #[serde(default)]
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Ok,
    Interrupted,
    Failed,
}

/// `docs/benchmarking/design.md` sec. 6. Nested config-ish fields
/// (`retry_configuration`, `database_pool_sizes`, `postgres_configuration`)
/// are kept as loose JSON rather than dedicated structs — they mirror
/// values read out of already-typed config (`reliableq_core::config`) and
/// `SHOW ALL`, and forcing a second parallel type for them would just be
/// duplication with no behavior of its own.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub schema_version: u32,
    pub timestamp_utc: chrono::DateTime<chrono::Utc>,
    pub scenario: String,
    pub scenario_params: Value,
    pub run_number: u32,
    pub status: RunStatus,

    // --- environment (docs/benchmarking/design.md sec. 6) ---
    pub git_commit: String,
    pub git_dirty: bool,
    pub rust_version: String,
    pub build_profile: String,
    pub os: String,
    pub os_version: String,
    pub architecture: String,
    pub cpu_model: Option<String>,
    pub logical_cpu_count: usize,
    pub memory_bytes: Option<u64>,
    pub docker_version: Option<String>,
    pub postgres_version: Option<String>,
    pub postgres_configuration: Value,

    // --- run configuration ---
    pub api_process_count: u32,
    pub worker_process_count: u32,
    pub worker_concurrency: u32,
    pub claim_batch_size: i64,
    pub database_pool_sizes: Value,
    pub lease_duration_secs: f64,
    pub heartbeat_interval_secs: f64,
    pub retry_configuration: Value,
    pub fake_charge_latency_ms: u64,
    pub fake_charge_failure_mode: String,
    pub job_count: u64,

    // --- measurement ---
    pub warmup_duration_secs: Option<f64>,
    pub measurement_duration_secs: f64,
    pub throughput: Throughput,
    pub latency_percentiles: LatencyPercentiles,
    pub error_counts: ErrorCounts,
    pub resource_measurements: ResourceMeasurements,
    pub correctness_results: CorrectnessResults,

    /// Scenario-specific extra data (e.g. retry amplification, dead-job
    /// counts, drain curves) that does not fit the common schema above.
    /// Always present (possibly `{}`), never used to smuggle in a field
    /// that belongs in one of the typed slots above.
    #[serde(default)]
    pub extra: Value,
}

impl RunResult {
    #[allow(clippy::too_many_arguments)]
    pub fn base(env: &EnvInfo, scenario: &str, scenario_params: Value, run_number: u32) -> Self {
        Self {
            schema_version: 1,
            timestamp_utc: chrono::Utc::now(),
            scenario: scenario.to_string(),
            scenario_params,
            run_number,
            status: RunStatus::Ok,
            git_commit: env.git_commit.clone(),
            git_dirty: env.git_dirty,
            rust_version: env.rust_version.clone(),
            build_profile: "release".to_string(),
            os: env.os.clone(),
            os_version: env.os_version.clone(),
            architecture: env.architecture.clone(),
            cpu_model: env.cpu_model.clone(),
            logical_cpu_count: env.logical_cpu_count,
            memory_bytes: env.memory_bytes,
            docker_version: env.docker_version.clone(),
            postgres_version: env.postgres_version.clone(),
            postgres_configuration: env.postgres_configuration.clone(),
            api_process_count: 0,
            worker_process_count: 0,
            worker_concurrency: 0,
            claim_batch_size: 0,
            database_pool_sizes: Value::Null,
            lease_duration_secs: 0.0,
            heartbeat_interval_secs: 0.0,
            retry_configuration: Value::Null,
            fake_charge_latency_ms: 0,
            fake_charge_failure_mode: "normal".to_string(),
            job_count: 0,
            warmup_duration_secs: None,
            measurement_duration_secs: 0.0,
            throughput: Throughput {
                unit: "jobs_per_sec".to_string(),
                value: 0.0,
            },
            latency_percentiles: LatencyPercentiles::empty("ms"),
            error_counts: ErrorCounts::default(),
            resource_measurements: ResourceMeasurements::default(),
            correctness_results: CorrectnessResults {
                passed: false,
                checks: Value::Null,
                failures: vec!["not yet run".to_string()],
            },
            extra: Value::Null,
        }
    }

    pub fn write(&self, out_dir: &Path) -> anyhow::Result<PathBuf> {
        let scenario_dir = out_dir.join(sanitize(&self.scenario));
        let params_dir = scenario_dir.join(sanitize(&params_fragment(&self.scenario_params)));
        fs::create_dir_all(&params_dir)?;
        let file_name = format!(
            "{}-run{}.json",
            self.timestamp_utc.format("%Y%m%dT%H%M%S%.3fZ"),
            self.run_number
        );
        let path = params_dir.join(file_name);
        let json = serde_json::to_string_pretty(self)?;
        fs::write(&path, json)?;
        Ok(path)
    }

    pub fn load_all(results_dir: &Path) -> anyhow::Result<Vec<RunResult>> {
        let mut out = Vec::new();
        if !results_dir.exists() {
            return Ok(out);
        }
        for entry in walk_json_files(results_dir)? {
            let raw = fs::read_to_string(&entry)?;
            match serde_json::from_str::<RunResult>(&raw) {
                Ok(run) => out.push(run),
                Err(err) => {
                    tracing::warn!(file = %entry.display(), error = %err, "skipping unparseable result file");
                }
            }
        }
        Ok(out)
    }
}

fn walk_json_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_json_files(&path)?);
        } else if path.extension().and_then(|e| e.to_str()) == Some("json") {
            out.push(path);
        }
    }
    Ok(out)
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn params_fragment(params: &Value) -> String {
    match params {
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| format!("{k}={}", v.to_string().trim_matches('"')))
            .collect::<Vec<_>>()
            .join(","),
        Value::Null => "default".to_string(),
        other => other.to_string(),
    }
}
