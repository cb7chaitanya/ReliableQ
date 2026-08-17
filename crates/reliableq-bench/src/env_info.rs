//! Captures the environment facts every `RunResult` records (see
//! docs/benchmarking/design.md sec. 6 and sec. 8: "fixed inputs, recorded
//! every run"). A value this process cannot determine is `None`, never
//! guessed.

use std::process::Command;

use serde_json::Value;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct EnvInfo {
    pub git_commit: String,
    pub git_dirty: bool,
    pub rust_version: String,
    pub os: String,
    pub os_version: String,
    pub architecture: String,
    pub cpu_model: Option<String>,
    pub logical_cpu_count: usize,
    pub memory_bytes: Option<u64>,
    pub docker_version: Option<String>,
    pub postgres_version: Option<String>,
    pub postgres_configuration: Value,
}

fn run(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

impl EnvInfo {
    /// `pool` is optional: environment capture must succeed even before a
    /// database connection exists (e.g. to report a config error).
    pub async fn capture(pool: Option<&PgPool>) -> Self {
        let git_commit = run("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
        let git_dirty = run("git", &["status", "--porcelain"])
            .map(|s| !s.trim().is_empty())
            .unwrap_or(true);
        let rust_version = run("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string());
        let docker_version = run("docker", &["version", "--format", "{{.Server.Version}}"]);

        let os = std::env::consts::OS.to_string();
        let architecture = std::env::consts::ARCH.to_string();
        let os_version = run("uname", &["-r"]).unwrap_or_else(|| "unknown".into());

        let cpu_model = if cfg!(target_os = "macos") {
            run("sysctl", &["-n", "machdep.cpu.brand_string"])
        } else {
            std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("model name"))
                    .and_then(|l| l.split(':').nth(1))
                    .map(|s| s.trim().to_string())
            })
        };
        let logical_cpu_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let memory_bytes = if cfg!(target_os = "macos") {
            run("sysctl", &["-n", "hw.memsize"]).and_then(|s| s.parse().ok())
        } else {
            std::fs::read_to_string("/proc/meminfo").ok().and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("MemTotal:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|kb| kb.parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
        };

        let (postgres_version, postgres_configuration) = match pool {
            Some(pool) => {
                let version: Option<String> = sqlx::query_scalar("SELECT version()")
                    .fetch_one(pool)
                    .await
                    .ok();
                let mut config = serde_json::Map::new();
                for key in ["max_connections", "shared_buffers", "effective_cache_size"] {
                    let value: Result<String, _> = sqlx::query_scalar(&format!("SHOW {key}"))
                        .fetch_one(pool)
                        .await;
                    if let Ok(value) = value {
                        config.insert(key.to_string(), Value::String(value));
                    }
                }
                (version, Value::Object(config))
            }
            None => (None, Value::Null),
        };

        Self {
            git_commit,
            git_dirty,
            rust_version,
            os,
            os_version,
            architecture,
            cpu_model,
            logical_cpu_count,
            memory_bytes,
            docker_version,
            postgres_version,
            postgres_configuration,
        }
    }
}
