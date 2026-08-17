//! Typed configuration shared by the API, worker, and fake-charge binaries.
//!
//! Every value has a documented default and is validated eagerly so a
//! misconfigured process fails fast at startup rather than misbehaving
//! at runtime (see spec sec. 19: "startup may fail fast with a contextual
//! error for invalid configuration").

use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    Missing(&'static str),
    #[error("invalid value for {name}: {reason}")]
    Invalid { name: String, reason: String },
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> Result<T, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw.trim().parse::<T>().map_err(|_| ConfigError::Invalid {
            name: key.to_string(),
            reason: format!("could not parse {raw:?}"),
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::Invalid {
            name: key.to_string(),
            reason: "value is not valid UTF-8".to_string(),
        }),
    }
}

/// PostgreSQL connection pool configuration, shared by every binary that
/// talks to the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub connect_timeout: Duration,
}

impl DatabaseConfig {
    pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;
    pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 5;
    const MAX_ALLOWED_CONNECTIONS: u32 = 100;
    const MAX_ALLOWED_CONNECT_TIMEOUT_SECS: u64 = 60;

    pub fn from_env() -> Result<Self, ConfigError> {
        let url = env::var("DATABASE_URL").map_err(|_| ConfigError::Missing("DATABASE_URL"))?;
        let max_connections = parse_env("DATABASE_MAX_CONNECTIONS", Self::DEFAULT_MAX_CONNECTIONS)?;
        let connect_timeout_secs = parse_env(
            "DATABASE_CONNECT_TIMEOUT_SECS",
            Self::DEFAULT_CONNECT_TIMEOUT_SECS,
        )?;
        let config = Self {
            url,
            max_connections,
            connect_timeout: Duration::from_secs(connect_timeout_secs),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.url.trim().is_empty() {
            return Err(invalid("DATABASE_URL", "must not be empty"));
        }
        if !(self.url.starts_with("postgres://") || self.url.starts_with("postgresql://")) {
            return Err(invalid("DATABASE_URL", "must be a postgres:// URL"));
        }
        if self.max_connections == 0 || self.max_connections > Self::MAX_ALLOWED_CONNECTIONS {
            return Err(invalid(
                "DATABASE_MAX_CONNECTIONS",
                format!("must be between 1 and {}", Self::MAX_ALLOWED_CONNECTIONS),
            ));
        }
        if self.connect_timeout.is_zero()
            || self.connect_timeout > Duration::from_secs(Self::MAX_ALLOWED_CONNECT_TIMEOUT_SECS)
        {
            return Err(invalid(
                "DATABASE_CONNECT_TIMEOUT_SECS",
                format!(
                    "must be between 1 and {} seconds",
                    Self::MAX_ALLOWED_CONNECT_TIMEOUT_SECS
                ),
            ));
        }
        Ok(())
    }
}

/// HTTP server configuration for a bound listener (API or fake-charge).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfig {
    pub bind_addr: SocketAddr,
    pub max_body_bytes: usize,
    pub request_timeout: Duration,
}

impl HttpConfig {
    pub const DEFAULT_MAX_BODY_BYTES: usize = 64 * 1024;
    pub const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 10;
    const MAX_ALLOWED_BODY_BYTES: usize = 10 * 1024 * 1024;
    const MAX_ALLOWED_REQUEST_TIMEOUT_SECS: u64 = 120;

    /// Reads `{prefix}_BIND_ADDR`, `{prefix}_MAX_BODY_BYTES`, and
    /// `{prefix}_REQUEST_TIMEOUT_SECS`, e.g. prefix `"API"` reads
    /// `API_BIND_ADDR`.
    pub fn from_env(prefix: &str, default_bind_addr: &str) -> Result<Self, ConfigError> {
        let bind_key = format!("{prefix}_BIND_ADDR");
        let bind_addr_raw = env::var(&bind_key).unwrap_or_else(|_| default_bind_addr.to_string());
        let bind_addr = bind_addr_raw
            .parse::<SocketAddr>()
            .map_err(|_| invalid(&bind_key, format!("{bind_addr_raw:?} is not host:port")))?;

        let max_body_bytes = parse_env(
            &format!("{prefix}_MAX_BODY_BYTES"),
            Self::DEFAULT_MAX_BODY_BYTES,
        )?;
        let request_timeout_secs = parse_env(
            &format!("{prefix}_REQUEST_TIMEOUT_SECS"),
            Self::DEFAULT_REQUEST_TIMEOUT_SECS,
        )?;

        let config = Self {
            bind_addr,
            max_body_bytes,
            request_timeout: Duration::from_secs(request_timeout_secs),
        };
        config.validate(prefix)?;
        Ok(config)
    }

    fn validate(&self, prefix: &str) -> Result<(), ConfigError> {
        if self.max_body_bytes == 0 || self.max_body_bytes > Self::MAX_ALLOWED_BODY_BYTES {
            return Err(invalid(
                &format!("{prefix}_MAX_BODY_BYTES"),
                format!("must be between 1 and {}", Self::MAX_ALLOWED_BODY_BYTES),
            ));
        }
        if self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(Self::MAX_ALLOWED_REQUEST_TIMEOUT_SECS)
        {
            return Err(invalid(
                &format!("{prefix}_REQUEST_TIMEOUT_SECS"),
                format!(
                    "must be between 1 and {} seconds",
                    Self::MAX_ALLOWED_REQUEST_TIMEOUT_SECS
                ),
            ));
        }
        Ok(())
    }
}

/// Worker polling, claiming, leasing, and downstream-call configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    /// Bounds in-flight handlers (enforced starting M6; stored and
    /// validated from M1 so the config surface does not change later).
    pub concurrency: usize,
    pub poll_interval: Duration,
    pub lease_duration: Duration,
    pub charge_service_url: String,
    pub charge_request_timeout: Duration,
    /// Retry backoff configuration (spec sec. 10 defaults: 1s base,
    /// 2x multiplier, 60s cap). Per-job `max_attempts` still comes from
    /// the job row itself, not from here.
    pub retry_base_delay: Duration,
    pub retry_multiplier: u32,
    pub retry_max_delay: Duration,
    /// How long shutdown waits for an in-flight batch to finish before
    /// abandoning it (spec sec. 9.5). Abandoned work is never marked
    /// successful; its lease simply expires and becomes reclaimable.
    pub shutdown_grace: Duration,
}

impl WorkerConfig {
    pub const DEFAULT_CONCURRENCY: usize = 10;
    pub const DEFAULT_POLL_INTERVAL_MS: u64 = 250;
    pub const DEFAULT_LEASE_DURATION_SECS: u64 = 30;
    pub const DEFAULT_CHARGE_REQUEST_TIMEOUT_SECS: u64 = 10;
    pub const DEFAULT_RETRY_BASE_DELAY_MS: u64 = 1_000;
    pub const DEFAULT_RETRY_MULTIPLIER: u32 = 2;
    pub const DEFAULT_RETRY_MAX_DELAY_SECS: u64 = 60;
    pub const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 30;
    const MAX_ALLOWED_CONCURRENCY: usize = 1000;
    const MAX_ALLOWED_POLL_INTERVAL_MS: u64 = 60_000;
    const MAX_ALLOWED_LEASE_DURATION_SECS: u64 = 3600;
    const MAX_ALLOWED_CHARGE_REQUEST_TIMEOUT_SECS: u64 = 120;
    const MAX_ALLOWED_RETRY_MAX_DELAY_SECS: u64 = 3600;
    const MAX_ALLOWED_SHUTDOWN_GRACE_SECS: u64 = 3600;

    pub fn from_env() -> Result<Self, ConfigError> {
        let concurrency = parse_env("WORKER_CONCURRENCY", Self::DEFAULT_CONCURRENCY)?;
        let poll_interval_ms =
            parse_env("WORKER_POLL_INTERVAL_MS", Self::DEFAULT_POLL_INTERVAL_MS)?;
        let lease_duration_secs = parse_env(
            "WORKER_LEASE_DURATION_SECS",
            Self::DEFAULT_LEASE_DURATION_SECS,
        )?;
        let charge_service_url = env::var("WORKER_CHARGE_SERVICE_URL")
            .map_err(|_| ConfigError::Missing("WORKER_CHARGE_SERVICE_URL"))?;
        let charge_request_timeout_secs = parse_env(
            "WORKER_CHARGE_REQUEST_TIMEOUT_SECS",
            Self::DEFAULT_CHARGE_REQUEST_TIMEOUT_SECS,
        )?;
        let retry_base_delay_ms = parse_env(
            "WORKER_RETRY_BASE_DELAY_MS",
            Self::DEFAULT_RETRY_BASE_DELAY_MS,
        )?;
        let retry_multiplier =
            parse_env("WORKER_RETRY_MULTIPLIER", Self::DEFAULT_RETRY_MULTIPLIER)?;
        let retry_max_delay_secs = parse_env(
            "WORKER_RETRY_MAX_DELAY_SECS",
            Self::DEFAULT_RETRY_MAX_DELAY_SECS,
        )?;
        let shutdown_grace_secs = parse_env(
            "WORKER_SHUTDOWN_GRACE_SECS",
            Self::DEFAULT_SHUTDOWN_GRACE_SECS,
        )?;

        let config = Self {
            concurrency,
            poll_interval: Duration::from_millis(poll_interval_ms),
            lease_duration: Duration::from_secs(lease_duration_secs),
            charge_service_url,
            charge_request_timeout: Duration::from_secs(charge_request_timeout_secs),
            retry_base_delay: Duration::from_millis(retry_base_delay_ms),
            retry_multiplier,
            retry_max_delay: Duration::from_secs(retry_max_delay_secs),
            shutdown_grace: Duration::from_secs(shutdown_grace_secs),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn retry_policy(&self) -> crate::retry::RetryPolicy {
        crate::retry::RetryPolicy {
            base_delay: self.retry_base_delay,
            multiplier: self.retry_multiplier,
            max_delay: self.retry_max_delay,
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.concurrency == 0 || self.concurrency > Self::MAX_ALLOWED_CONCURRENCY {
            return Err(invalid(
                "WORKER_CONCURRENCY",
                format!("must be between 1 and {}", Self::MAX_ALLOWED_CONCURRENCY),
            ));
        }
        if self.poll_interval.is_zero()
            || self.poll_interval > Duration::from_millis(Self::MAX_ALLOWED_POLL_INTERVAL_MS)
        {
            return Err(invalid(
                "WORKER_POLL_INTERVAL_MS",
                format!(
                    "must be between 1 and {} ms",
                    Self::MAX_ALLOWED_POLL_INTERVAL_MS
                ),
            ));
        }
        if self.lease_duration.is_zero()
            || self.lease_duration > Duration::from_secs(Self::MAX_ALLOWED_LEASE_DURATION_SECS)
        {
            return Err(invalid(
                "WORKER_LEASE_DURATION_SECS",
                format!(
                    "must be between 1 and {} seconds",
                    Self::MAX_ALLOWED_LEASE_DURATION_SECS
                ),
            ));
        }
        if !(self.charge_service_url.starts_with("http://")
            || self.charge_service_url.starts_with("https://"))
        {
            return Err(invalid(
                "WORKER_CHARGE_SERVICE_URL",
                "must be an http:// or https:// URL",
            ));
        }
        if self.charge_request_timeout.is_zero()
            || self.charge_request_timeout
                > Duration::from_secs(Self::MAX_ALLOWED_CHARGE_REQUEST_TIMEOUT_SECS)
        {
            return Err(invalid(
                "WORKER_CHARGE_REQUEST_TIMEOUT_SECS",
                format!(
                    "must be between 1 and {} seconds",
                    Self::MAX_ALLOWED_CHARGE_REQUEST_TIMEOUT_SECS
                ),
            ));
        }
        if self.retry_base_delay.is_zero() {
            return Err(invalid(
                "WORKER_RETRY_BASE_DELAY_MS",
                "must be greater than 0",
            ));
        }
        if self.retry_multiplier < 1 {
            return Err(invalid("WORKER_RETRY_MULTIPLIER", "must be at least 1"));
        }
        if self.retry_max_delay.is_zero()
            || self.retry_max_delay > Duration::from_secs(Self::MAX_ALLOWED_RETRY_MAX_DELAY_SECS)
        {
            return Err(invalid(
                "WORKER_RETRY_MAX_DELAY_SECS",
                format!(
                    "must be between 1 and {} seconds",
                    Self::MAX_ALLOWED_RETRY_MAX_DELAY_SECS
                ),
            ));
        }
        if self.retry_base_delay > self.retry_max_delay {
            return Err(invalid(
                "WORKER_RETRY_BASE_DELAY_MS",
                "must not exceed WORKER_RETRY_MAX_DELAY_SECS",
            ));
        }
        if self.shutdown_grace > Duration::from_secs(Self::MAX_ALLOWED_SHUTDOWN_GRACE_SECS) {
            return Err(invalid(
                "WORKER_SHUTDOWN_GRACE_SECS",
                format!(
                    "must be at most {} seconds",
                    Self::MAX_ALLOWED_SHUTDOWN_GRACE_SECS
                ),
            ));
        }
        Ok(())
    }
}

/// Structured log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

impl LogFormat {
    pub fn from_env() -> Result<Self, ConfigError> {
        match env::var("LOG_FORMAT") {
            Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "json" => Ok(Self::Json),
                "pretty" => Ok(Self::Pretty),
                other => Err(invalid(
                    "LOG_FORMAT",
                    format!("{other:?} must be \"json\" or \"pretty\""),
                )),
            },
            Err(_) => Ok(Self::Json),
        }
    }
}

fn invalid(name: &str, reason: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        name: name.to_string(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn clear_database_env() {
        for key in [
            "DATABASE_URL",
            "DATABASE_MAX_CONNECTIONS",
            "DATABASE_CONNECT_TIMEOUT_SECS",
        ] {
            unsafe { env::remove_var(key) };
        }
    }

    #[test]
    #[serial]
    fn database_config_requires_url() {
        clear_database_env();
        assert_eq!(
            DatabaseConfig::from_env(),
            Err(ConfigError::Missing("DATABASE_URL"))
        );
    }

    #[test]
    #[serial]
    fn database_config_rejects_non_postgres_url() {
        clear_database_env();
        unsafe { env::set_var("DATABASE_URL", "mysql://localhost/db") };
        let err = DatabaseConfig::from_env().unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { name, .. } if name == "DATABASE_URL"));
        clear_database_env();
    }

    #[test]
    #[serial]
    fn database_config_applies_defaults() {
        clear_database_env();
        unsafe { env::set_var("DATABASE_URL", "postgres://localhost/reliableq") };
        let config = DatabaseConfig::from_env().unwrap();
        assert_eq!(
            config.max_connections,
            DatabaseConfig::DEFAULT_MAX_CONNECTIONS
        );
        assert_eq!(
            config.connect_timeout,
            Duration::from_secs(DatabaseConfig::DEFAULT_CONNECT_TIMEOUT_SECS)
        );
        clear_database_env();
    }

    #[test]
    #[serial]
    fn database_config_rejects_out_of_range_pool_size() {
        clear_database_env();
        unsafe { env::set_var("DATABASE_URL", "postgres://localhost/reliableq") };
        unsafe { env::set_var("DATABASE_MAX_CONNECTIONS", "0") };
        assert!(DatabaseConfig::from_env().is_err());
        unsafe { env::set_var("DATABASE_MAX_CONNECTIONS", "500") };
        assert!(DatabaseConfig::from_env().is_err());
        clear_database_env();
    }

    #[test]
    fn http_config_uses_default_bind_addr() {
        let config = HttpConfig::from_env("RQ_TEST_UNSET_PREFIX", "127.0.0.1:8080").unwrap();
        assert_eq!(config.bind_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.max_body_bytes, HttpConfig::DEFAULT_MAX_BODY_BYTES);
    }

    #[test]
    fn http_config_rejects_invalid_bind_addr() {
        unsafe { env::set_var("RQ_BADADDR_BIND_ADDR", "not-an-addr") };
        let err = HttpConfig::from_env("RQ_BADADDR", "127.0.0.1:8080").unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { .. }));
        unsafe { env::remove_var("RQ_BADADDR_BIND_ADDR") };
    }

    #[test]
    fn log_format_defaults_to_json() {
        unsafe { env::remove_var("LOG_FORMAT") };
        assert_eq!(LogFormat::from_env().unwrap(), LogFormat::Json);
    }

    fn clear_worker_env() {
        for key in [
            "WORKER_CONCURRENCY",
            "WORKER_POLL_INTERVAL_MS",
            "WORKER_LEASE_DURATION_SECS",
            "WORKER_CHARGE_SERVICE_URL",
            "WORKER_CHARGE_REQUEST_TIMEOUT_SECS",
        ] {
            unsafe { env::remove_var(key) };
        }
    }

    #[test]
    #[serial]
    fn worker_config_requires_charge_service_url() {
        clear_worker_env();
        assert_eq!(
            WorkerConfig::from_env(),
            Err(ConfigError::Missing("WORKER_CHARGE_SERVICE_URL"))
        );
    }

    #[test]
    #[serial]
    fn worker_config_applies_defaults() {
        clear_worker_env();
        unsafe { env::set_var("WORKER_CHARGE_SERVICE_URL", "http://localhost:8081") };
        let config = WorkerConfig::from_env().unwrap();
        assert_eq!(config.concurrency, WorkerConfig::DEFAULT_CONCURRENCY);
        assert_eq!(
            config.poll_interval,
            Duration::from_millis(WorkerConfig::DEFAULT_POLL_INTERVAL_MS)
        );
        assert_eq!(
            config.lease_duration,
            Duration::from_secs(WorkerConfig::DEFAULT_LEASE_DURATION_SECS)
        );
        clear_worker_env();
    }

    #[test]
    #[serial]
    fn worker_config_rejects_non_http_charge_service_url() {
        clear_worker_env();
        unsafe { env::set_var("WORKER_CHARGE_SERVICE_URL", "ftp://localhost:8081") };
        assert!(WorkerConfig::from_env().is_err());
        clear_worker_env();
    }

    #[test]
    #[serial]
    fn worker_config_rejects_zero_concurrency() {
        clear_worker_env();
        unsafe { env::set_var("WORKER_CHARGE_SERVICE_URL", "http://localhost:8081") };
        unsafe { env::set_var("WORKER_CONCURRENCY", "0") };
        assert!(WorkerConfig::from_env().is_err());
        clear_worker_env();
    }
}
