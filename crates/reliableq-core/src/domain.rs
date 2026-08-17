//! Core job/attempt vocabulary shared by the API, worker, and db crates.

use serde::{Deserialize, Serialize};

/// Mirrors the `jobs.status` check constraint in
/// `migrations/0001_init_schema.sql`. See DESIGN.md sec. 3 for the full
/// state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Pending,
    Running,
    Succeeded,
    Dead,
}

impl JobStatus {
    pub const fn as_db_str(self) -> &'static str {
        match self {
            JobStatus::Pending => "PENDING",
            JobStatus::Running => "RUNNING",
            JobStatus::Succeeded => "SUCCEEDED",
            JobStatus::Dead => "DEAD",
        }
    }

    pub fn parse_db_str(s: &str) -> Option<Self> {
        match s {
            "PENDING" => Some(JobStatus::Pending),
            "RUNNING" => Some(JobStatus::Running),
            "SUCCEEDED" => Some(JobStatus::Succeeded),
            "DEAD" => Some(JobStatus::Dead),
            _ => None,
        }
    }

    /// `SUCCEEDED` and `DEAD` are not automatically claimable (invariant 6).
    pub const fn is_terminal(self) -> bool {
        matches!(self, JobStatus::Succeeded | JobStatus::Dead)
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_db_str())
    }
}

/// Mirrors the `job_attempts.outcome` check constraint. `RetryScheduled`
/// and `LeaseLost` are unused until M2/M4 but modeled now so the audit
/// trail's shape does not change later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AttemptOutcome {
    Succeeded,
    RetryScheduled,
    Dead,
    LeaseLost,
}

impl AttemptOutcome {
    pub const fn as_db_str(self) -> &'static str {
        match self {
            AttemptOutcome::Succeeded => "SUCCEEDED",
            AttemptOutcome::RetryScheduled => "RETRY_SCHEDULED",
            AttemptOutcome::Dead => "DEAD",
            AttemptOutcome::LeaseLost => "LEASE_LOST",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_status_round_trips_through_db_string() {
        for status in [
            JobStatus::Pending,
            JobStatus::Running,
            JobStatus::Succeeded,
            JobStatus::Dead,
        ] {
            assert_eq!(JobStatus::parse_db_str(status.as_db_str()), Some(status));
        }
    }

    #[test]
    fn job_status_rejects_unknown_string() {
        assert_eq!(JobStatus::parse_db_str("BOGUS"), None);
    }

    #[test]
    fn only_succeeded_and_dead_are_terminal() {
        assert!(!JobStatus::Pending.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(JobStatus::Succeeded.is_terminal());
        assert!(JobStatus::Dead.is_terminal());
    }
}
