//! Deterministic worker crash-point injection (spec sec. 12). The
//! trait-based design with a no-op production default means the
//! *production* code path (`execute_and_finalize`,
//! `spawn_bounded_batch`) never references failpoints at all — only
//! the chaos suite's `execute_and_finalize_with_failpoints` does — so
//! there is no runtime cost or accidental-activation risk in normal
//! operation.

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailpointName {
    /// Simulates a worker crash right after claiming, before the
    /// downstream call — the M2 stranded-`RUNNING` scenario.
    AfterClaimBeforeEffect,
    /// Simulates a worker crash right after the charge commits but
    /// before the job is finalized — the M3 duplicate-charge scenario.
    AfterEffectBeforeFinalize,
    /// Simulates a worker crash immediately after the finalize call
    /// resolves, before the worker can log or react to it. The DB
    /// transaction itself is atomic and already committed; this
    /// failpoint proves that fact does not depend on the worker
    /// surviving long enough to observe it.
    DuringFinalize,
}

/// Returns `true` if execution should stop at `name` for `job_id`,
/// simulating a crash at that exact point.
pub trait Failpoints: Send + Sync {
    fn should_trigger(&self, name: FailpointName, job_id: Uuid) -> bool;
}

/// The only implementation ever wired into `execute_and_finalize` /
/// `spawn_bounded_batch` / `run_worker_loop` — always returns `false`.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopFailpoints;

impl Failpoints for NoopFailpoints {
    fn should_trigger(&self, _name: FailpointName, _job_id: Uuid) -> bool {
        false
    }
}
