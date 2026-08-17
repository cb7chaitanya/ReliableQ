# ADR 0006: Bounded concurrency, lease heartbeat, and graceful shutdown

- Status: accepted
- Date: 2026-08-17 (M6)

## Context

Through M5 the worker executed one claimed job at a time, sequentially
— safe, but throwing away throughput and never actually exercising the
concurrency bound `WorkerConfig::concurrency` had been storing since
M1. Making execution concurrent without a bound reproduces the
delayed-dependency overload problem the spec warns about; making it
concurrent *with* a naive bound reintroduces the lease-expiry risk for
any job whose real work outlives one lease period.

## Decision

1. **Bound concurrency with a semaphore, acquired before spawning, not
   after.** `spawn_bounded_batch` acquires one permit per job inside
   each spawned task, but claiming itself is capped at
   `available_permits()` (spec sec. 9.5: "Claim no more than available
   permits and a configurable batch maximum") so claimed-but-unstarted
   work never piles up as ticking leases waiting for a permit.
2. **Every in-flight job renews its own lease** every
   `lease_duration / 3` via a background heartbeat task, aborted the
   moment the charge call resolves (spec sec. 9.3).
3. **Graceful shutdown reuses the actual in-flight batch future**,
   racing it against a `shutdown_grace` timeout, rather than inferring
   completion from semaphore permit counts.

## Rationale — the permit-count bug this ADR exists to document

The first implementation of shutdown draining polled
`semaphore.available_permits() < concurrency` in a loop to detect "is
anything still running." A test proved this wrong within one run:

```text
test shutdown_waits_for_in_flight_work_within_grace_period ... FAILED
  left: Running
 right: Succeeded
```

The job never finished — because it never *started*. `tokio::spawn`
schedules a task; it does not guarantee the task has been polled even
once by the time the spawning code continues. A permit is only
decremented once the spawned task's own `acquire_owned().await` line
actually runs. On the timing in that test, shutdown fired and the
drain loop checked `available_permits()` before the freshly spawned
task had been scheduled at all — so the check read "4 of 4 permits
free" and concluded, wrongly, that no work was in flight, while a job
sat claimed and abandoned in `RUNNING`.

The fix removes the inference entirely: instead of asking "does the
semaphore *look* idle," the shutdown path awaits the *same* `JoinHandle`
future the loop was already tracking, with a timeout. That can't be
fooled by scheduling order — it directly observes task completion.

## Consequences

- This is why `spawn_bounded_batch` returns real `JoinHandle`s that the
  caller is expected to hold onto and await, not a fire-and-forget
  spawn — the handles *are* the source of truth for "is this batch
  done," and nothing else should be trusted for that question.
- The claim/execute/shutdown loop was extracted from `main.rs` into
  `reliableq_worker::run_worker_loop`, parameterized over a generic
  shutdown future, specifically so this exact race could be caught by
  a test with a synthetic (instant, deterministic) shutdown trigger
  instead of requiring a real OS signal and a flaky sleep.
- Work abandoned past the grace period is never marked successful —
  the task is simply left to keep running to actual completion or get
  killed at process exit; its lease expires and (M2) becomes
  reclaimable by whichever worker polls next.
- fake-charge's `DelayMs` chaos mode and in-flight peak counter (spec
  sec. 12) exist specifically so "peak concurrent calls never exceeded
  N" is a measured assertion, not an inference.
