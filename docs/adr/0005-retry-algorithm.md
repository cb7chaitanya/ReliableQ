# ADR 0005: Failure classification and retry algorithm

- Status: accepted
- Date: 2026-08-17 (M4)

## Context

Through M3, every execution failure — a validation error, a permanent
downstream rejection, a transient `503`, a plain network timeout — sent
the job straight to `DEAD` after exactly one attempt. That is safe but
wasteful: transient blips (a downstream restart, a momentary network
issue) don't deserve to burn a job's entire outcome on one bad attempt.
But naive immediate retries create their own failure mode: hammering an
already-struggling dependency with zero backoff, potentially in
lockstep across every worker in the fleet.

## Decision

1. **Classify every downstream outcome** into `Transient` (408, 429,
   5xx — a response was received, try again), `Permanent` (other 4xx,
   e.g. 422 — a response was received, it will never succeed), or
   `Ambiguous` (no response at all — timeout, connection error; safe to
   retry only because the effect is idempotent, ADR 0004).
2. **Only `Transient`/`Ambiguous` failures are retryable**, and only if
   the job has budget left (`attempts < max_attempts`).
3. **Backoff is capped exponential with full jitter**:
   `cap(n) = min(max_delay, base_delay * multiplier^(n-1))`,
   `delay = uniform(0, cap(n))`, computed with saturating arithmetic so
   extreme configuration cannot overflow. Defaults: `base_delay = 1s`,
   `multiplier = 2`, `max_delay = 60s` (spec sec. 10).
4. `next_attempt_at` is computed in SQL as `now() + interval` — database
   time, not worker wall-clock time — matching the same reasoning as
   lease expiry (ADR 0003).
5. A retryable failure that exhausts the budget on this attempt goes to
   `DEAD` with reason `RETRY_BUDGET_EXHAUSTED`, distinct from a genuine
   permanent-failure `DEAD` — useful for M7's metrics, which must
   distinguish the two (spec sec. 11).

## Rationale

- **Full jitter, not fixed exponential backoff.** Fixed backoff still
  synchronizes retries across a worker fleet — every worker computes
  the same delay for the same attempt number and they all retry
  together. `uniform(0, cap)` spreads that out.
- **The demonstrated danger of zero delay** —
  `crates/reliableq-db/tests/jobs.rs::zero_delay_retry_scheduling_makes_a_job_immediately_reclaimable`
  proves directly that nothing in the repository layer stops a
  `delay = 0` retry from being claimable again immediately. This is why
  the worker's actual policy always has `base_delay >= 1s`: the
  repository primitive doesn't enforce a floor, so the policy has to.
- **Classification lives in reliableq-core, not the worker binary**,
  because it's pure logic over a status code — testable with plain unit
  tests, no HTTP server needed (see `crates/reliableq-core/src/failure.rs`
  and `retry.rs`, 15 unit tests between them, all deterministic given a
  seeded RNG).
- **Injected failure control (fake-charge's `chaos` module)** exists
  because testing retry/backoff/exhaustion behavior against the real
  worker code honestly requires a downstream that can be told "fail the
  next N calls with 503" or "reject everything permanently." It is
  disabled by default and only mounted when an operator explicitly sets
  `FAKE_CHARGE_ENABLE_TEST_CONTROL` — never in a default deployment
  (spec sec. 12).

## Consequences

- A job that fails transiently `max_attempts` times in a row now takes
  meaningfully longer to reach `DEAD` than before (up to
  `sum(cap(1..=max_attempts-1))`, worst case `~2 minutes` at defaults
  for `max_attempts = 5`) — a real latency tradeoff operators should
  understand when setting `max_attempts` and the backoff constants.
- `Retry-After` handling for `429`/`503` (spec sec. 10) is **not**
  implemented in M4 — the classifier treats them as ordinary transient
  failures subject to the computed backoff, not the server-suggested
  one. Flagged as a residual gap, not silently dropped.
- The chaos control endpoint is new attack surface *if* accidentally
  enabled in production — worth flagging in `docs/operations.md` (M7)
  explicitly, not just relying on the default being off.
