# Interview notes

## 60-second summary

ReliableQ is a durable, database-backed job queue in Rust: clients
`POST` a job over HTTP, it commits to PostgreSQL before the API
acknowledges it, and a pool of worker processes polls, claims, and
executes it against a downstream (a bundled fake charge service). It
provides **at-least-once execution**, not exactly-once — a worker
crash or lease expiry can cause the same job to run twice — and makes
that survivable through three mechanisms working together: **expiring,
token-fenced leases** so abandoned work is recoverable and a resumed
stale worker can't corrupt a new owner's outcome; a **deterministic,
job-scoped idempotency key** so the one bundled side effect (a charge)
happens at most once even when the job runs twice; and **capped
exponential backoff with full jitter** plus a terminal `DEAD` state so
retries are bounded and permanently-failed work is inspectable and
explicitly replayable, never silently dropped or retried forever. The
whole project is built by reproducing each failure — a stranded job, a
duplicate charge, an unbounded retry storm, unbounded concurrency —
before adding the mechanism that fixes it, with the reproduction and
evidence recorded in `docs/failure-lab.md` at every step.

## 5-minute deep dive

**The state machine.** `PENDING -> RUNNING -> SUCCEEDED` (or `DEAD`),
with `RUNNING -> PENDING` on a retryable failure and `DEAD -> PENDING`
only via an explicit operator retry. An expired `RUNNING` row is
claimable directly by any worker — no separate reaper process. Every
transition is a single guarded UPDATE (`WHERE status = 'X' AND
lease_token = $N`), and the caller checks the affected row count to
know whether it actually won that transition.

**Claiming.** `FOR UPDATE SKIP LOCKED` inside one short transaction
that also writes the `job_attempts` audit row — committed before any
network call happens (invariant 13). This is the concurrency primitive
that lets multiple worker processes safely share one queue with zero
distributed coordination: two workers racing on the same row, one
wins, the other's `SELECT` just skips it instead of blocking.

**Leases and fencing (ADR 0003).** A lease is a `(lease_token,
lease_expires_at)` pair, generated fresh on claim/reclaim, compared
against *database* time (never worker wall-clock, so clock skew can't
matter). Expiry decides *when* a job becomes reclaimable; the token
decides *who* is allowed to finalize or renew it. That distinction
matters because expiry alone doesn't stop a merely-*paused* (GC stall,
suspended VM) worker from resuming and racing a legitimate new owner —
only the token guard, checked with every finalize/renew statement,
makes that race safe.

**Idempotency (ADR 0004).** The charge idempotency key is
`reliableq:charge:<job_id>` — constant across every attempt of the
same job, not per-attempt. `fake-charge`'s insert is a single `INSERT
... ON CONFLICT (idempotency_key) DO NOTHING RETURNING *` — atomic, no
separate check-then-insert race window — with the response
distinguishing a genuine new charge, a same-payload replay, and a
different-payload conflict (`409`). This closes exactly the gap
fencing leaves open: fencing stops a stale worker from corrupting job
state, not from having already sent (or being about to send) a
duplicate network call.

**Retries (ADR 0005).** `cap(n) = min(max_delay, base * mult^(n-1))`,
`delay = uniform(0, cap(n))` — full jitter, not fixed backoff, so a
worker fleet's retries don't stay synchronized. Only `Transient`
(408/429/5xx) and `Ambiguous` (no response at all) failures retry;
`Permanent` (other 4xx) and exhausted-budget failures go straight to
`DEAD`, with the terminal reason distinguishing the two.

**Bounded concurrency (ADR 0006).** A semaphore, permits acquired
*inside* each spawned task, with claiming itself capped at
`available_permits()` so unclaimed work never queues up behind a
ticking lease waiting on a permit. The one real bug this milestone
surfaced: the first graceful-shutdown implementation inferred
"nothing in flight" from `available_permits() == concurrency`, which
is wrong because a freshly spawned task isn't guaranteed to have been
polled — and therefore hasn't acquired its permit — yet. Fixed by
awaiting the actual in-flight `JoinHandle` future with a timeout
instead of inferring anything from a counter.

**Observability (ADR 0007).** Each process (API, worker) exposes its
own `/metrics`, since the worker is the only process that observes
execution-side signals (in-flight count, lease renewals). `worker_id`
is attached once via a tracing span at the top of the poll loop and
explicitly re-attached across `tokio::spawn` boundaries (spans don't
cross those automatically). Lease tokens are logged as a 12-character
SHA-256 fingerprint, never raw.

## Likely trade-off questions

**"Why PostgreSQL instead of a message broker?"** `FOR UPDATE SKIP
LOCKED` gives the exact claim semantics a queue needs, and colocating
queue state with job/attempt/charge state means one transaction claims
a job and writes its audit row atomically — no distributed-transaction
problem. A broker would add delivery semantics this project doesn't
need and a second store for job state regardless. Tradeoff: throughput
is bounded by what one Postgres instance's row locking sustains — a
deliberate non-goal (SPEC.md sec. 3 excludes horizontal sharding).

**"Why not exactly-once?"** Because it isn't achievable without a
distributed transaction spanning the database and every downstream
effect. The honest, buildable guarantee is at-least-once execution
plus an idempotent bundled effect — which delivers the property that
actually matters (the customer is charged once) without the
undeliverable one.

**"What happens if two workers claim the same job?"** They can't — the
claim query's `FOR UPDATE SKIP LOCKED` guarantees exactly one worker's
transaction locks and updates a given row; the other's `SELECT` simply
doesn't see it. What *can* happen is a stale worker (post-lease-expiry,
post-reclaim-by-someone-else) trying to *finalize* a job it no longer
owns — that's what the lease-token fencing guard rejects.

**"Why full jitter instead of fixed exponential backoff?"** Fixed
backoff still synchronizes a fleet: every worker computes the same
delay for the same attempt number and they all come back together.
Uniform-random-in-`[0, cap]` spreads that out. Tradeoff: less
predictable individual retry timing, which is the right trade for
avoiding thundering-herd retries against an already-struggling
dependency.

**"What's the actual blast radius if the worker process dies mid-job?"**
Bounded by the lease duration (default 30s) plus however long the next
poll cycle takes to notice. The job sits `RUNNING` until its lease
expires, then any worker reclaims it. If the charge had already
committed before the crash, the deterministic idempotency key makes
the retry a replay, not a duplicate. No manual intervention, no data
repair.

## Honest production extensions

Things a real production deployment would need that this project
deliberately scoped out:

- **`Retry-After` handling** for `429`/`503` (spec sec. 10) — currently
  every transient failure gets the computed backoff, not the server's
  suggested wait.
- **A real end-to-end trace ID** from API submission through to the
  charge call, not just per-process log correlation — would need a
  `request_id` column persisted on `jobs` at submission time.
- **Retention/archival** of completed jobs — `SUCCEEDED`/`DEAD` rows
  accumulate forever in v1 (SPEC.md sec. 3 explicit non-goal).
- **Authentication/authorization** on the API — it's unauthenticated
  by design (SPEC.md sec. 3), not meant for direct public exposure.
- **Fleet-wide concurrency limits** — `WORKER_CONCURRENCY` bounds one
  process; N workers can still produce N×C total concurrent downstream
  calls. A production deployment scaling workers would need either a
  downstream that can absorb that or a shared rate limiter.
- **A real downstream** instead of the bundled fake-charge — the
  idempotency contract this project proves is specific to the one
  handler it ships; a different downstream needs its own dedup story.
