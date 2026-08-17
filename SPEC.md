# ReliableQ — End-to-End Project Specification

> Implementation brief for an autonomous coding agent. Build the system in the milestone order below. Preserve the learning progression: reproduce each failure before adding the mechanism that fixes it.

## 1. Project summary

ReliableQ is a durable, database-backed background-job system written in Rust. Clients submit jobs over HTTP; a bounded pool of workers claims and executes them asynchronously by calling a local fake charge service. The project begins as a deliberately naive queue and evolves through observed failures into a system with expiring leases, safe retries, idempotent side effects, exponential backoff with jitter, dead-letter handling, bounded concurrency, metrics, structured logs, and deterministic chaos tests.

The project is an interview-learning artifact, not merely a finished application. The repository must retain design notes, failure demonstrations, tests, and milestone commits that explain why every reliability mechanism exists.

## 2. Goals

- Accept jobs durably through an HTTP API.
- Execute jobs asynchronously using one or more worker processes.
- Make abandoned work recoverable with leases.
- Provide at-least-once execution while preventing duplicate effects in the included charge service through idempotency.
- Retry transient failures with capped exponential backoff and jitter.
- Move permanently failed or retry-exhausted jobs to a terminal dead state.
- Bound worker concurrency and support graceful shutdown.
- Expose enough state, logs, and metrics to diagnose lifecycle and failure behavior.
- Demonstrate the guarantees with automated failure injection and chaos tests.
- Remain small enough to explain fully in an SDE2 backend interview.

## 3. Non-goals

- Global exactly-once execution.
- Exactly-once delivery to arbitrary external systems.
- A general workflow/DAG engine, cron scheduler, priorities, job dependencies, or multi-tenant fairness.
- Kafka/RabbitMQ/Redis integration.
- Horizontal sharding or multi-region operation.
- A production authentication/authorization system or public internet deployment.
- Arbitrary user-provided code execution.
- A web UI. JSON inspection endpoints and metrics are sufficient.
- Automatic retention/archival of completed jobs in v1.

## 4. System contract and semantics

Primary durability contract:

> Once `POST /v1/jobs` returns `202 Accepted`, the job row has committed and will not silently disappear.

Execution semantics:

> ReliableQ provides at-least-once job execution. A worker crash can cause the same job to be attempted more than once. The bundled charge service makes repeated attempts safe by atomically deduplicating an idempotency key.

Terminal states are `SUCCEEDED` and `DEAD`. Every accepted job should eventually enter a terminal state, assuming the database and dependency recover, workers continue to run, and the job does not fail forever outside its retry budget.

Do not describe the system as exactly-once. The job handler may execute twice; only an idempotent downstream effect can make the externally visible outcome occur once.

## 5. Required architecture

Use a Rust Cargo workspace with these runtime components:

1. **API server** — validates submissions, persists jobs, exposes read/list/retry endpoints, health probes, and Prometheus metrics.
2. **Worker** — polls PostgreSQL, atomically claims due jobs, renews leases while work is active, invokes the charge service, and persists outcomes.
3. **Fake charge service** — records charges in PostgreSQL and deduplicates by idempotency key. It supports deterministic latency/failure injection in test/dev mode.
4. **PostgreSQL** — source of truth for job, attempt, charge, and idempotency state.

The API and worker must be independently runnable processes. They may share library crates. Multiple worker processes must safely operate against the same database.

Default technology choices:

- Stable Rust; edition 2024 if supported by the pinned toolchain, otherwise 2021.
- `tokio`, `axum`, `sqlx` with PostgreSQL and compile-time checked queries where practical.
- `serde`, `serde_json`, `uuid`, `chrono`, `thiserror`, `tracing`, `tracing-subscriber`.
- `reqwest` for the worker's charge call.
- `metrics` plus a Prometheus exporter, or an equivalently maintained crate.
- SQL migrations committed to the repository.
- Docker Compose for PostgreSQL and optional local process orchestration.

## 6. Repository structure

```text
reliableq/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── DESIGN.md
├── CHANGELOG.md
├── docker-compose.yml
├── .env.example
├── Makefile                         # or justfile; one task runner only
├── crates/
│   ├── reliableq-core/              # domain types, state transitions, retry math
│   ├── reliableq-db/                # migrations/query repository
│   ├── reliableq-api/               # API binary and handlers
│   ├── reliableq-worker/            # worker binary, leasing, execution
│   └── fake-charge/                 # idempotent downstream service binary
├── migrations/
├── tests/
│   ├── integration/
│   └── chaos/
├── scripts/                         # reproducible demos, not core logic
├── docs/
│   ├── adr/
│   ├── failure-lab.md
│   ├── operations.md
│   └── blog/
└── .github/workflows/ci.yml
```

Minor deviations are allowed if documented in an ADR and the responsibilities remain separated. Do not create abstractions or crates without a concrete need.

## 7. Domain model and state machine

### 7.1 States

```text
PENDING  --claim--------------------------> RUNNING
RUNNING  --successful effect--------------> SUCCEEDED
RUNNING  --retryable failure--------------> PENDING (scheduled in future)
RUNNING  --permanent/exhausted failure----> DEAD
RUNNING  --lease expires------------------> claimable RUNNING by a new owner
DEAD     --manual retry-------------------> PENDING (explicit new retry cycle)
```

An expired `RUNNING` row is claimable directly; a separate reaper is optional. Claiming it must replace the lease owner/token and increment the attempt atomically. Stale workers must be unable to finalize it.

### 7.2 `jobs` table

Required columns (exact SQL types may be adjusted and documented):

```sql
id                  uuid primary key
kind                text not null
payload             jsonb not null
status              text not null check (...)
attempts            integer not null default 0
max_attempts        integer not null
next_attempt_at     timestamptz not null
lease_token         uuid null
lease_expires_at    timestamptz null
last_error_code     text null
last_error_message  text null
created_at          timestamptz not null
updated_at          timestamptz not null
started_at          timestamptz null
finished_at         timestamptz null
version             bigint not null default 0
```

Constraints:

- `attempts >= 0`, `max_attempts >= 1`, and `attempts <= max_attempts`.
- Lease token and expiry are both null or both non-null.
- `PENDING` has no lease and no `finished_at`.
- `RUNNING` has a lease token and expiry.
- Terminal rows have `finished_at` and no active lease.
- Store full timestamps in UTC.

Indexes must support claiming due pending jobs, reclaiming expired jobs, listing by status/creation time, and uniqueness where required. At minimum, add partial indexes for `(next_attempt_at, created_at)` on `PENDING` and `lease_expires_at` on `RUNNING`.

### 7.3 `job_attempts` table

Maintain an append-only audit trail:

```sql
id, job_id, attempt_number, worker_id, lease_token,
started_at, finished_at, outcome, error_code, error_message,
scheduled_delay_ms, duration_ms
```

There must be a unique constraint on `(job_id, attempt_number)`. Outcomes include `SUCCEEDED`, `RETRY_SCHEDULED`, `DEAD`, and `LEASE_LOST`. Do not store unbounded or sensitive downstream response bodies.

### 7.4 `charges` table

```sql
id                  uuid primary key
idempotency_key     text not null unique
customer_id         text not null
amount_cents        bigint not null check (amount_cents > 0)
currency            text not null
created_at          timestamptz not null
```

The unique key is the enforcement mechanism. An in-memory cache is not sufficient.

## 8. API contracts

All endpoints use JSON except `/metrics`. Errors use a stable envelope:

```json
{
  "error": {
    "code": "INVALID_ARGUMENT",
    "message": "amount_cents must be positive",
    "request_id": "..."
  }
}
```

### 8.1 Submit a job

`POST /v1/jobs`

```json
{
  "kind": "charge",
  "payload": {
    "customer_id": "c123",
    "amount_cents": 5000,
    "currency": "INR"
  },
  "max_attempts": 5
}
```

Response: `202 Accepted`

```json
{
  "id": "uuid",
  "status": "PENDING",
  "attempts": 0,
  "max_attempts": 5,
  "created_at": "RFC3339 timestamp"
}
```

Validate kind, payload shape, positive amount, three-letter currency, bounded string lengths, and `max_attempts` within configurable limits. The success response is sent only after commit.

### 8.2 Inspect jobs

- `GET /v1/jobs/{id}` — full safe job representation plus attempt summaries; `404` if absent.
- `GET /v1/jobs?status=PENDING&limit=50&cursor=...` — stable cursor pagination, default 50, maximum 200.
- `GET /v1/dead-jobs` — convenience view equivalent to filtering `DEAD`.

Do not expose lease tokens. Error messages may be returned if sanitized.

### 8.3 Retry a dead job

`POST /v1/jobs/{id}/retry`

- Valid only from `DEAD`; use a guarded update.
- Reset status, scheduling, lease, last error, and terminal timestamps.
- Keep historical attempts and continue monotonically numbered attempts.
- Set a new configurable retry budget so `max_attempts` remains greater than the existing attempt count.
- Return `200`; return `409 INVALID_STATE` for a non-dead job.
- Reuse the same job ID and therefore the same downstream idempotency key. If the charge had already happened, downstream replay must return the original charge without duplicating it.

### 8.4 Operations endpoints

- `GET /health/live` — process event loop is alive; no dependency check.
- `GET /health/ready` — required configuration is valid and PostgreSQL is reachable.
- `GET /metrics` — Prometheus text format.

### 8.5 Fake charge service

`POST /v1/charges` with required `Idempotency-Key` header:

```json
{
  "customer_id": "c123",
  "amount_cents": 5000,
  "currency": "INR"
}
```

- First valid request atomically inserts and returns `201`.
- Same key and identical semantic payload returns the original charge with `200` and `replayed: true`.
- Same key with a different payload returns `409 IDEMPOTENCY_CONFLICT`.
- Invalid request returns `400`; configured transient failure returns `503`; configured permanent rejection returns `422`.
- Concurrent duplicate requests must produce one charge row.

## 9. Worker behavior

### 9.1 Claiming

Claim only jobs that are due:

- `PENDING AND next_attempt_at <= database_now`, or
- `RUNNING AND lease_expires_at <= database_now`.

Use a short transaction and PostgreSQL row locking such as `FOR UPDATE SKIP LOCKED`. Within the same transaction:

1. Select up to available local capacity.
2. Guard `attempts < max_attempts`; transition exhausted rows to `DEAD` if encountered.
3. Set `RUNNING`, increment attempts, set `started_at` if null, generate a unique lease token, set lease expiry from database time, update version/timestamps, and create the attempt record.
4. Commit before any network call.

Never hold a database transaction open while executing the side effect. Multiple workers must not claim the same active lease.

### 9.2 Execution

- Derive the idempotency key deterministically as `reliableq:charge:<job_uuid>`.
- Send bounded connect/request timeouts.
- Classify responses into success, retryable, permanent, and ambiguous outcomes.
- Treat timeouts, connection errors, `408`, `429`, and `5xx` as retryable/ambiguous.
- Treat validated business rejections (`4xx` such as `422`) as permanent, excluding retryable codes above.
- Parse and validate success responses before finalization.

### 9.3 Lease renewal and fencing

- Default lease duration: 30 seconds; configurable.
- Heartbeat every one-third of the lease duration, with a minimum sensible interval.
- Renew only with `WHERE id = ? AND status = 'RUNNING' AND lease_token = ?`.
- Final success/retry/dead updates use the same token guard. This token is the fencing mechanism.
- If renewal or finalization affects zero rows, the worker has lost ownership. Record/log `LEASE_LOST` where possible and do not mutate the job further.
- Database time, not worker wall-clock time, determines lease expiry and due status.
- Do not falsely claim leases prevent all concurrent side effects: a paused worker can resume after expiry. Idempotency makes that overlap safe for the included handler.

### 9.4 Finalization

- Success: `SUCCEEDED`, clear lease/error, set `finished_at`, complete attempt.
- Retryable failure with budget remaining: `PENDING`, calculate `next_attempt_at`, clear lease, save sanitized error, complete attempt.
- Permanent failure or exhausted budget: `DEAD`, clear lease, set terminal timestamp/error, complete attempt.
- Job and attempt updates must be in one transaction.

### 9.5 Polling and bounded concurrency

- A semaphore bounds in-flight handlers. Default 10; configurable and validated.
- Claim no more than available permits and a configurable batch maximum.
- Use cancellation-aware async tasks; do not create an unbounded task queue.
- When no work is found, poll with a small configurable interval and jitter to avoid synchronized worker fleets. PostgreSQL notification is optional and must retain polling as fallback.
- On shutdown: stop claiming, allow active work a configurable grace period, continue lease heartbeats during grace, then cancel. Do not mark unfinished work successful; allow leases to expire if finalization is impossible.

## 10. Retry policy, backoff, and jitter

Default values:

```text
max_attempts = 5
base_delay = 1 second
multiplier = 2
max_delay = 60 seconds
jitter = full jitter
```

For attempt number `n` after its failure:

```text
cap(n) = min(max_delay, base_delay * 2^(n - 1))
delay  = uniform(0, cap(n))
```

Requirements:

- Use checked/saturating arithmetic; no overflow for extreme configuration.
- Inject an RNG or deterministic seed for tests.
- Honor `Retry-After` for `429`/`503` when valid, bounded by a configured maximum; document how it combines with jitter.
- Persist the selected schedule and use database time when calculating `next_attempt_at`.
- Test boundary values and statistical bounds; tests must not depend on exact production randomness.

## 11. Dead jobs / DLQ semantics

`DEAD` is a state in the jobs table, not a second queue. A job becomes dead when a failure is permanent or the current attempt consumes its budget. Preserve payload, last sanitized error, timestamps, and complete attempt history.

Dead jobs are never claimed automatically. Operators can inspect and explicitly retry them. Metrics must distinguish permanent failures from retry exhaustion. Avoid logging secrets or full sensitive payloads.

## 12. Failure injection

Failure injection is required and must be disabled by default. Enable it only under `cfg(test)` or an explicit development flag unavailable in production mode.

Named worker crash points:

- `after_claim_before_effect`
- `after_effect_before_finalize` — the essential ambiguity demonstration
- `during_finalize`

Fake charge modes, configurable per request in tests or through a deterministic test control endpoint:

- succeed
- delay by N milliseconds
- fail next N calls with `503`
- permanently reject
- commit charge then drop/abort response (ambiguous outcome)

Injection must be deterministic, scoped, observable in logs, and safe from accidental production activation. Prefer a trait-based failpoint interface with a no-op production implementation.

## 13. Observability

### 13.1 Structured logs

Emit JSON-capable structured logs with `request_id`, `job_id`, `attempt`, `worker_id`, and `lease_token_hash` where applicable. Log state transitions, claims, renewal failures, retry schedules, downstream classification, shutdown, and injected failures. Never log database URLs, auth headers, raw lease tokens, or unredacted arbitrary payloads.

### 13.2 Metrics

At minimum:

- `reliableq_jobs_submitted_total{kind}`
- `reliableq_job_attempts_total{kind,outcome}`
- `reliableq_job_duration_seconds{kind,outcome}` histogram
- `reliableq_downstream_requests_total{result}`
- `reliableq_retries_scheduled_total{reason}`
- `reliableq_dead_jobs_total{reason}`
- `reliableq_lease_renewals_total{result}`
- `reliableq_lease_expirations_reclaimed_total`
- `reliableq_inflight_jobs` gauge
- `reliableq_job_queue_depth{status}` gauge
- `reliableq_oldest_pending_age_seconds` gauge

Keep metric labels low-cardinality: never use job IDs, customer IDs, messages, or URLs as labels.

### 13.3 Correlation and operations

Propagate request IDs to the charge service. Provide `docs/operations.md` with startup, shutdown, inspecting stuck/dead jobs, replaying a dead job, interpreting metrics, and recovery procedures.

## 14. Invariants

Encode these in database constraints where possible and test all of them:

1. A job ID identifies exactly one durable row.
2. A successful submission is committed before the API replies.
3. Only due jobs with remaining budget are claimable.
4. An active lease has exactly one token; stale tokens cannot finalize or renew.
5. A job attempt number is unique and increases monotonically.
6. `SUCCEEDED` and `DEAD` are not automatically claimable.
7. Every transition is legal, guarded, timestamped, and observable.
8. Retry scheduling never exceeds configured bounds.
9. Local in-flight handler count never exceeds configured concurrency.
10. The charge service creates at most one charge per idempotency key.
11. Reusing an idempotency key for different input never silently succeeds.
12. A crash after a committed charge but before job finalization can cause re-execution, but not a duplicate charge.
13. No network call occurs inside a job-claim database transaction.

## 15. Strict reasoning workflow — do not skip

For every milestone, the coding agent must use this loop and record the result in `docs/failure-lab.md` or an ADR:

1. **State the invariant or learning objective.** Explain the guarantee in plain language.
2. **Describe the naive design.** Say what it does under normal operation.
3. **Name the failure window.** Give the exact event sequence that violates or threatens the invariant.
4. **Add a deterministic reproduction.** Write a failing test or script before implementing the fix. If the milestone is initial scaffolding, add the smallest test proving the intended baseline.
5. **Record observed evidence.** Include relevant states/log excerpts or test assertions, without fabricated results.
6. **Propose the smallest mechanism.** Explain why it addresses this failure and what it still cannot guarantee.
7. **Implement the mechanism.** Keep the diff scoped to the milestone.
8. **Verify normal and failure paths.** Run focused tests, then the full quality gate.
9. **Update documentation.** State semantics, trade-offs, operational impact, and residual risks.
10. **Commit the milestone.** Use the prescribed commit shape. Do not combine later reliability features early.

If a test cannot be run, do not mark the milestone complete. Document the concrete blocker and leave the acceptance box unchecked. Never weaken/remove an assertion just to make a test pass. Never invent command output, test results, benchmarks, or chaos evidence.

The agent must not implement leases before demonstrating stranded `RUNNING` work, idempotency before demonstrating the post-effect/pre-finalize ambiguity, retry backoff before showing immediate retry behavior, or bounded concurrency before measuring unbounded in-flight work.

## 16. Milestones and required order

### M0 — Contract, skeleton, and local environment

- Write `DESIGN.md` with guarantees/non-guarantees and state machine.
- Create workspace, configuration parsing/validation, migrations, Compose, health endpoints, CI, and test harness.
- Add one migration smoke test.

Exit: clean checkout can start PostgreSQL, migrate, build, lint, and test through documented commands.

### M1 — Naive durable queue

- Submit/get/list API.
- Worker polling and atomic claim of pending jobs.
- Fake charge service without idempotency initially.
- Normal success path and attempt audit.
- Intentionally no lease recovery/retry sophistication.

Exit: a submitted charge job succeeds normally; document why crash recovery is incomplete.

### M2 — Demonstrate stranded work; add leases

- Crash after claim and prove the naive job remains `RUNNING`.
- Add lease fields, expired-lease reclaim, renewal, and token-fenced finalization.
- Add competing-worker and stale-worker tests.

Exit: a killed worker's job is reclaimed after expiry, and the stale owner cannot finalize it.

### M3 — Demonstrate duplicate effect; add idempotency

- Crash/drop response after charge commit but before success finalization.
- First prove a duplicate charge is possible in the naive service.
- Add database-backed idempotency and payload-conflict behavior.

Exit: the job is attempted again but exactly one charge row exists.

### M4 — Failure taxonomy and retries

- Classify transient, permanent, and ambiguous failures.
- Initially demonstrate tight retries.
- Add capped exponential backoff, full jitter, retry budget, and persisted scheduling.

Exit: transient failures retry on bounded schedules; permanent failures do not retry; deterministic tests cover math.

### M5 — Dead jobs and operator replay

- Add terminal dead transition, inspection API, reason metrics, and manual retry.
- Preserve attempt history and idempotency semantics.

Exit: exhausted/permanent jobs remain unclaimable until explicit replay.

### M6 — Bounded concurrency and graceful shutdown

- Demonstrate excess in-flight calls with a delayed charge service.
- Add semaphore capacity, capacity-aware claims, cancellation, and shutdown grace.

Exit: measured peak calls do not exceed the configured bound; no new claims begin after shutdown starts.

### M7 — Observability and operational polish

- Structured logs, metrics, request correlation, readiness/liveness, operations guide.
- Ensure no high-cardinality labels or secret leakage.

Exit: a failure can be traced from API submission through attempts to charge outcome using logs and inspection endpoints.

### M8 — Chaos suite, hardening, and narrative

- Run repeatable crash, latency, transient failure, concurrent worker, and shutdown scenarios.
- Check invariants directly against PostgreSQL after each scenario.
- Finish README, ADRs, diagrams, blog artifacts, and demo script.

Exit: all acceptance criteria pass from a clean environment and the project can be demonstrated end-to-end.

## 17. Test plan

### Unit tests

- Legal/illegal state transitions.
- Request and configuration validation.
- Error classification.
- Backoff cap, overflow protection, jitter bounds, deterministic seed.
- Idempotency-key derivation.
- Sanitization/redaction.

### Database/repository tests

- Migrations up from empty database.
- Atomic claim under concurrent transactions.
- `SKIP LOCKED` behavior and no duplicate active claim.
- Expired lease reclaim; non-expired lease exclusion.
- Stale token cannot renew/finalize.
- Attempt uniqueness and guarded terminal transitions.
- Dead jobs cannot be automatically claimed.
- Atomic concurrent idempotency insert and payload conflict.

### API tests

- Submit, validation errors, committed visibility, get, pagination, filters, not found.
- Retry valid dead job and conflict for other states.
- Health/readiness and stable error envelope.

### End-to-end tests

- Happy path from submission to one persisted charge and `SUCCEEDED`.
- Transient failures followed by success with expected attempt count.
- Permanent rejection to `DEAD` after one attempt.
- Exhausted retries to `DEAD`.
- Worker killed after claim; another worker recovers after lease expiry.
- Charge committed and response lost/worker killed; replay returns original charge.
- Two or more workers process a batch without lost or multiply finalized jobs.
- Graceful shutdown with active tasks.

### Chaos/property tests

- Seeded random injection across at least 100 jobs and multiple workers: crashes at named failpoints, response loss, delays longer than leases, and transient failures.
- After quiescence, assert every job is terminal, no attempt exceeds budget without manual replay, and charge count per idempotency key is at most one.
- Run at least one scenario where a lease expires while the old worker is paused, ensuring fencing plus idempotency protect the outcome.
- Avoid timing-flaky sleeps: expose short test configuration, poll eventual conditions with deadlines, and print seed/state on failure.

### Quality gate

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
integration and chaos test command documented in README
sql migration check from an empty database
```

## 18. Acceptance criteria

The project is complete only when all are true:

- [ ] A clean checkout has one documented setup path and `.env.example`.
- [ ] `POST /v1/jobs` acknowledges only committed jobs.
- [ ] API and workers run independently; at least two workers can share the queue safely.
- [ ] Claims are transactional and no network request occurs in the claim transaction.
- [ ] Worker death leaves recoverable work through expiring leases.
- [ ] Lease renewal and all finalization operations are token-fenced.
- [ ] A stale worker cannot overwrite the outcome produced by a new lease owner.
- [ ] The ambiguous post-charge/pre-finalize failure is reproducible.
- [ ] Re-execution after that failure creates one charge, proven by database assertion.
- [ ] Retry classifications, attempt limits, exponential backoff, cap, and jitter are tested.
- [ ] Permanent and exhausted jobs enter `DEAD` and remain unclaimed.
- [ ] Dead jobs can be inspected and explicitly retried without losing history.
- [ ] Runtime concurrency never exceeds the configured bound under a delayed dependency.
- [ ] Graceful shutdown stops claims and handles active leases correctly.
- [ ] Logs and metrics cover every lifecycle transition and contain no high-cardinality identifiers as metric labels.
- [ ] Seeded chaos tests pass repeatedly and report enough state to debug failures.
- [ ] Database constraints and tests enforce the listed invariants.
- [ ] README contains architecture, semantics, quick start, demo, and explicit non-guarantees.
- [ ] ADRs/failure lab demonstrate the reasoning progression rather than only the final design.
- [ ] Full quality gate passes without ignored failures or warnings.

## 19. Implementation constraints

- Prefer explicit SQL and small repository functions over a generic ORM/domain framework.
- All state-changing SQL must use guarded predicates and verify affected row count.
- Use UTC and database time for scheduling/lease comparisons.
- No `unwrap`, `expect`, or `panic!` in normal runtime paths; startup may fail fast with a contextual error for invalid configuration.
- No unsafe Rust unless an ADR proves necessity; expected count is zero.
- Pin toolchain and dependencies; commit `Cargo.lock`.
- Use typed configuration with documented defaults and validation.
- Bound HTTP body sizes, connection pools, request timeouts, batches, payload sizes, and concurrency.
- Use parameterized SQL only.
- Handle shutdown signals on supported platforms.
- Tests own isolated database schemas/databases and clean up safely.
- Keep production failpoints unreachable unless an explicit development build/flag enables them.
- Do not add a message broker or distributed lock service.
- Do not replace PostgreSQL correctness with process-local mutexes.
- Avoid speculative abstractions. Each public trait/module needs a current use or test seam.
- Comments explain invariants and failure windows, not syntax.

## 20. Commit plan

Create small, buildable commits. Recommended subjects:

1. `docs: define reliableq guarantees and state machine`
2. `chore: scaffold rust workspace and local postgres`
3. `feat: add durable job submission and inspection api`
4. `feat: add naive polling worker and fake charge service`
5. `test: reproduce worker crash leaving stranded job`
6. `feat: add expiring leases and fenced finalization`
7. `test: reproduce ambiguous duplicate charge window`
8. `feat: make charge side effects idempotent`
9. `feat: classify failures and schedule bounded retries`
10. `feat: add dead jobs and explicit replay`
11. `feat: bound worker concurrency and graceful shutdown`
12. `feat: add lifecycle metrics and structured tracing`
13. `test: add seeded multi-worker chaos scenarios`
14. `docs: finish operations guide and project narrative`

Each commit must pass formatting and relevant focused tests. Before the final commit, run the complete gate. Do not rewrite published/user commits or combine milestones unless explicitly instructed. If no Git repository exists, initialize only if authorized by the surrounding environment; otherwise prepare changes and list the intended commits without fabricating them.

## 21. Blog and interview artifacts

Create these under `docs/blog/`:

- `01-naive-queue.md` — initial contract, state machine, normal path, and stranded-work failure.
- `02-leases-are-not-exactly-once.md` — lease design, fencing, pause/expiry overlap, limitations.
- `03-idempotency-closes-the-ambiguity-window.md` — exact crash timeline and database uniqueness approach.
- `04-backoff-dlq-and-backpressure.md` — failure taxonomy, retry math, dead jobs, bounded concurrency.
- `05-chaos-results.md` — test setup, seeds, scenarios, observed results, bugs found, remaining risks.

Also provide:

- An architecture diagram and job-state diagram in Mermaid in `README.md`.
- A 5–10 minute `scripts/demo.sh` (or documented task-runner target) that submits jobs, shows success, injects a transient failure, demonstrates a crash/reclaim, proves idempotent replay, and inspects a dead job.
- `docs/interview-notes.md` containing: a 60-second summary, a 5-minute deep dive, likely trade-off questions, and honest production extensions.
- ADRs for PostgreSQL as queue, at-least-once semantics, lease/fencing design, idempotency scope, and retry algorithm.

Blog prose must be derived from actual implementation and test evidence. Leave placeholders marked `TODO: run and record` until evidence exists; never invent results.

## 22. Autonomous agent operating instructions

1. Read this spec completely, then inspect the repository and any `AGENTS.md` before editing.
2. Create a milestone checklist and work on only the earliest incomplete milestone.
3. Preserve existing user changes; do not reset or overwrite unrelated work.
4. At milestone start, write the reasoning artifact and failing reproduction required by Section 15.
5. Make the smallest coherent implementation; run focused checks frequently.
6. Resolve failures by finding their cause. Do not suppress warnings, skip tests, broaden timeouts blindly, or weaken invariants.
7. Update README/design/operations material as behavior changes.
8. Run the milestone exit checks and record real evidence.
9. Commit only after the milestone passes. If committing is unavailable or unauthorized, stop at a clean, tested diff and provide the exact proposed commit.
10. Continue until every acceptance criterion is checked or a genuine external blocker remains.
11. When blocked, document the attempted commands, exact error, what is known, and the minimum user action required. Continue any independent work first.
12. Final report must include implemented milestones, test commands and actual results, migrations/config changes, residual risks, and commit hashes. Never claim completion if any required acceptance item is unverified.

## 23. Definition of done

ReliableQ is done when a reviewer can start from a clean checkout, follow the README, observe each important failure mode and its mitigation, run the full test/chaos suite, and explain from repository evidence why the system offers durable acceptance, at-least-once execution, recoverable leases, idempotent bundled side effects, bounded retries, dead-job handling, and bounded concurrency—without confusing any of those properties with universal exactly-once execution.
