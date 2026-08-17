# Operations

Practical procedures for running and troubleshooting ReliableQ. See
[`DESIGN.md`](../DESIGN.md) for guarantees/non-guarantees and
[`docs/failure-lab.md`](failure-lab.md) for the reasoning behind each
mechanism referenced here.

## Startup

```bash
cp .env.example .env
make up        # start local postgres (docker compose)
make migrate   # apply migrations (idempotent)
make run-api            # reliableq-api on :8080, /metrics on the same port
make run-fake-charge    # fake-charge on :8081
make run-worker         # polls the queue, /metrics on :9091
```

Each binary applies pending migrations itself at startup
(`reliableq_db::run_migrations`), so starting them directly (without
`make migrate` first) is safe — the first one to start applies the
schema, the rest see nothing pending.

Readiness order does not matter for correctness: the worker will simply
find no due jobs until the API has accepted some, and the API will
report `503` on `/health/ready` until it can reach PostgreSQL.

## Shutdown

Send `SIGINT` (Ctrl-C) or `SIGTERM` to any binary.

- **API / fake-charge**: axum's graceful shutdown stops accepting new
  connections and lets in-flight requests finish.
- **Worker**: stops claiming new jobs immediately, then waits up to
  `WORKER_SHUTDOWN_GRACE_SECS` (default 30s) for whatever batch was
  already in flight to finish — each in-flight job keeps renewing its
  own lease during this window. Anything still running past the grace
  period is abandoned, **never marked successful**; its lease expires
  naturally and becomes reclaimable by the next worker that polls
  (spec sec. 9.5, ADR 0006).

There is no data-loss risk in an abrupt `SIGKILL` either: a job with no
surviving owner just sits `RUNNING` until its lease expires, then M2's
reclaim path picks it up. The only cost is latency, not correctness.

## Inspecting stuck or dead jobs

```bash
# A specific job, with its full attempt history:
curl -s http://localhost:8080/v1/jobs/<id> | jq

# Jobs stuck RUNNING longer than expected (should be rare — a job's
# lease should have already expired and been reclaimed by M2's path;
# if you see many of these, check whether any worker process is alive
# at all):
curl -s 'http://localhost:8080/v1/jobs?status=RUNNING&limit=200' | jq

# All dead jobs (spec sec. 8.2 convenience view):
curl -s http://localhost:8080/v1/dead-jobs | jq

# Paginate with the returned next_cursor:
curl -s 'http://localhost:8080/v1/jobs?status=DEAD&limit=50&cursor=<next_cursor>' | jq
```

Each job's `attempts` array shows every historical attempt with its
`outcome` (`SUCCEEDED`, `RETRY_SCHEDULED`, `DEAD`, or `LEASE_LOST`) and
`error_code`/`error_message` — this is usually enough to tell whether a
dead job failed permanently (a validated business rejection, e.g.
`422`) or exhausted its retry budget against a flaky/down dependency
(`RETRY_BUDGET_EXHAUSTED` as the terminal `last_error_code`, or
`LEASE_EXPIRED_BUDGET_EXHAUSTED` if the worker died mid-attempt on its
last try).

Direct SQL, if you need it (read-only, never write directly to `jobs`
or `job_attempts` outside the application — every write path is
guarded for a reason):

```sql
-- Jobs by status, oldest first:
SELECT id, kind, status, attempts, max_attempts, last_error_code, created_at
FROM jobs WHERE status = 'DEAD' ORDER BY created_at ASC LIMIT 50;

-- A specific job's full attempt trail:
SELECT attempt_number, worker_id, outcome, error_code, scheduled_delay_ms, duration_ms
FROM job_attempts WHERE job_id = '<id>' ORDER BY attempt_number;
```

## Replaying a dead job

```bash
# Default: max_attempts becomes (current attempts + 5)
curl -s -X POST http://localhost:8080/v1/jobs/<id>/retry | jq

# Explicit budget (must exceed the job's current attempts count):
curl -s -X POST http://localhost:8080/v1/jobs/<id>/retry \
  -H 'Content-Type: application/json' -d '{"max_attempts": 20}' | jq
```

- `200` with the reset job (now `PENDING`) on success.
- `404` if the job ID doesn't exist.
- `409 INVALID_STATE` if the job is not currently `DEAD` (only dead jobs
  can be retried — this is not a way to cancel/reschedule active work).

Retry reuses the same job ID and therefore the same charge idempotency
key (`reliableq:charge:<job_id>`, ADR 0004). If the job had already
charged successfully before dying for an unrelated reason, retrying it
**replays** that charge rather than duplicating it — verified in
`crates/reliableq-worker/tests/dead_job_retry.rs`. Attempt history is
preserved; the new attempt continues numbering from where the old one
left off.

## Interpreting metrics

`GET /metrics` on the API (`:8080` by default) and the worker (`:9091`
by default) in Prometheus text format.

| Metric | Type | Labels | What it tells you |
|---|---|---|---|
| `reliableq_jobs_submitted_total` | counter | `kind` | Submission rate. |
| `reliableq_job_attempts_total` | counter | `kind`, `outcome` | Attempt outcomes over time — compare `SUCCEEDED` vs `DEAD` vs `RETRY_SCHEDULED` rates. |
| `reliableq_job_duration_seconds` | histogram | `kind`, `outcome` | How long attempts take; watch p99 against your lease duration. |
| `reliableq_downstream_requests_total` | counter | `result` (`success`/`transient`/`permanent`/`ambiguous`/`unreachable`) | Is the downstream itself healthy? |
| `reliableq_retries_scheduled_total` | counter | `reason` | Volume of retries by failure code — a spike means a downstream is degrading. |
| `reliableq_dead_jobs_total` | counter | `reason` | `RETRY_BUDGET_EXHAUSTED` vs a specific permanent-rejection code — tells you whether dead jobs are a downstream outage or bad input data. |
| `reliableq_lease_renewals_total` | counter | `result` (`ok`/`lost`/`error`) | `lost` renewals mean a worker is running jobs slower than its own lease duration allows, or another worker reclaimed the job out from under it. |
| `reliableq_lease_expirations_reclaimed_total` | counter | — | Crash-recovery activity. Should track worker restarts/crashes, not be constantly high during steady-state operation. |
| `reliableq_inflight_jobs` | gauge | — | Current concurrent executions on this worker process; should never exceed `WORKER_CONCURRENCY`. |
| `reliableq_job_queue_depth` | gauge | `status` | Backlog by state; a persistently growing `PENDING` depth means workers can't keep up. |
| `reliableq_oldest_pending_age_seconds` | gauge | — | The single most useful "are we falling behind" signal — alert on this, not just queue depth, since depth alone doesn't distinguish a burst from real lag. |

`reliableq_inflight_jobs`, `reliableq_lease_renewals_total`,
`reliableq_lease_expirations_reclaimed_total`, and
`reliableq_downstream_requests_total` are only observed by the worker
process (they describe execution, which only the worker does) and are
therefore only present on the worker's `/metrics`, not the API's.
`reliableq_jobs_submitted_total` is API-only for the same reason.
`reliableq_job_queue_depth` and `reliableq_oldest_pending_age_seconds`
are refreshed by the API every 5 seconds from a direct table scan —
expect up to that much staleness.

## Recovery procedures

**A worker process died.** Nothing to do. Its claimed jobs' leases
expire on schedule (`WORKER_LEASE_DURATION_SECS`, default 30s) and
become reclaimable by any other running worker. If no worker is
running at all, jobs will simply sit `RUNNING` (or `PENDING`) until one
starts — start `reliableq-worker` and it will pick up where things left
off. No manual intervention, no data repair needed.

**A worker is up but the queue depth is growing.** Check
`reliableq_inflight_jobs` against `WORKER_CONCURRENCY` — if it's
pegged at the limit, either raise `WORKER_CONCURRENCY` (if the
downstream can take it) or run more worker processes (they safely
share the queue — SPEC.md invariant: claiming is `FOR UPDATE SKIP
LOCKED`, workers never double-claim). Check
`reliableq_downstream_requests_total{result="transient"}` — a slow or
flaky downstream will bottleneck the queue even with plenty of worker
concurrency available.

**A batch of jobs is dying with the same permanent error code.** That's
usually a real bug or bad input, not an infrastructure problem —
`GET /v1/dead-jobs`, inspect `last_error_message`, fix the root cause
(a bad deploy, a schema change on the downstream, etc.), then
`POST /v1/jobs/{id}/retry` each one (or write a small script that pages
through `/v1/dead-jobs` and retries jobs matching a specific
`last_error_code`).

**Postgres was briefly unreachable.** `/health/ready` on the API
reports `503` during the outage; the worker's claim/finalize calls
fail and are logged (`failed to claim jobs, backing off` /
`failed to finalize ...`), then retried on the next poll cycle once
Postgres recovers. No manual repair — every state-changing statement is
guarded and atomic, so a failed attempt at reading or writing job state
never leaves a job half-transitioned.

**Suspected duplicate charge.** Should not happen for the bundled
charge handler (ADR 0004) — check
`SELECT * FROM charges WHERE idempotency_key = 'reliableq:charge:<job_id>'`
and confirm exactly one row. If you find more than one, that is a bug
in this project, not expected behavior; file it with the job ID and
both charge IDs.

## What ReliableQ does not do (so you don't page someone for it)

See [`DESIGN.md` sec. 2](../DESIGN.md#2-explicit-non-guarantees) for
the full list. The two that come up operationally most:

- A job's handler **can** run more than once for the same job (at-least-once,
  not exactly-once). This is expected under lease expiry/reclaim, not a bug.
- There is no cross-worker-fleet concurrency limit — `N` workers each
  configured for `WORKER_CONCURRENCY = C` can produce up to `N × C`
  concurrent downstream calls in total.
