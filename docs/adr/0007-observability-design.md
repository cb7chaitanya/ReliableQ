# ADR 0007: Observability design — per-process metrics, task-local correlation, tracing spans over threaded parameters

- Status: accepted
- Date: 2026-08-17 (M7)

## Context

Through M6 every reliability guarantee in this project was provable by
running the test suite, but none of it was *visible* to an operator
watching a running system: no metrics endpoint, no way to correlate a
single logical operation's log lines across the API, the worker, and
fake-charge, and lease tokens never appeared in logs at all only
because nothing had reason to log them yet — not because of an active
redaction policy.

## Decisions

**1. Each process exposes its own `/metrics`, not just the API.**
`reliableq_job_attempts_total`, `reliableq_downstream_requests_total`,
`reliableq_lease_renewals_total`, `reliableq_inflight_jobs`, and
`reliableq_lease_expirations_reclaimed_total` describe *execution*,
which only the worker process observes. Centralizing them on the API's
`/metrics` would require the worker to either push metrics somewhere
(a pushgateway — explicitly out of scope, adds a component) or expose
them itself. The worker gets its own small `/metrics`-only HTTP server
(`WORKER_METRICS_BIND_ADDR`, default `:9091`) for exactly this reason.
`reliableq_jobs_submitted_total` and the two whole-table gauges
(`reliableq_job_queue_depth`, `reliableq_oldest_pending_age_seconds`)
stay on the API's `/metrics` since only the API observes submission,
and either process could compute the table-wide gauges equally well.

**2. Request correlation uses `tokio::task_local!`, not a parameter
threaded through every function.** The error envelope's `request_id`
field (spec sec. 8) needs to be readable from `ApiError::into_response`,
which the `IntoResponse` trait does not give request/header access to.
Rather than attach a request ID to every `ApiError` at every
construction site (dozens of call sites via `?`), a `tokio::task_local!`
set once by a request-scoped middleware makes the current request's ID
readable from anywhere in that request's call tree, including
`From` impls invoked by `?`.

**3. Structured logs get `worker_id` via a tracing span at the top of
the poll loop (`#[instrument]`), not a parameter on every logged
function.** The same reasoning as (2): passing `worker_id: &str` into
`execute_and_finalize`, `execute_charge`, `spawn_heartbeat`, and every
`tracing::` call site inside them would work, but spans are the
mechanism `tracing` provides for exactly this — attach context once,
have it propagate. The one subtlety worth recording: spans do **not**
cross a `tokio::spawn` boundary automatically. `spawn_bounded_batch`
captures `tracing::Span::current()` before spawning and instruments
each spawned task's future with it explicitly
(`.instrument(span.clone())`) — otherwise every per-job log line inside
a spawned task would silently lose the `worker_id` field the moment
M6's bounded concurrency made execution actually concurrent.

**4. Lease tokens are logged as a 12-hex-character SHA-256 fingerprint,
never raw.** `reliableq_core::redact::lease_token_hash` exists
specifically so "same token across log lines" stays visually
correlatable in logs without a raw UUID appearing anywhere that could,
even indirectly, be replayed against the fencing guard it protects.
Implemented with the `sha2` crate (already a transitive dependency via
sqlx) rather than hand-rolled hashing — cryptographic primitives are
exactly the code not worth writing from scratch even for a 12-character
log fingerprint.

## Rationale for what's *not* done

- **No distributed tracing (OpenTelemetry spans across process
  boundaries).** The `X-Request-Id` header the worker sends to
  fake-charge (`job:<job_uuid>`) gives log correlation across
  processes, but it is not a true trace originating from the API
  request that created the job — that would require persisting a
  request ID on the `jobs` row at submission time and threading it
  through every later attempt, a real design change to the schema this
  milestone did not need to make to satisfy spec sec. 13.3's literal
  ask ("Propagate request IDs to the charge service").
- **No push-based metrics.** Scrape-based `/metrics` per process
  matches how every other piece of this system is designed: no new
  infrastructure component, no additional failure mode from a metrics
  pipeline going down independently of the thing it measures.

## Consequences

- Anyone adding a new async task spawned from within the worker must
  remember to `.instrument()` it with the current span, or its logs
  will silently lose `worker_id`. Nothing enforces this at compile
  time; it's a pattern to follow, not a guarantee.
- `current_request_id()` falls back to a fresh UUID if called outside
  the middleware's task-local scope (e.g. from a background task not
  spawned within a request) rather than panicking — a defensive
  default, not a claim that such a call site has a real inbound
  correlation ID to report.
