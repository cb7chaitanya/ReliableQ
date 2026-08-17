# Failure Lab

Each milestone that introduces a reliability mechanism is recorded here
following the loop in SPEC.md sec. 15: state the invariant, describe the
naive design, name the failure window, reproduce it, record evidence,
then justify the smallest fix. M0 has no failure to reproduce yet — it is
scaffolding — so its entry instead records the baseline proof required
before any failure demonstration is meaningful.

## M0 — Contract, skeleton, and local environment

**Learning objective.** Before any reliability mechanism can be
demonstrated as necessary, there must be a real, runnable baseline: a
workspace that builds, a schema that migrates cleanly from empty, and a
process that can prove its own dependencies are reachable.

**Baseline proof (in place of a failure reproduction).**

1. `crates/reliableq-core` provides typed, validated configuration
   (`DatabaseConfig`, `HttpConfig`, `LogFormat`) so a misconfigured
   process fails fast at startup with a specific error rather than
   misbehaving. 7 unit tests cover missing/invalid values and defaults.
2. `crates/reliableq-db` embeds `migrations/0001_init_schema.sql`
   (jobs, job_attempts, charges — see DESIGN.md sec. 3) and exposes a
   migration runner. Its smoke test
   (`migrations_apply_cleanly_to_empty_schema`) creates an isolated
   PostgreSQL schema, runs every migration against it, asserts the
   exact resulting table set, and drops the schema — proving a clean
   checkout can migrate an empty database.
3. `crates/reliableq-api` boots, applies migrations, and serves
   `GET /health/live` (never touches the database) and
   `GET /health/ready` (asserts the database is reachable). Both were
   exercised against a live `docker-compose` PostgreSQL instance:

   ```text
   $ curl -s -w '\nstatus=%{http_code}\n' http://127.0.0.1:8080/health/live
   {"status":"ok"}
   status=200

   $ curl -s -w '\nstatus=%{http_code}\n' http://127.0.0.1:8080/health/ready
   {"status":"ok"}
   status=200
   ```

4. `reliableq-worker` and `fake-charge` exist as explicit placeholder
   binaries so the five-crate architecture required by SPEC.md sec. 5
   is present; their behavior is intentionally deferred to M1.

**Evidence: full local quality gate**, run against the docker-compose
PostgreSQL instance (`make up && make gate`):

```text
cargo fmt --all -- --check         -> clean
cargo clippy --workspace \
  --all-targets --all-features \
  -- -D warnings                   -> clean, 0 warnings
cargo test --workspace \
  --all-features                  -> 11 passed; 0 failed
  reliableq-core: 7 config tests
  reliableq-api:  3 health-endpoint tests
  reliableq-db:   1 migration smoke test
```

**Residual risk carried into M1.** Nothing here demonstrates recoverable
work, idempotency, retries, or bounded concurrency — those require a
naive baseline to fail against first, which M1 builds and M2 onward
breaks on purpose. Do not read M0's passing gate as evidence of any
reliability guarantee beyond "the scaffolding builds, migrates, and
reports its own health."

## M1 — Naive durable queue

**Learning objective.** Establish the normal, no-failure path end to
end — submit, claim, execute, finalize — as the concrete baseline the
rest of the project breaks on purpose. SPEC.md sec. 16 exit criterion:
"a submitted charge job succeeds normally; document why crash recovery
is incomplete."

**The naive design.**

- `POST /v1/jobs` validates and inserts a `PENDING` row, returning
  `202` only after commit (`reliableq-api::jobs::submit_job`).
- `reliableq-worker` polls on a fixed interval with full jitter, claims
  due `PENDING` rows with `FOR UPDATE SKIP LOCKED` in one short
  transaction (no network call inside it — invariant 13), sets a lease
  token/expiry, and writes the matching `job_attempts` row, all before
  returning to call the charge service.
- Execution calls `fake-charge`'s naive, non-idempotent `POST
  /v1/charges` (see this file's M1 entry is paired with the sibling gap
  documented in `crates/fake-charge/src/charges.rs`) with an
  **attempt-scoped**, not job-scoped, `Idempotency-Key`:
  `reliableq:charge:<job_id>:attempt:<n>`.
- Finalization is a single guarded transaction:
  `WHERE id = $1 AND status = 'RUNNING' AND lease_token = $2`. Success
  moves the job to `SUCCEEDED`; **any** failure — validation, downstream
  rejection, unreachable service — moves it straight to `DEAD`. There is
  no retry policy yet (that is M4).

**Why crash recovery is incomplete (the failure window left open on
purpose).** If a worker process dies after committing the claim
transaction but before it calls `finalize_success`/`finalize_dead`, the
job is left `RUNNING` with a lease token that nothing will ever check
again:

1. Worker A claims job `J` → `RUNNING`, lease token `T`, lease expiry
   `now()+30s`.
2. Worker A crashes (process killed, panic, OOM) before the charge call
   resolves.
3. `claim_pending_jobs`'s `WHERE` clause only matches `status =
   'PENDING'`. `J` is `RUNNING` and stays `RUNNING` forever — no reaper,
   no expired-lease reclaim exists yet.
4. `J` is now permanently stuck: not visible to any future claim, not
   retried, not marked `DEAD`. The only signs are `GET /v1/jobs/J`
   showing `RUNNING` with a `finished_at` that never arrives, and (once
   M7 metrics exist) an oldest-in-flight-job gauge that never resets.

This is exactly invariant 4 ("stale tokens cannot finalize or renew")
and the state-machine's `RUNNING --lease expires--> claimable RUNNING`
edge (DESIGN.md sec. 3) with no implementation behind it yet. M2's
job is to reproduce this stranded-`RUNNING` scenario as a deterministic
test, then add lease expiry + reclaim + fencing so a new worker can
finish what the dead one started.

**Evidence.**

- End-to-end happy path
  (`tests/integration/happy_path.rs::submitted_job_succeeds_and_persists_exactly_one_charge`):
  submits a job through the real API, drives one worker poll cycle
  in-process against the real fake-charge service, asserts `SUCCEEDED`
  with one attempt and exactly one charge row. Passing.
- Manually run end to end (api + fake-charge + worker, all real
  processes, against docker-compose postgres):

  ```text
  $ curl -s -X POST http://127.0.0.1:8080/v1/jobs -d '{...}'
  {"id":"ce90fd4a-...","status":"PENDING","attempts":0,"max_attempts":5,...}

  $ curl -s http://127.0.0.1:8080/v1/jobs/ce90fd4a-...
  {"status":"SUCCEEDED","attempts":[{"attempt_number":1,"outcome":"SUCCEEDED",...}],...}
  ```

- Full local gate (`make gate`, docker-compose postgres): fmt clean,
  clippy clean (0 warnings), **57 tests passing, 0 failed** across
  reliableq-core (24), reliableq-db (15 + 1 migration smoke test),
  reliableq-api (13), fake-charge (4), reliableq-integration-tests (1).

**Residual risk carried into M2.** Stranded `RUNNING` work on worker
crash (documented above). Duplicate charges on re-execution after a
crash (the naive per-attempt idempotency key and the naive charge
service's lack of pre-check both contribute — see M3). Immediate,
unbounded-severity failure-to-`DEAD` with no retry for what might be a
transient blip (M4). Unbounded per-worker concurrency (M6).

## M2 — Demonstrate stranded work; add leases

**Invariant.** Invariant 4 (DESIGN.md/SPEC.md sec. 14): "An active
lease has exactly one token; stale tokens cannot finalize or renew,"
plus the state machine edge `RUNNING --lease expires--> claimable
RUNNING by a new owner` (DESIGN.md sec. 3).

**Naive design under test.** M1's `claim_pending_jobs` matched only
`status = 'PENDING'`. A `RUNNING` row with an expired lease was
invisible to every future claim — nothing in the system ever looked at
it again.

**Failure window.** Worker A claims job `J` (`RUNNING`, lease token
`T`, expiry `now()+30s`) → Worker A crashes before calling
`finalize_success`/`finalize_dead` → `J` stays `RUNNING` forever; no
reaper, no reclaim path exists.

**Deterministic reproduction, run against pre-fix code** (see ADR
0003; full command: `DATABASE_URL=... cargo test -p reliableq-db
--test leases`):

```text
running 5 tests
test two_workers_racing_to_reclaim_dont_double_claim ... FAILED
test expired_lease_with_exhausted_budget_becomes_dead_not_stranded ... FAILED
test expired_lease_is_reclaimable_by_a_new_worker ... FAILED
test stale_worker_cannot_finalize_after_reclaim ... FAILED
test reclaim_marks_the_abandoned_attempt_as_lease_lost ... FAILED

---- expired_lease_is_reclaimable_by_a_new_worker stdout ----
assertion `left == right` failed: an expired RUNNING lease must be reclaimable by a new worker
  left: 0
 right: 1

---- expired_lease_with_exhausted_budget_becomes_dead_not_stranded stdout ----
assertion `left == right` failed: it must be DEAD, not left stranded RUNNING with an expired lease
  left: Running
 right: Dead

test result: FAILED. 0 passed; 5 failed; 0 ignored; 0 measured; 0 filtered out
```

**The smallest mechanism (ADR 0003).** Extend `claim_pending_jobs`'s
`WHERE` clause to also match `status = 'RUNNING' AND lease_expires_at
<= now()`, under the same `FOR UPDATE SKIP LOCKED` lock as the
`PENDING` branch. Before that, close out any dangling attempt whose
lease has expired as `LEASE_LOST`, and move any expired-and-exhausted
job straight to `DEAD` (it could otherwise never be claimed again and
would be stranded a different way). Every finalize/renew statement was
already guarded by `WHERE status = 'RUNNING' AND lease_token = $N`
(built in M1) — that guard is the actual fencing mechanism token-based
reclaim relies on; nothing about it needed to change.

**What this still cannot guarantee.** A paused (not dead) worker can
resume and call the downstream charge service *after* its lease has
been reclaimed by someone else. Fencing stops it from corrupting the
job's row, but does not stop the network call — a second charge attempt
for the same logical job is still possible. Making that overlap safe is
idempotency's job (M3), explicitly out of scope here.

**Evidence, same tests against the fix:**

```text
running 5 tests
test stale_worker_cannot_finalize_after_reclaim ... ok
test expired_lease_is_reclaimable_by_a_new_worker ... ok
test two_workers_racing_to_reclaim_dont_double_claim ... ok
test reclaim_marks_the_abandoned_attempt_as_lease_lost ... ok
test expired_lease_with_exhausted_budget_becomes_dead_not_stranded ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

Full gate (`make gate`): fmt clean, clippy clean, **62 tests passing,
0 failed** (up from 57 at M1: +5 lease tests).

**Residual risk carried into M3.** The exact overlap ADR 0003 names
above — a resumed, paused worker's charge call racing a reclaiming
worker's charge call — is the concrete mechanism M3's duplicate-charge
reproduction exercises.

## M3 — Demonstrate duplicate effect; add idempotency

**Invariant.** Invariant 12 (SPEC.md sec. 14): "A crash after a
committed charge but before job finalization can cause re-execution,
but not a duplicate charge." Invariants 10-11: the charge service
creates at most one charge per idempotency key, and reusing a key for
different input never silently succeeds.

**Naive design under test.** The worker derived its idempotency key as
`reliableq:charge:<job_id>:attempt:<n>` — different on every attempt.
`fake-charge`'s `insert_charge` did a plain `INSERT`, relying entirely
on the table's unique constraint, which only helps if the *same* key is
ever sent twice.

**Failure window.** Worker A claims job `J` (attempt 1) → calls
`fake-charge` with key `...attempt:1` → charge commits, one row exists
→ Worker A crashes before calling `finalize_success` → `J`'s lease
expires (M2) → Worker B reclaims `J` (attempt 2) → calls `fake-charge`
with key `...attempt:2` (different key!) → charge commits *again* → two
charge rows exist for one logical job.

**Deterministic reproduction, run against pre-fix code**
(`crates/reliableq-worker/tests/duplicate_charge.rs`, driving
`execute_charge` directly for each attempt with no finalize in
between):

```text
running 1 test
test crash_after_charge_before_finalize_then_retry_produces_one_charge ... FAILED

thread '...' panicked:
assertion `left == right` failed: re-executing the same job must produce exactly one charge (invariant 12)
  left: 2
 right: 1

test result: FAILED. 0 passed; 1 failed
```

**The smallest mechanism (ADR 0004).** Two changes, together:

1. Worker: derive the idempotency key from the job ID alone
   (`reliableq_core::idempotency::charge_idempotency_key`), constant
   across every attempt.
2. `fake-charge`: replace the plain `INSERT` with
   `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`
   (`reliableq_db::charges::insert_or_get_charge`) — one atomic
   statement, no separate check-then-insert race window. If it returns
   a row, this call created the charge (`201`). If not, fetch the
   existing row and compare payloads: same payload → replay (`200`,
   `replayed: true`); different payload → `409 IDEMPOTENCY_CONFLICT`.

**What this still cannot guarantee.** Job *execution* is still not
exactly-once (ADR 0002) — the worker still calls the charge service
twice in the scenario above. What changed is that the second call is
now provably a no-op from the customer's point of view. A job handler
without an idempotent downstream (i.e. any handler other than the one
bundled here) does not inherit this property automatically.

**Evidence, same test against the fix, plus new coverage:**

```text
crates/reliableq-worker/tests/duplicate_charge.rs
  test crash_after_charge_before_finalize_then_retry_produces_one_charge ... ok

crates/fake-charge/tests/charges.rs (6 tests)
  reused_idempotency_key_with_same_payload_replays ... ok
  reused_idempotency_key_with_different_payload_is_a_conflict ... ok
  concurrent_duplicate_requests_produce_one_charge_row ... ok
  (+ 3 existing: first_charge_returns_201, missing key, invalid payload)

crates/reliableq-db/tests/charges.rs (6 tests)
  reusing_a_key_with_the_same_payload_replays ... ok
  reusing_a_key_with_a_different_payload_is_a_conflict ... ok
  concurrent_inserts_with_the_same_key_produce_one_row ... ok
  (+ 3 existing: round trip, missing key, first-insert)
```

Full gate (`make gate`): fmt clean, clippy clean, **71 tests passing,
0 failed** (up from 62 at M2: +5 attempt-scoped-key removal net, +6
fake-charge, +3 reliableq-db charges, +1 worker reproduction, -3
superseded M1 tests documenting the now-fixed naive behavior).

**Residual risk carried into M4.** Every execution failure — including
genuinely transient ones (timeout, `503`) — still goes straight to
`DEAD` with no retry. The failure taxonomy (transient/permanent/
ambiguous) and backoff schedule do not exist yet.

## M4 — Failure taxonomy and retries

**Invariant.** Invariant 8 (SPEC.md sec. 14): "Retry scheduling never
exceeds configured bounds." Also: permanent failures must not retry
(would waste the retry budget on outcomes that can't change), and
transient failures must not retry so tightly that they overwhelm a
struggling dependency.

**Naive design under test.** M1-M3's worker treated every failure
identically: straight to `DEAD` after one attempt, no distinction
between "this will never work" (a validated `422` rejection) and "this
might work in a second" (a `503`).

**"Initially demonstrate tight retries."** Rather than build a
throwaway naive-retry worker just to delete it, the danger of zero/tight
backoff is demonstrated directly at the mechanism `finalize_retry_scheduled`
exposes: nothing stops a caller from scheduling a retry with
`delay_seconds = 0`, and if one does, the job is claimable again with
**no wait at all**:

```text
running 1 test
test zero_delay_retry_scheduling_makes_a_job_immediately_reclaimable ... ok
```

That test passing is itself the evidence: the repository layer places
no floor on retry delay, so *only* the worker's policy — never a
default of `0` — stands between this system and a thundering-herd
retry loop against a failing downstream. That's why
`reliableq_core::retry::RetryPolicy::DEFAULT` starts at `base_delay =
1s` and full jitter draws from `[0, cap(n)]`, never a fixed point.

**The smallest mechanism (ADR 0005).** `reliableq-core::failure`
classifies downstream outcomes (`Transient`/`Permanent`/`Ambiguous`);
`reliableq-core::retry` computes `cap(n) = min(max_delay, base_delay *
multiplier^(n-1))` and `delay = uniform(0, cap(n))` with saturating
arithmetic. The worker routes: retryable + budget remaining ->
`finalize_retry_scheduled` (job back to `PENDING`, `next_attempt_at`
computed in SQL from **database** time); retryable + budget exhausted,
or permanent regardless of budget -> `finalize_dead`, with the reason
distinguishing `RETRY_BUDGET_EXHAUSTED` from a genuine permanent
failure code (spec sec. 11: metrics must be able to tell these apart).

Deterministic chaos injection (`fake-charge`'s `chaos` module, disabled
unless `FAKE_CHARGE_ENABLE_TEST_CONTROL` is explicitly set) let the
worker-level tests exercise real transient/permanent/exhaustion
sequences against the real HTTP path, not mocks.

**What this still cannot guarantee.** `Retry-After` on `429`/`503` is
not honored yet — those get the computed backoff like any other
transient failure, not the server's suggested wait. Flagged as a real
gap in ADR 0005, not silently dropped.

**Evidence.** `reliableq-core` (15 new unit tests: 5 classification, 10
retry math — cap growth/saturation/overflow-safety, delay bounds,
determinism given a seed, statistical spread). `reliableq-db` (+2:
`finalize_retry_scheduled` round-trip, the zero-delay demonstration
above). `reliableq-worker` (+3, against real chaos-injected
fake-charge): transient reschedules, permanent dies after one attempt,
repeated transient failures exhaust budget and die with
`RETRY_BUDGET_EXHAUSTED`:

```text
crates/reliableq-worker/tests/retries.rs
  test transient_failure_reschedules_instead_of_dying ... ok
  test permanent_failure_goes_dead_after_one_attempt ... ok
  test transient_failures_exhausting_budget_eventually_die ... ok
```

Full gate (`make gate`): fmt clean, clippy clean, **87 tests passing,
0 failed** (up from 71 at M3).

**Residual risk carried into M5.** Dead jobs (from either exhaustion or
a permanent failure) have no inspection endpoint and no replay path —
`GET /v1/dead-jobs` and `POST /v1/jobs/{id}/retry` do not exist yet.

## M5 — Dead jobs and operator replay

**Invariant.** SPEC.md sec. 11: "Dead jobs are never claimed
automatically. Operators can inspect and explicitly retry them." Also:
retry "must reuse the same job ID and therefore the same downstream
idempotency key" (sec. 8.3) — a direct load-bearing consequence of ADR
0004's job-scoped key.

**What M1-M4 already provide.** `DEAD` has been a real, guarded,
terminal state since M1; `dead_jobs_are_not_claimable` has been tested
since M1 and `expired_lease_with_exhausted_budget_becomes_dead_not_stranded`
since M2. What was actually missing was purely the *operator-facing*
surface: no way to list dead jobs specifically, no way to replay one.

**The mechanism.** `reliableq_db::jobs::retry_dead_job` is a single
guarded statement — `WHERE status = 'DEAD'` — that resets
status/scheduling/lease/error/`finished_at` but **preserves
`attempts`**, so the historical `job_attempts` rows stay intact and the
next attempt continues numbering monotonically rather than restarting
at 1. `POST /v1/jobs/{id}/retry` computes a new `max_attempts` (client
override, validated `> attempts`, or a default `+5` over the existing
count), returns `200` with the reset job, `404` if the job doesn't
exist, `409 INVALID_STATE` if it isn't currently `DEAD`. `GET
/v1/dead-jobs` reuses the same list/pagination logic as `GET /v1/jobs`
with status fixed to `DEAD`.

**The idempotency consequence, proven not just asserted.** Since retry
reuses the job ID, and the charge idempotency key is
`reliableq:charge:<job_id>` (ADR 0004), a dead job that already charged
before dying must replay — not double-charge — on manual retry, with
zero new idempotency logic required:

```text
crates/reliableq-worker/tests/dead_job_retry.rs
  test retrying_a_dead_job_that_already_charged_replays_not_duplicates ... ok
```

**Evidence.** 8 new API tests (retry happy path, explicit
`max_attempts` override, rejected same-or-lower override, 409 on
non-dead, 404 on missing, attempt-history preservation, dead-jobs
listing) plus the worker-level replay-on-retry test above.

Full gate (`make gate`): fmt clean, clippy clean, **95 tests passing,
0 failed** (up from 87 at M4).

**Residual risk carried into M6.** Nothing bounds how many jobs a
single worker executes concurrently — a claimed batch of up to 10 jobs
is currently processed one at a time only because the loop happens to
be sequential, not because anything enforces a limit. A delayed
downstream would let in-flight calls pile up unboundedly.

## M6 — Bounded concurrency and graceful shutdown

**Invariant.** SPEC.md sec. 14 invariant 9: "Local in-flight handler
count never exceeds configured concurrency." Also sec. 9.5: no
unbounded task queue, and shutdown must stop claiming, grant active
work a grace period, and never mark abandoned work successful.

**Naive design under test, demonstrated with a real measured
downstream.** fake-charge grew a `DelayMs` chaos mode (persists until
reset — every request sleeps N ms) and an in-flight peak counter, so
"how many calls were actually concurrent" is measured, not inferred.
Spawning all 20 claimed jobs immediately with no semaphore at all
produced a measured peak of exactly 20 concurrent calls:

```text
test unbounded_spawning_lets_every_claimed_job_run_concurrently ... ok
  (peak_inflight == JOB_COUNT == 20)
```

**The mechanism.** `spawn_bounded_batch` acquires one semaphore permit
per job inside its own spawned task, and claiming itself is capped at
`available_permits()` so unclaimed work never queues up behind a
ticking lease waiting on a permit. Each in-flight job renews its own
lease every `lease_duration / 3` via a background heartbeat, aborted
once the charge call resolves — necessary now that jobs can genuinely
run concurrently for a meaningful span. With `CONCURRENCY = 4` against
the same 150ms-delayed downstream:

```text
test bounded_batch_never_exceeds_configured_concurrency ... ok
  (peak_inflight <= 4, all 20 jobs still SUCCEEDED)
```

**A second, sharper failure found and fixed inside this milestone**
(ADR 0006, worth its own entry): the first graceful-shutdown
implementation inferred "no work in flight" from
`semaphore.available_permits() == concurrency`. A test caught this as
genuinely wrong, not flaky:

```text
test shutdown_waits_for_in_flight_work_within_grace_period ... FAILED
  left: Running
 right: Succeeded
```

Root cause: a freshly `tokio::spawn`ed task is not guaranteed to have
been polled even once by the time the spawning code continues, so its
permit hadn't been acquired yet — the drain loop read "all permits
free" while a job sat claimed and abandoned. The fix: await the actual
`JoinHandle` future the loop already had, with a timeout, instead of
inferring anything from permit counts. Re-run clean, and stable across
5 repeated runs (not a one-off pass):

```text
test shutdown_waits_for_in_flight_work_within_grace_period ... ok
test shutdown_abandons_work_that_exceeds_the_grace_period ... ok
test no_new_claims_after_shutdown_fires ... ok
```

**What this still cannot guarantee.** Concurrency is bounded *per
worker process*, not fleet-wide — running N workers each configured
for concurrency C can still produce up to N×C total in-flight calls
against the downstream. That is a deliberate scope boundary (SPEC.md
sec. 3: no multi-tenant fairness or cross-process coordination), not an
oversight.

**Evidence.** 5 new worker-level tests (2 concurrency, 3 shutdown) plus
the `renew_lease` repository test. Full gate (`make gate`): fmt clean,
clippy clean, **101 tests passing, 0 failed** (up from 95 at M5).

**Residual risk carried into M7.** No metrics exist yet — the peak/
in-flight/queue-depth numbers proven in this milestone's tests are only
visible through test assertions, not through `/metrics` an operator
could actually watch. Structured logs exist but aren't yet consistently
enriched with `request_id` end-to-end.

## M7 — Observability and operational polish

**Invariant/goal.** SPEC.md sec. 13: structured logs carry
`request_id`, `job_id`, `attempt`, `worker_id`, `lease_token_hash`
where applicable and never a raw lease token; the metric list in sec.
13.2 exists and stays low-cardinality; a failure is traceable from
submission through attempts to charge outcome using logs and
inspection endpoints alone.

**What was actually missing.** Every mechanism this project claims was
already correct and tested by M6 — this milestone made the *evidence*
visible outside the test suite: no `/metrics` anywhere, `worker_id`
never appeared in a log line, lease tokens never appeared in logs at
all (not because of active redaction — nothing had needed to log them),
and every error response minted an unrelated fresh UUID as
`request_id` instead of one traceable to an actual request.

**The mechanism (ADR 0007).** Each process gets its own `/metrics`
(the API's on `:8080`, the worker's own on `:9091`, since worker-only
signals like `reliableq_inflight_jobs` and
`reliableq_lease_renewals_total` have no way to reach the API process
without adding a push pipeline this project deliberately doesn't have).
`worker_id` is attached once via `#[tracing::instrument]` on the poll
loop and propagated through `tokio::spawn` boundaries by explicitly
capturing `tracing::Span::current()` and instrumenting each spawned
task — spans do not cross that boundary automatically, which is easy
to get wrong silently (no compiler error, just a missing field in
logs). `request_id` uses `tokio::task_local!` so
`ApiError::into_response` — which the `IntoResponse` trait gives no
request access to — can still read the current request's correlation
ID. `reliableq_core::redact::lease_token_hash` gives every lease-token
log line a stable, non-reversible 12-character fingerprint.

**Evidence, from actually running all three processes together**
(not simulated):

```text
$ curl -s -X POST http://127.0.0.1:8080/v1/jobs -d '{...}'
{"id":"fb4dc426-...","status":"PENDING",...}

$ curl -s http://127.0.0.1:8080/v1/jobs/fb4dc426-... | jq .status
"SUCCEEDED"

$ curl -s http://127.0.0.1:8080/metrics | grep ^reliableq
reliableq_jobs_submitted_total{kind="charge"} 1
reliableq_job_queue_depth{status="SUCCEEDED"} 2
reliableq_oldest_pending_age_seconds 0
...

$ curl -s http://127.0.0.1:9091/metrics | grep ^reliableq
reliableq_downstream_requests_total{result="success"} 1
reliableq_job_attempts_total{kind="charge",outcome="SUCCEEDED"} 1
reliableq_inflight_jobs 0
reliableq_job_duration_seconds{kind="charge",outcome="SUCCEEDED",...} ...

$ curl -s -D - -o /dev/null http://127.0.0.1:8080/health/live | grep -i x-request-id
x-request-id: 550f1375-e987-461d-be43-733999e02ea8

# The worker's own log line for that job, unedited:
run_worker_loop{worker_id=worker-199a0f3a-...}: reliableq_worker: job succeeded
  job_id=fb4dc426-... attempt=1 lease_token_hash=ecb17df20b92

# Chaos control confirmed inert by default:
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8081/v1/test/control -d '{"mode":"normal"}'
404
```

The log line above is the concrete proof the span-propagation-across-
`tokio::spawn` mechanism in ADR 0007 actually works: `worker_id` is
present despite being attached three call-frames away, in a task
spawned inside `spawn_bounded_batch`, not passed as a parameter
anywhere in `execute_and_finalize`. `lease_token_hash` is present and
is visibly not the raw UUID.

**What this still cannot guarantee.** No distributed trace connects a
job's API submission to its eventual worker execution — the
`X-Request-Id` the worker sends to fake-charge is derived from the job
ID at execution time, not carried from the original HTTP request that
created the job. A real end-to-end trace would need a `request_id`
column persisted on `jobs` at submission — a schema change this
milestone did not need to make to satisfy the literal spec ask.

**Evidence, full suite.** Full gate (`make gate`): fmt clean, clippy
clean, **106 tests passing, 0 failed** (up from 101 at M6).

**Residual risk carried into M8.** Every failure mode through M7 has
been demonstrated *individually*. None has been run *together*, under
seeded randomness, with multiple concurrent workers, across process
crashes — the actual chaos suite the spec's test plan (sec. 17) calls
for does not exist yet.

## M8 — Chaos suite, hardening, and narrative

**Goal.** SPEC.md sec. 17: seeded random injection across >=100 jobs
and multiple workers, checking invariants directly against PostgreSQL
after quiescence; at least one scenario where a lease expires while
the old worker is paused. SPEC.md sec. 21: finish README, ADRs, blog,
demo script.

**The mechanism.** A trait-based failpoint interface
(`reliableq_worker::failpoint::Failpoints`) checked at the three named
crash points (spec sec. 12), with `execute_and_finalize` — the only
function the real poll loop calls — remaining a thin wrapper that
always passes `NoopFailpoints`. The chaos suite
(`tests/chaos/seeded_chaos.rs`) submits 120 jobs, runs 3 concurrent
simulated workers through repeated claim/execute rounds against a
250ms lease, triggers all three failpoints at a seeded 12% probability
per job, and drives a real 40-request burst of transient `503`s early
in the run. After quiescence (polled with a bounded deadline, never a
fixed sleep), it asserts directly against PostgreSQL: every job
terminal, none over its retry budget, none lost/duplicated as a job,
and — the sharpest check — no idempotency key backing more than one
charge row.

**Evidence.**

```text
seeded_chaos_suite: SEED=20260817 JOB_COUNT=120 WORKER_COUNT=3
seeded_chaos_suite: stopped after 35 rounds, reached_quiescence=true
seeded_chaos_suite: SEED=20260817 rounds=35 succeeded=114 dead=6 total=120
test seeded_chaos_suite_converges_and_holds_every_invariant ... ok
```

Stable across 3 repeated runs (32-50 rounds, 114-116 succeeded,
1.6-2.5s wall time each — round count varies because real async
scheduling isn't fully deterministic even with a seeded RNG; the seed
reproduces the *failpoint decisions*, not wall-clock timing) and a
second, unrelated seed (`424242`): zero invariant violations in every
run. See `docs/blog/05-chaos-results.md` for the full walkthrough,
including the exact event sequence when `AfterEffectBeforeFinalize`
fires — the concrete "lease expires while the old worker is paused"
scenario spec sec. 17 asks for explicitly.

**Bugs found by actually running things, not just writing tests for
them.** Two real bugs surfaced while producing this milestone's other
deliverable, `scripts/demo.sh` — worth recording precisely because
neither was caught by the (passing, green) test suite:

1. Every chaos-control `curl` call in the first draft of the demo
   script omitted `Content-Type: application/json`. Axum's `Json`
   extractor silently rejects such a request; because the script
   piped control-call output to `/dev/null`, every injected failure
   mode (transient 503s, delayed responses, permanent rejection)
   silently never activated — every "chaos" step just showed a job
   succeeding normally. Fixed by adding a `chaos_control()` helper
   that sets the header and asserts on the HTTP status.
2. The script captured `$!` after `cargo run -p X &`, which is `cargo`
   run's own wrapper process, not the server process it spawns as a
   child. `kill -9` on that PID killed the wrapper; the actual server
   kept running, orphaned. This made the "worker crash" demo step a
   no-op — the job just finished normally because the "killed" worker
   was still alive. Fixed by exec'ing the built binaries directly
   (`./target/debug/X`) instead of through `cargo run`.
3. A genuine production bug, unrelated to the script: the API log
   showed `oldest_pending_age_seconds` failing to refresh every 5s —
   `EXTRACT(EPOCH FROM ...)` returns Postgres `NUMERIC`, and sqlx's
   `f64` decode was silently failing against it (logged as a warning,
   never crashing, gauge simply never updated). Fixed with an explicit
   `::float8` cast; see the `fix:` commit for this milestone.

None of these were caught by `cargo test` — the test suite exercises
the library code the binaries call, not the deployed-and-actually-run
binaries wired together with real shell scripting. This is the
concrete argument for actually running the demo end to end rather than
trusting that a green test suite implies a working demo script.

**Full gate, final state:**

```text
cargo fmt --all -- --check              -> clean
cargo clippy --workspace --all-targets \
  --all-features -- -D warnings         -> clean, 0 warnings
cargo test --workspace --all-features   -> 107 passed; 0 failed
```

**What remains honestly unproven** (see `docs/interview-notes.md`
"Honest production extensions" and `docs/blog/05-chaos-results.md`
"What's still not covered"): `Retry-After` handling, a true end-to-end
distributed trace ID, retention/archival, authentication, and
fleet-wide (cross-process) concurrency limiting. All are explicit
non-goals or deliberately deferred, documented rather than silently
absent.
