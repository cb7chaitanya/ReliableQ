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
