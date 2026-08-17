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
