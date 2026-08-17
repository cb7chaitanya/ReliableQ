# ReliableQ Benchmark Design

This document is written *before* the benchmark harness exists (per the
project's own reasoning discipline — see `SPEC.md` sec. 15). It states what
questions the benchmark suite answers, how load is generated, what is
measured, how correctness is verified alongside performance, and what is
known in advance to add noise to the numbers this harness will produce.

The suite measures **this repository's implementation on one developer
machine**. It is not a capacity-planning tool, not a comparison against any
other queue, and every result it produces must be read as a **local
benchmark**, not a universal claim (see "Environment controls" below and the
labeling requirement in every generated artifact).

## 1. Benchmark questions

In order of what a reader of `docs/benchmarking/results.md` should walk away
knowing:

1. **Ingestion capacity.** How many `POST /v1/jobs` requests/sec can the API
   durably accept (row committed before `202`), and how does client
   concurrency change p50/p95/p99 latency and error rate? This isolates the
   API + PostgreSQL insert path from worker execution entirely (zero
   workers running).
2. **Execution overhead at zero downstream cost.** With the downstream
   charge call answering instantly, what throughput ceiling does
   ReliableQ's own machinery (claim transaction, HTTP round trip to
   `fake-charge`, finalize transaction, lease heartbeat) impose, and how
   does it scale with worker concurrency?
3. **Execution throughput against a realistic downstream.** With a fixed,
   representative downstream latency (100 ms), how closely does observed
   throughput track the naive ceiling `concurrency / handler_latency`, and
   where does it fall short?
4. **Multi-process scaling.** Does splitting a fixed total concurrency
   budget across more worker *processes* change aggregate throughput,
   fairness, or database pressure — independent of whether the environment
   even needs more than one process?
5. **Claim-batch sizing.** Does the size of one claim transaction's batch
   change claim latency, transaction rate, or downstream queue latency
   enough to matter, and where do diminishing returns start?
6. **Downstream-latency sensitivity.** How does end-to-end throughput and
   queue growth degrade as the one thing ReliableQ cannot control — the
   downstream's own latency — gets worse?
7. **Retry cost under real failure rates.** At a given transient-failure
   rate, how much attempt volume (retry amplification) does the backoff
   policy generate, what does that cost in eventual-success latency, and
   how many jobs still end up `DEAD`?
8. **Backlog drain behavior.** Starting from a large pre-existing backlog
   with workers off, then turning workers on: how long to drain, what does
   sustained throughput look like once claiming and executing overlap
   steadily, and what happens to tail latency for jobs stuck at the back of
   a long queue?
9. **Crash-recovery cost.** For each of the three named failpoints (and a
   real `kill -9` of a live worker process), how long does it take from
   worker death to lease expiry to reclaim to terminal state — and does
   recovery ever cost a duplicate charge or a budget violation?
10. **Idempotency-key contention.** Under concurrent identical requests
    sharing one idempotency key, is exactly one charge created regardless
    of concurrency, and what does the serialization cost (via the unique
    constraint) look like as concurrency rises?

No single number answers all of these; §8 of this document explicitly
forbids collapsing the report to one throughput figure.

## 2. What already exists to build on

Read before writing any new code (SPEC.md sec. 1, "inspect the repository
first"):

- **Metrics.** `/metrics` on both the API (`:8080`) and worker (`:9091`) in
  Prometheus text format — `reliableq_jobs_submitted_total`,
  `reliableq_job_attempts_total{kind,outcome}`,
  `reliableq_job_duration_seconds{kind,outcome}` (histogram),
  `reliableq_downstream_requests_total{result}`,
  `reliableq_retries_scheduled_total{reason}`, `reliableq_dead_jobs_total`,
  `reliableq_lease_renewals_total{result}`,
  `reliableq_lease_expirations_reclaimed_total`,
  `reliableq_inflight_jobs` (gauge), `reliableq_job_queue_depth{status}`
  (gauge, refreshed every 5s), `reliableq_oldest_pending_age_seconds`
  (gauge, same refresh cadence — so it is a lagging, not instantaneous,
  signal at high load).
- **Test-only control surface on `fake-charge`**, mounted only when
  `FAKE_CHARGE_ENABLE_TEST_CONTROL=true` (`crates/fake-charge/src/chaos.rs`):
  `POST /v1/test/control` (`normal` / `fail_next{n,status}` /
  `permanent_reject` / `delay_ms{ms}`), `GET /v1/test/inflight`
  (`peak_inflight`, a high-water-mark counter — exactly the M6 bounded-
  concurrency evidence, directly reusable for correctness gate item
  "measured in-flight work never exceeds configured capacity"), and
  `POST /v1/test/inflight/reset`.
- **Failpoint injection**, but only reachable through the *library*, not the
  compiled `reliableq-worker` binary: `reliableq_worker::failpoint::{Failpoints,
  FailpointName}` and `execute_and_finalize_with_failpoints` are `pub`, and
  `tests/chaos/seeded_chaos.rs` already proves the pattern — call
  `reliableq_db::jobs::claim_pending_jobs` directly, then
  `execute_and_finalize_with_failpoints` per claimed job with a custom
  `Failpoints` impl. `reliableq-worker/src/main.rs` never exposes a flag to
  turn this on in the shipped binary — intentionally, per SPEC.md sec. 12
  ("safe from accidental production activation"). The benchmark harness
  reuses this exact library-level pattern for scenario I's three named
  failpoints; it does not add a production flag to do it a different way.
- **Configuration.** Every tunable the scenarios below need already exists
  as an env var read by `reliableq_core::config` — `WORKER_CONCURRENCY`,
  `WORKER_LEASE_DURATION_SECS`, `WORKER_POLL_INTERVAL_MS`,
  `WORKER_CHARGE_SERVICE_URL`, `DATABASE_MAX_CONNECTIONS`, etc. — **except
  one**: the worker's per-poll claim batch cap
  (`CLAIM_BATCH_MAX: i64 = 10` in `crates/reliableq-worker/src/poll.rs`) is
  a private, hardcoded constant, not configurable. Scenario E (claim-batch
  sensitivity) needs to vary this. Rather than adding a new env var to
  production code to chase this one benchmark, the harness reimplements a
  minimal claim/execute driver directly against `reliableq_db::jobs` and
  `reliableq_worker::execute_and_finalize` (see §5) — the same technique
  `tests/integration/happy_path.rs` and `tests/chaos/seeded_chaos.rs`
  already use to drive one controlled poll cycle without going through the
  compiled worker binary. **This means scenario E measures the claim
  transaction and execution pipeline at a chosen batch size, not the
  shipped worker binary's own (fixed-at-10) batching** — documented
  explicitly in that scenario's results, not blurred into a general
  worker-binary throughput claim.
- **fake-charge as a library**, not just a binary — `fake_charge::build_app`
  / `fake_charge::AppState` / `fake_charge::chaos::ChaosState` are all
  `pub`, reused as-is by the existing integration and chaos test harnesses.
- **One real, additive gap**: scenario G (retry degradation) needs a
  *persistent probabilistic* transient-failure mode ("fail N% of calls"),
  not the existing `fail_next{n}` ("fail exactly the next N calls, then
  stop"). This does not exist yet. §4 documents the one small, opt-in
  addition to `fake-charge`'s already-test-only chaos control this
  requires — the only production-adjacent code this benchmarking task
  touches, and it is inert unless `FAKE_CHARGE_ENABLE_TEST_CONTROL=true` is
  set, exactly like every other chaos mode already in that file.

## 3. Workload model

- **Job shape.** All scenarios submit `kind: "charge"` jobs (the only kind
  the system executes) with a small, deterministic JSON payload
  (`customer_id`, `amount_cents`, `currency`) generated from the run's job
  index so payloads are reproducible and distinguishable in the database
  without being meaningfully different in size or shape from each other.
  `customer_id` embeds the scenario name and job index
  (`bench-<scenario>-<n>`) so leftover rows from an aborted run are easy to
  identify and are never mistaken for another scenario's data.
- **Job IDs.** Left to the API's own `Uuid::new_v4()` generation (submission
  goes through the real `POST /v1/jobs` contract); the harness captures
  each returned `id` from the `202` response body for later correlation
  (claim latency, end-to-end latency, per-job attempt counts) rather than
  generating IDs itself.
- **Ingestion-only scenarios (A)** generate load with HTTP clients issuing
  `POST /v1/jobs` at a fixed concurrency (no artificial rate limiting inside
  the harness — concurrency alone is the control variable, matching how the
  scenario table specifies it).
- **Execution scenarios (B/C/D/F/G/H)** *preload* the exact configured job
  count via direct `POST /v1/jobs` calls (counted separately from the
  execution measurement — see "warm-up vs. measurement" below), confirm the
  count durably landed by querying `jobs` directly, then start real worker
  process(es) and measure from that start until drain (queue depth and
  in-flight both zero, or a scenario-specific stop condition).
- **fake-charge is always the real, compiled binary** for every scenario
  except the claim-batch driver's isolated claim-latency sub-measurement,
  which still calls the real `fake-charge` HTTP endpoint for the charge
  call itself — nothing in this suite mocks the downstream HTTP hop.

## 4. Fake-charge addition: `fail_rate` chaos mode

New `ControlRequest`/`ChaosMode` variant, additive and default-off:

```text
POST /v1/test/control
{"mode": "fail_rate", "rate": 0.10, "status": 503, "seed": 20260818}
```

- `rate` (`0.0..=1.0`persists (like `delay_ms`) until reset — every request
  independently draws one uniform sample and fails with the given `status`
  when the draw is below `rate`.
- `seed` is optional; when given, the draw sequence is reproducible for a
  fixed request order (useful for a repeatable scenario-G run); when
  omitted, entropy-seeded (fine for a benchmark, not for a correctness
  test — this suite's correctness gate never depends on which specific
  requests fail, only on the aggregate invariants in §7).
- Implementation: a `Mutex<StdRng>` field on `ChaosState`, independent from
  the existing mode `Mutex`, so `ChaosMode` keeps deriving `Copy`. Covered
  by a new unit test in `crates/fake-charge/tests/charges.rs` asserting the
  observed failure proportion over a few hundred trials is within a wide
  statistical tolerance of the configured rate (same style as
  `reliableq-core::retry`'s existing statistical-spread test — a tolerance
  band, not an exact-count assertion). This is the **only** change this
  benchmarking task makes to any crate outside `reliableq-bench` and
  `benchmarks/`, and it ships disabled by default exactly like every
  existing chaos mode.

## 5. Harness architecture

`crates/reliableq-bench` — one binary crate, workspace member, depending on
`reliableq-core`, `reliableq-db`, and `reliableq-worker` as libraries (same
pattern the chaos/integration test crates already use) plus `reqwest`,
`sqlx`, `serde`/`serde_json`, `tokio`, `clap`, `rand`. It never links
against a mocked anything — every scenario talks to real PostgreSQL and, for
worker/API/fake-charge, either the real compiled `--release` binaries
(spawned as child processes) or the same library entry points the project's
own tests already use to drive one controlled claim/execute cycle (used
only for scenario E's batch-size sweep and scenario I's three named
failpoints, per §2's gap explanation — every other scenario runs the real
binaries).

```text
crates/reliableq-bench/
  src/
    main.rs           CLI (clap): `--config <toml> --scenario <name|all> --repeat N --out <dir>`
    config.rs         BenchConfig (deserialized from quick.toml / full.toml)
    env_info.rs        captures git commit/dirty, rustc version, os/arch, cpu, mem, docker/postgres version
    result.rs          RunResult (the schema in SPEC-BENCH sec. 6 below) + JSON writer, one file per run
    stats.rs            p50/p95/p99/max/throughput helpers, from raw per-request/per-job samples
    procs.rs             spawn/wait-ready/graceful-stop/kill-9 real api/worker/fake-charge child processes
    resource.rs          periodic `ps`-based CPU/RSS sampling per child PID; `docker stats` for postgres
    chaos_client.rs      thin HTTP client for fake-charge's /v1/test/* control surface
    correctness.rs       the invariant gate (SPEC-BENCH sec. 7), run after every scenario
    db.rs                 preload/query helpers (submit N jobs, count by status, drain-wait, etc.)
    scenarios/
      ingestion.rs         A
      execution.rs          B, C, F (one latency×concurrency sweep; B and C are two points on it)
      scaling.rs             D
      claim_batch.rs         E
      retry_degradation.rs   G
      backlog.rs              H
      crash_recovery.rs        I
      idempotency.rs            J
```

### Warm-up vs. measurement

Every scenario that measures a rate (A, B/C/F, D, E, H) runs a **warm-up
phase** first — a smaller, fixed-size burst through the exact same code
path (same process count, same concurrency) that is executed and discarded
(its rows still land in the database and count toward correctness checks,
but its timings are never included in the reported percentiles/throughput).
This absorbs first-connection TCP/TLS-less handshake cost, PostgreSQL plan
cache warm-up, and OS-level page-cache warm-up for the ReliableQ binaries
themselves. Warm-up size is config-driven (`warmup_jobs` /
`warmup_requests` per scenario in the TOML), smaller in the quick profile.
Scenarios G, I, and J are not throughput sweeps in the same sense and do
not use a separate warm-up phase; their "measurement" is the scenario
itself (see each scenario's own section).

### Repetition

Every *published* scenario/parameter-point combination runs **3 times**
(`--repeat 3`, the harness's default), each a fully independent process:
fresh preload, fresh worker start, fresh correctness gate. Raw results are
written per-run (`benchmarks/results/<scenario>/<params>/<timestamp>-run<N>.json`);
`docs/benchmarking/results.md` is generated from all raw runs, reporting the
distribution across repeats (not just their mean) so run-to-run variance on
a shared laptop is visible rather than hidden.

### Interruption handling

Every spawned child process is tracked by PID in the harness's own process
table; a `Ctrl-C`/`SIGTERM` to `reliableq-bench` itself triggers a shutdown
path that kills every tracked child, drops any bench-owned Postgres schema,
and writes a `status: "interrupted"` marker into that run's result file (so
report generation and the "did this actually complete" check in §9 can
never mistake a half-finished run for a clean one). The harness never
touches a process or database object it did not itself create — see
"database isolation" below.

## 6. Result schema (`RunResult`, one JSON object per run)

```json
{
  "schema_version": 1,
  "timestamp_utc": "2026-08-18T12:00:00Z",
  "scenario": "execution_sweep",
  "scenario_params": { "downstream_latency_ms": 100, "worker_concurrency": 8 },
  "run_number": 1,
  "status": "ok",
  "git_commit": "b0da27a3e3d15e11239a6b2c3e5a288ed57c6ebe",
  "git_dirty": false,
  "rust_version": "1.94.0",
  "build_profile": "release",
  "os": "macos",
  "os_version": "24.4.0",
  "architecture": "arm64",
  "cpu_model": "Apple M-series (via sysctl machdep.cpu.brand_string, or \"unavailable\")",
  "logical_cpu_count": 8,
  "memory_bytes": 17179869184,
  "docker_version": "27.4.0",
  "postgres_version": "16.x (from SELECT version())",
  "postgres_configuration": { "max_connections": "...", "shared_buffers": "..." },
  "api_process_count": 1,
  "worker_process_count": 1,
  "worker_concurrency": 8,
  "claim_batch_size": 10,
  "database_pool_sizes": { "api": 10, "worker": 10 },
  "lease_duration_secs": 30,
  "heartbeat_interval_secs": 10,
  "retry_configuration": { "base_delay_ms": 1000, "multiplier": 2, "max_delay_secs": 60 },
  "fake_charge_latency_ms": 100,
  "fake_charge_failure_mode": "normal",
  "job_count": 10000,
  "warmup_duration_secs": 5.2,
  "measurement_duration_secs": 41.7,
  "throughput": { "unit": "jobs_per_sec", "value": 239.6 },
  "latency_percentiles": { "unit": "ms", "p50": 32.1, "p95": 61.4, "p99": 88.0, "max": 210.5 },
  "error_counts": { "http_errors": 0, "timeouts": 0 },
  "resource_measurements": {
    "api": { "cpu_pct_avg": 4.1, "cpu_pct_peak": 12.0, "rss_bytes_peak": 41943040 },
    "worker": { "...": "..." },
    "fake_charge": { "...": "..." },
    "postgres": { "cpu_pct_avg": null, "note": "docker stats unavailable in this environment" }
  },
  "correctness_results": {
    "passed": true,
    "checks": { "submitted_equals_durable": true, "terminal_state_consistent": true, "...": "..." }
  }
}
```

Every field is always present; a value the environment cannot supply is
`null` with an accompanying `"...": "unavailable: <reason>"` note in the
same object, never a fabricated number (§9's re-read of raw results checks
specifically for silently-substituted zeros where `null` belongs). A run
whose worktree was dirty is still recorded (so an accidental dirty run
during development isn't silently lost) but `report.rs` **excludes any
`git_dirty: true` run from the published `results.md`** and prints a
warning instead — matching this task's "reject or clearly mark results
produced from a dirty worktree" requirement without discarding the
evidence entirely.

## 7. Correctness gate

Run directly against PostgreSQL after every scenario (`correctness.rs`),
independent of whatever the scenario's own timing measurement already
believed happened:

1. `submitted_durable_count == expected_job_count` (every job the harness
   believes it submitted has a row).
2. Every job the scenario tracked by ID is accounted for (found, with a
   known status) — no silently-lost job.
3. `count(status='SUCCEEDED') + count(status='DEAD') + count(status IN
   ('PENDING','RUNNING'))== total`, and for scenarios that run to drain,
   the non-terminal count is `0`.
4. `count(*) FROM jobs WHERE attempts > max_attempts` is `0`.
5. `job_attempts(job_id, attempt_number)` uniqueness and monotonicity —
   enforced by the schema's own unique constraint (§ verified by asserting
   no gaps larger than expected from `LEASE_LOST` attempts, not by
   re-deriving the constraint).
6. No idempotency key backs more than one `charges` row (the exact seeded-
   chaos-suite check, reused verbatim).
7. For scenarios with idempotency-key contention (J), a differing-payload
   reuse never returns `2xx` — checked by inspecting the harness's own
   captured HTTP responses, not just the database.
8. `peak_inflight` (from `fake-charge`'s `/v1/test/inflight`, reset before
   each scenario) never exceeds `worker_process_count * worker_concurrency`
   — the direct, already-instrumented measurement of "in-flight work never
   exceeds configured capacity," reused rather than re-derived.
9. For crash-recovery scenarios (I), no `job_attempts` row shows a
   `SUCCEEDED`/`DEAD` outcome recorded *after* that job was already
   re-claimed by a different `worker_id` with a later attempt number (the
   direct database evidence that a stale owner never finalized a reclaimed
   job — the fencing guarantee, checked by querying the actual production
   fencing implementation's output, not by re-implementing the fencing
   logic in the checker).

Any failed check sets `correctness_results.passed = false` and the whole
run is marked invalid in `results.md` regardless of how good its timing
numbers look (§7 of the task, restated as this document's own commitment).

## 8. Environment controls and known noise sources

- **Fixed inputs, recorded every run**: git commit/dirty flag, Rust
  version, build profile, OS/arch, logical CPU count, total memory, Docker
  version, PostgreSQL version and the subset of `SHOW ALL` this project's
  own tuning cares about (`max_connections`, `shared_buffers`,
  `effective_cache_size` if set).
- **PostgreSQL runs in the project's own `docker-compose.yml`**, unmodified
  — no benchmark-specific tuning of Postgres itself, so results reflect
  what a reader following `README.md`'s own quick start would actually get,
  not a hand-tuned database.
- **Known noise, stated up front rather than discovered mid-report**:
  - This is a single developer laptop (Apple Silicon, macOS), not a
    dedicated benchmarking host — no CPU pinning, no isolated network
    namespace, Docker Desktop's VM layer adds its own scheduling and
    virtualized-disk overhead versus bare-metal Linux Postgres.
  - Docker Desktop on macOS runs containers inside a Linux VM; disk I/O for
    PostgreSQL's WAL crosses a virtualized filesystem boundary, which is
    slower and less predictable than a native Linux host — this alone can
    depress commit-latency-sensitive numbers (ingestion, claim/finalize
    transactions) relative to a native deployment.
  - Background OS/user processes on a shared laptop are not suspended
    during a run; `resource_measurements` for the *bench-owned* processes
    are still meaningful, but ambient system load is a source of run-to-run
    variance the 3x repeat is specifically meant to surface, not eliminate.
  - `reliableq_job_queue_depth` / `reliableq_oldest_pending_age_seconds`
    refresh on a 5-second cadence (by design, see `docs/operations.md`) —
    any chart built from them has up to 5s of staleness baked in; the
    harness's own queue-depth-over-time sampling for the backlog scenario
    polls `jobs` directly instead, to avoid inheriting that lag.
  - `ps`-based CPU% sampling on macOS is coarse (roughly one sample per
    second, OS-level accounting granularity) — treated as directional
    (relative comparison across concurrency levels), not as
    profiler-grade attribution.
  - The full 100k-job backlog scenario and the full concurrency × latency
    matrices are the ones most likely to be materially reshaped by a
    dedicated host; the quick profile intentionally excludes or shrinks
    them (§9's execution report says exactly which ones actually ran here).

## 9. Planned commands and output formats

```bash
# One-time environment prep (idempotent):
make up && make migrate
cargo build --workspace --release

# Quick profile (small job counts, few concurrency points, excludes the
# 100k backlog scenario by default — matches "smaller defaults for quick
# local runs"):
make bench-quick

# Full profile (published-result scale; long-running):
make bench-full

# Regenerate docs/benchmarking/results.md (and benchmarks/reports/*.svg
# charts) from whatever raw JSON already exists in benchmarks/results/:
make bench-report
```

Every `RunResult` is one JSON file. `make bench-report` runs
`reliableq-bench report` (reads every non-dirty, non-interrupted raw run,
groups by scenario+params, computes cross-repeat distributions), writes
`docs/benchmarking/results.md`, and renders the same raw data as SVG charts
(via the `plotters` crate's SVG backend, in-process — no external plotting
tool or hand-edited chart) into `benchmarks/reports/*.svg`, embedded from
`results.md`. Nothing in `results.md` is a number or image produced any
other way (§8 of the task).
