# ReliableQ

A durable, database-backed background-job system in Rust. See
[`DESIGN.md`](DESIGN.md) for guarantees/non-guarantees and the state
machine, [`SPEC.md`](SPEC.md) for the full implementation brief, and
[`docs/failure-lab.md`](docs/failure-lab.md) for the milestone-by-
milestone reasoning trail.

> Status: M2 (expiring leases) complete — a worker crash no longer
> strands work: an expired RUNNING lease is reclaimable, and stale
> lease tokens are fenced out of finalization. Idempotency, retries,
> dead jobs, and bounded concurrency are not implemented yet by design;
> see `docs/failure-lab.md` for what's proven so far and what each
> milestone still owes.

## Quick start

Requires the pinned toolchain in `rust-toolchain.toml` (installed
automatically by `rustup` if you have it) and Docker.

```bash
cp .env.example .env

make up        # start local postgres (docker compose)
make migrate   # apply migrations
make gate      # fmt-check + clippy + full test suite
```

Run a binary individually:

```bash
make run-api            # reliableq-api on :8080
make run-worker         # polls and executes charge jobs
make run-fake-charge    # naive charge sink on :8081
```

`make down` stops postgres. See `Makefile` for every target.

With all three running, submit a job and watch it succeed:

```bash
curl -s -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"kind":"charge","payload":{"customer_id":"c1","amount_cents":500,"currency":"INR"},"max_attempts":5}'

curl -s http://127.0.0.1:8080/v1/jobs/<id-from-above>
```

## Architecture

See [`DESIGN.md`](DESIGN.md#4-architecture) for the component diagram
and [`DESIGN.md`](DESIGN.md#3-state-machine) for the job state machine.
A full README with demo script and interview notes lands at milestone
M8 (see `SPEC.md` sec. 16, 21).

## Non-guarantees

ReliableQ provides at-least-once execution, not exactly-once. See
[`DESIGN.md` sec. 2](DESIGN.md#2-explicit-non-guarantees) for the full
list of what this project deliberately does not do.
