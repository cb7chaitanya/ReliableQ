# ReliableQ

A durable, database-backed background-job system in Rust. See
[`DESIGN.md`](DESIGN.md) for guarantees/non-guarantees and the state
machine, [`SPEC.md`](SPEC.md) for the full implementation brief, and
[`docs/failure-lab.md`](docs/failure-lab.md) for the milestone-by-
milestone reasoning trail.

> Status: M0 (contract, skeleton, local environment) complete. See
> `docs/failure-lab.md` for what is and is not proven so far.

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
make run-worker         # placeholder until M1
make run-fake-charge    # placeholder until M1
```

`make down` stops postgres. See `Makefile` for every target.

## Architecture

See [`DESIGN.md`](DESIGN.md#4-architecture) for the component diagram
and [`DESIGN.md`](DESIGN.md#3-state-machine) for the job state machine.
A full README with demo script and interview notes lands at milestone
M8 (see `SPEC.md` sec. 16, 21).

## Non-guarantees

ReliableQ provides at-least-once execution, not exactly-once. See
[`DESIGN.md` sec. 2](DESIGN.md#2-explicit-non-guarantees) for the full
list of what this project deliberately does not do.
