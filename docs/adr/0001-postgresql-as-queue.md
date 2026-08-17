# ADR 0001: PostgreSQL as the job queue

- Status: accepted
- Date: 2026-08-17 (M0)

## Context

ReliableQ needs a durable place to store jobs, claim them exactly-once
per attempt among competing workers, and record an audit trail —
without adding a message broker or distributed lock service (SPEC.md
sec. 19 explicitly forbids both).

## Decision

Use a single PostgreSQL database as the source of truth for jobs,
attempts, and charges (DESIGN.md sec. 4). Claiming uses
`SELECT ... FOR UPDATE SKIP LOCKED` inside a short transaction, not an
external queue or broker.

## Rationale

- `FOR UPDATE SKIP LOCKED` gives exactly the concurrency primitive a
  job queue needs — competing workers never block on each other and
  never double-claim a row — without introducing a second system to
  operate, back up, or reason about consistency with.
- Every other piece of state (job status, attempt history, charge
  idempotency) already needs to live in a relational store with real
  constraints (see `migrations/0001_init_schema.sql`); colocating the
  queue there means one transaction can claim a job *and* write its
  attempt row atomically, with no distributed-transaction problem.
- A dedicated broker (Kafka/RabbitMQ/Redis) would add delivery
  semantics ReliableQ does not need (pub/sub fan-out, topic
  partitioning) and would still require a separate durable store for
  job/attempt state, doubling the failure surface for no benefit at
  this project's scale.

## Consequences

- Throughput is bounded by what a single PostgreSQL instance's row
  locking can sustain — acceptable for the stated non-goal of
  horizontal sharding/multi-region (SPEC.md sec. 3).
- Polling (not push notification) is the default claim trigger
  (SPEC.md sec. 9.5); `LISTEN`/`NOTIFY` is optional future work, not a
  correctness requirement.
- All state-changing SQL must use guarded predicates and check affected
  row counts (SPEC.md sec. 19) — there is no ORM abstracting this away.
