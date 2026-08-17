# ADR 0002: At-least-once execution, not exactly-once

- Status: accepted
- Date: 2026-08-17 (M1)

## Context

A job's handler can be interrupted at any point — before, during, or
after its side effect — by a worker crash, network partition, or lease
expiry. Once M2 adds lease-based reclaim, an interrupted job's second
attempt is a certainty, not an edge case. The project has to pick and
name an honest delivery/execution guarantee up front.

## Decision

ReliableQ guarantees **at-least-once execution**: a job's handler may
run more than once for the same job. It does not attempt exactly-once
execution. The one guarantee ReliableQ makes about the *externally
visible effect* is scoped to the bundled charge handler specifically,
via idempotency (see ADR 0004), not to arbitrary job handlers in
general.

## Rationale

- Exactly-once *execution* is not achievable without a distributed
  transaction spanning the database and every downstream side effect,
  which SPEC.md sec. 3 rules out as a non-goal.
- At-least-once is the honest guarantee for a system with expiring
  leases and no external transaction coordinator: the moment a lease
  can expire and be reclaimed while the original worker might still be
  running (paused, GC stall, slow network), two workers can be
  executing the same job concurrently. Claiming to prevent that outright
  would be false.
- Making the *effect* idempotent (charges table's unique
  `idempotency_key`) turns "the handler ran twice" into "the customer
  was charged once," which is the property that actually matters to
  callers, without requiring exactly-once execution underneath it.

## Consequences

- Every job handler this project ships (currently: charge) must be
  written assuming it can be invoked more than once for the same job.
  A handler without an idempotent downstream effect does not inherit
  this project's safety for free — that is a property of the specific
  handler, not the queue.
- README/DESIGN.md must never describe the system as exactly-once
  (SPEC.md sec. 4); this ADR is the canonical reference for why.
- Chaos tests (M8) must include scenarios where the same job executes
  twice and assert the *effect* still happened once, not that execution
  itself was deduplicated.
