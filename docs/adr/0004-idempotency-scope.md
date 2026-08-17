# ADR 0004: Idempotency scope and mechanism

- Status: accepted
- Date: 2026-08-17 (M3)

## Context

M2's lease/fencing design left one gap by design (ADR 0003): a worker
that loses its lease can still have a network call to the charge
service in flight, or may have already completed one, before it
discovers the loss. At-least-once execution (ADR 0002) makes a second
attempt at the same job a certainty once retries or reclaim exist.
Reproduced concretely in
`crates/reliableq-worker/tests/duplicate_charge.rs`: with M1/M2's
attempt-scoped idempotency key, a claim → charge → (simulated crash,
no finalize) → reclaim → charge sequence produced **two** charge rows
for one logical job.

## Decision

1. The idempotency key sent to the charge service is derived
   deterministically from the job ID alone —
   `reliableq:charge:<job_uuid>` — constant across every attempt of the
   same job, never including the attempt number.
2. The charge service's write path (`insert_or_get_charge`) is a single
   atomic statement: `INSERT ... ON CONFLICT (idempotency_key) DO
   NOTHING RETURNING *`. If it returns a row, this request created the
   charge. If not, it looks up the existing row and compares payloads:
   identical payload → replay (`200`, `replayed: true`); different
   payload → conflict (`409 IDEMPOTENCY_CONFLICT`).
3. Idempotency is scoped to exactly one thing: the bundled charge
   handler, keyed by job ID. It is not a general-purpose idempotency
   framework for arbitrary job kinds or arbitrary downstream calls.

## Rationale

- Only a *deterministic, stable* key lets the downstream service
  recognize "this is the same logical request" across separate HTTP
  calls from separate worker processes. An attempt-scoped or random key
  can never do this — no re-execution would ever look like a replay to
  the receiving service, no matter how good its dedup logic is. This is
  why the M3 fix required a change to *both* the caller (worker) and
  the callee (fake-charge), not just one.
- `ON CONFLICT ... DO NOTHING RETURNING *` is a single round trip with
  the row lock already held by the constraint index — there is no
  separate `SELECT`-then-`INSERT` window for two concurrent requests to
  both see "no existing row." This is what makes
  `concurrent_inserts_with_the_same_key_produce_one_row` pass
  deterministically rather than being a rare-flake test.
- Treating a payload mismatch on a reused key as a *conflict*, not a
  silent replay of the old row, matters because silently returning the
  original charge would let a caller believe a different amount or
  customer was charged when it wasn't — worse than an error.
- Scoping this to the one bundled handler (not a general framework) is
  intentional per SPEC.md sec. 3 non-goals: there is no commitment here
  to make arbitrary future job kinds or arbitrary downstream systems
  idempotent for free.

## Consequences

- Every job kind this project ever adds needs its own idempotency
  strategy if it needs one; `charge_idempotency_key` is not reusable
  for a different handler without its own dedicated dedup mechanism at
  the downstream service.
- `POST /v1/jobs/{id}/retry` (M5) reuses the same job ID, and therefore
  automatically reuses the same idempotency key — a dead job that had
  already charged before going dead will replay, not double-charge, on
  manual retry. This is a direct, free consequence of keying on job ID
  rather than attempt number, worth calling out because M5 will rely on
  it without needing new idempotency logic.
- This does not make the *job's* execution exactly-once (ADR 0002
  stands); it makes the one externally visible *effect* happen once.
