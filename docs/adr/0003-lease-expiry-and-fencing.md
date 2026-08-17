# ADR 0003: Lease expiry and token fencing

- Status: accepted
- Date: 2026-08-17 (M2)

## Context

M1's naive worker left a job permanently `RUNNING` if the claiming
process crashed before finalizing (reproduced in
`crates/reliableq-db/tests/leases.rs::expired_lease_is_reclaimable_by_a_new_worker`,
which failed against pre-M2 code — see `docs/failure-lab.md` M2). A
fix needs two properties simultaneously: abandoned work must become
claimable again, and a worker that merely *paused* (not actually dead)
must not be able to clobber whatever a new owner does after reclaiming.

## Decision

- Every `RUNNING` job carries a `lease_token` (random UUID) and
  `lease_expires_at`, both set at claim/reclaim time from **database**
  time, never worker wall-clock time.
- `claim_pending_jobs` treats `(status='PENDING' AND due)` and
  `(status='RUNNING' AND lease_expires_at <= now())` as equally
  claimable, under the same `FOR UPDATE SKIP LOCKED` row lock, so two
  workers racing to reclaim the same expired row never both win.
- Every finalize/renew statement is guarded by
  `WHERE id = $1 AND status = 'RUNNING' AND lease_token = $2` and
  reports whether it matched a row. Zero rows matched means "you do not
  own this job anymore" — the caller must not mutate anything further.
- A job whose lease expires *and* whose retry budget is already
  exhausted moves straight to `DEAD` at claim time instead of being
  left stranded (nothing would ever be able to claim it again, since
  the same claim query's `attempts < max_attempts` guard would keep
  skipping it forever otherwise).
- The abandoned attempt row is closed out as `LEASE_LOST` by whichever
  future claim cycle notices the expiry — not by the original worker,
  which by definition may never run again.

## Rationale

- The token (not just "is the lease expired") is the actual fencing
  mechanism: expiry alone would let a worker that resumes after a long
  pause — GC stall, suspended VM, slow disk — race a legitimate new
  owner. The token guard makes that race safe: the resumed worker's
  finalize call matches zero rows and is rejected, regardless of what
  it *believes* about its own lease.
- Using database time for expiry comparisons means clock skew between
  worker processes cannot cause a lease to appear expired (or not) to
  different workers — they all evaluate `lease_expires_at <= now()`
  against the same server clock.
- Closing the abandoned attempt out as `LEASE_LOST` from the reclaiming
  side (not the original worker) is the only place that can reliably do
  it: the original worker may have crashed hard enough to never run
  this code path at all.

## Consequences

- This does **not** prevent a paused-then-resumed worker from calling
  the downstream charge service after its lease has already been
  reclaimed by someone else — it only prevents that worker from
  corrupting the *job's* state afterward. Two calls to the charge
  service for the same logical job are still possible; making that
  overlap safe is idempotency's job (M3), not leasing's.
- Lease duration is a tradeoff the operator controls
  (`WORKER_LEASE_DURATION_SECS`, default 30s): too short risks
  reclaiming work that is still legitimately in progress (a slow
  downstream call); too long delays recovery from a genuine crash.
- A dead worker's abandoned `job_attempts` row stays `outcome = NULL`
  until *some* future claim cycle observes its expiry — there is no
  separate reaper process (DESIGN.md sec. 3: "An expired RUNNING row is
  claimable directly ... a separate reaper is optional").
