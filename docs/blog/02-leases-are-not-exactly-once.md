# Leases are not exactly-once

*Part 2 of the ReliableQ series. Part 1: [`01-naive-queue.md`](01-naive-queue.md).*

M1's worker had a bug so simple it's almost embarrassing to admit: it
had no idea what to do if it died mid-job. Claim a row, set it
`RUNNING`, call a downstream service, write the result — and if step
three never comes back because the process got killed, that row just...
stays `RUNNING`. Forever. Nothing in the system ever looks at it again.

## Proving it, not asserting it

Before touching the fix, I wrote the test I expected to fail:

```rust
#[tokio::test]
async fn expired_lease_is_reclaimable_by_a_new_worker() {
    // Worker A claims with a lease so short it's already expired.
    let claimed_a = jobs::claim_pending_jobs(&db.pool, "worker-a", 10, Duration::from_millis(1)).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Worker A crashed here, forever. Worker B polls next.
    let claimed_b = jobs::claim_pending_jobs(&db.pool, "worker-b", 10, Duration::from_secs(30)).await?;
    assert_eq!(claimed_b.len(), 1, "an expired RUNNING lease must be reclaimable");
}
```

Ran it against the M1 code. It failed exactly as predicted:

```text
assertion `left == right` failed: an expired RUNNING lease must be reclaimable by a new worker
  left: 0
 right: 1
```

Four more tests around it failed the same way — a stale worker's
finalize call, a race between two workers reclaiming the same row, and
a job whose retry budget ran out mid-abandonment. Full output is in
`docs/failure-lab.md` M2. Nothing here is hypothetical; every failure
mode has a red test run behind it.

## The fix is smaller than the bug

`claim_pending_jobs` grew one more branch. Before, it claimed rows
where `status = 'PENDING' AND next_attempt_at <= now()`. Now it also
claims rows where `status = 'RUNNING' AND lease_expires_at <= now()` —
under the exact same `FOR UPDATE SKIP LOCKED` lock, so two workers
racing to reclaim the same expired row can't both win.

Two things had to come along with that one-line-sounding change:

1. **The abandoned attempt needs closure.** Before reclaiming, the
   claim query marks the old attempt's `job_attempts` row
   `LEASE_LOST`. Otherwise the audit trail has a permanently-open
   attempt with no outcome, which is its own kind of lie.
2. **Exhausted-and-expired jobs need somewhere to go.** If a job's
   lease expires *and* it's already used up its retry budget, the
   `attempts < max_attempts` guard on the claim query would skip it
   forever — a different flavor of the exact bug I'd just fixed. So
   that case gets caught and moved straight to `DEAD` instead.

## What fencing actually buys you

Here's the part that's easy to get wrong: an expired lease alone isn't
enough. Imagine Worker A doesn't crash — it just *pauses*. A garbage
collection stall, a suspended VM, a slow disk write, whatever. Its lease
expires while it's paused. Worker B reclaims the job, finishes it,
marks it `SUCCEEDED`. Then Worker A wakes up, still thinking it owns
the job, and tries to finalize it too.

This is why every job carries a `lease_token` — a fresh random UUID
issued at claim/reclaim time — and why every finalize/renew statement
is guarded by `WHERE status = 'RUNNING' AND lease_token = $2`, checking
the *affected row count*:

```rust
let stale_ok = jobs::finalize_success(&db.pool, id, stale_token, 5).await?;
assert!(!stale_ok, "worker A's stale lease token must not be able to finalize");
```

Zero rows matched means "you don't own this anymore" — not an error,
just a fact the caller has to respect. That's the actual fencing
mechanism. Expiry decides *when* a job becomes reclaimable; the token
decides *who's allowed to finalize it*.

## What this still doesn't fix

Fencing stops Worker A from corrupting the job's row. It does **not**
stop Worker A from having already sent (or still sending) a network
request to the charge service before it discovered it lost the lease.
Two calls to the downstream service, for the same logical job, from two
different workers that both believed they owned it — that's still
possible after this milestone, and it's not a bug in the leasing
mechanism. It's leasing doing exactly what it's supposed to do (protect
the *job's* state) and nothing more.

Making that overlap safe for the one side effect this project ships is
the entire subject of Part 3.
