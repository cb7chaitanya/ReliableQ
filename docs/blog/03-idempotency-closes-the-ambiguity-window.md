# Idempotency closes the ambiguity window

*Part 3 of the ReliableQ series. Part 2: [`02-leases-are-not-exactly-once.md`](02-leases-are-not-exactly-once.md).*

Part 2 ended with a confession: fencing protects the job's row, not the
network call. A worker that loses its lease might have already told
the charge service to charge someone, or might be about to, and there's
nothing about token-based fencing that stops that call from happening.
Part 3 is about making that call safe to send twice.

## Reproducing it required breaking two things on purpose

I could have just written "the worker's idempotency key includes the
attempt number, that's obviously wrong" and moved on. Instead I wrote
the sequence out as a test, because "obviously wrong" claims are where
I've been bitten before:

```rust
// Attempt 1: claim, execute the charge call, then do NOT finalize —
// simulating a crash right after the effect committed.
let claimed1 = jobs::claim_pending_jobs(&pool, "worker-a", 10, Duration::from_millis(1)).await?;
execute_charge(&client, &charge_url, id, &claimed1[0].job.payload).await?;

tokio::time::sleep(Duration::from_millis(20)).await; // lease expires

// Attempt 2: a new worker reclaims and retries.
let claimed2 = jobs::claim_pending_jobs(&pool, "worker-b", 10, Duration::from_secs(30)).await?;
execute_charge(&client, &charge_url, id, &claimed2[0].job.payload).await?;

let charge_count: i64 = sqlx::query_scalar("SELECT count(*) FROM charges").fetch_one(&pool).await?;
assert_eq!(charge_count, 1); // <- this is what should be true
```

Run against the M1/M2 code, this failed with `left: 2, right: 1`. Two
charges, one job. Confirmed, not assumed.

## Why the naive version had two independent bugs

It's tempting to think "the database has a unique constraint on
`idempotency_key`, so duplicates can't happen." That's true — for a
*specific* key. The bug was upstream of the constraint: the worker was
generating a *different* key on every attempt
(`reliableq:charge:<job_id>:attempt:<n>`), so the unique constraint
never even got a chance to object. Two different keys, two rows,
constraint fully satisfied, customer charged twice.

That's the first fix: derive the key from the job ID alone.
`reliableq:charge:<job_id>` — nothing else. Constant across every
attempt, forever, for that job.

But that's only half of it. Even with a stable key, the *naive* insert
was a plain `INSERT`. If the same key ever did arrive twice — which is
now the expected case, not an edge case — it would hit the unique
constraint and blow up with a raw database error. Not a duplicate row,
but not a graceful response either.

## The actual fix is one SQL statement

```sql
INSERT INTO charges (id, idempotency_key, customer_id, amount_cents, currency)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *
```

If this returns a row, this call created the charge. If it returns
nothing, someone else's request — maybe this exact same logical
request, replayed — already holds that key. The handler then fetches
the existing row and compares payloads:

```rust
if existing.customer_id == customer_id
    && existing.amount_cents == amount_cents
    && existing.currency == currency
{
    InsertChargeOutcome::Replayed(existing)  // 200, replayed: true
} else {
    InsertChargeOutcome::Conflict(existing)  // 409 IDEMPOTENCY_CONFLICT
}
```

The `ON CONFLICT DO NOTHING` matters more than it looks like it should.
A naive "check if it exists, then insert if not" has a race window
between the check and the insert — two concurrent requests can both
see "doesn't exist yet" and both try to insert. `ON CONFLICT` makes the
check-and-insert atomic, so there's no window. A test proves it
directly, firing two identical requests at once:

```rust
let (a, b) = tokio::join!(
    post_charge(app.clone(), Some("race"), payload.clone()),
    post_charge(app.clone(), Some("race"), payload),
);
// exactly one 201, exactly one 200 (replayed: true), every time
```

## What idempotency does and doesn't buy you

It's worth being precise here, because it's easy to oversell this.
Idempotency does not make job execution exactly-once — the worker in
the reproduction above genuinely called the charge service twice. What
changed is that the *second* call is now provably a no-op: same
customer, same amount, same result, `replayed: true` instead of a new
charge.

That guarantee is scoped to exactly one thing: this project's bundled
charge handler, keyed by job ID. It's not a general framework. A future
job kind with a different downstream effect would need its own
idempotency story — this doesn't come for free just because the queue
underneath it is reliable.

One more thing falls out of this for free, worth flagging now even
though the feature doesn't exist until Part 4's dead-letter milestone:
an operator manually retrying a dead job reuses the same job ID,
therefore the same idempotency key, therefore the same replay
protection. A job that already charged before going dead won't
double-charge on manual retry. Nothing new had to be built for that —
it's a consequence of keying on job ID instead of attempt number.
