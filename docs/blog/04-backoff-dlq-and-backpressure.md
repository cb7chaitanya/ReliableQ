# Backoff, DLQ, and backpressure

*Part 4 of the ReliableQ series. Part 3: [`03-idempotency-closes-the-ambiguity-window.md`](03-idempotency-closes-the-ambiguity-window.md).*

Through three milestones, every failure in this system had exactly one
outcome: `DEAD`. A `422` from a validated business rule, a `503` from a
downstream having a bad minute, a plain timeout — all identical, all
final after one attempt. That's safe, but it throws away information
the system actually has: some of those failures are worth trying again.

## Classification first, mechanism second

Before writing any retry logic, I wrote down what actually
distinguishes a failure worth retrying from one that isn't:

```rust
pub enum FailureClass {
    Transient,  // 408, 429, 5xx — a response came back, try again
    Permanent,  // other 4xx, e.g. 422 — it will never succeed
    Ambiguous,  // no response at all — safe to retry because it's idempotent
}
```

This is pure, boring logic — a match statement over a status code —
and that's exactly why it lives in `reliableq-core` with its own unit
tests, no HTTP server required:

```rust
#[test]
fn business_rejection_is_permanent() {
    assert_eq!(classify_http_status(422), FailureClass::Permanent);
}
```

## The retry math nobody should hand-roll per project

```text
cap(n) = min(max_delay, base_delay * multiplier^(n-1))
delay  = uniform(0, cap(n))
```

Full jitter, not fixed exponential backoff. The difference matters more
than it looks: fixed backoff still synchronizes a fleet — every worker
computes the same delay for the same attempt number and they all come
back at once. Drawing uniformly from `[0, cap]` spreads that out.

The implementation is unremarkable once you commit to saturating
arithmetic everywhere (nobody should get a panic because they configured
a multiplier of 1000):

```rust
pub fn cap(&self, attempt_number: u32) -> Duration {
    let exponent = attempt_number.saturating_sub(1);
    let multiplier_pow = saturating_pow(self.multiplier, exponent);
    self.base_delay.saturating_mul(multiplier_pow).min(self.max_delay)
}
```

What's worth showing is the test that isn't an exact-value assertion,
because you can't assert an exact value against jitter without
depending on production randomness (which the spec explicitly says not
to do):

```rust
let samples: Vec<Duration> = (0..200).map(|_| policy.delay(3, &mut rng)).collect();
let min = samples.iter().min().unwrap();
let max = samples.iter().max().unwrap();
assert!(max.as_millis() - min.as_millis() > 1000, "should spread out meaningfully");
```

Statistical sanity, not a fixed point. Deterministic seed, so it's
reproducible — but the assertion is about the *shape* of the
distribution, not one number.

## Proving tight retries are dangerous without building a throwaway worker

The spec's process wants you to demonstrate a naive/tight version before
justifying the fix. I could have shipped a zero-backoff worker, watched
it hammer a test dependency, then rewritten it — but that's a lot of
throwaway code for a point the repository layer can prove directly:

```rust
// Nothing stops a caller from scheduling a retry with zero delay.
jobs::finalize_retry_scheduled(&db.pool, id, lease_token, 0.0, "TRANSIENT", "simulated", 1).await?;

// No sleep. If this is claimable *right now*, that's the danger.
let reclaimed = jobs::claim_pending_jobs(&db.pool, "worker-b", 10, Duration::from_secs(30)).await?;
assert_eq!(reclaimed.len(), 1, "a zero-delay retry is due again with no backoff at all");
```

That test passing is the whole argument: the database schema and the
claim query place no floor on retry delay. The *only* thing standing
between this system and a thundering-herd retry loop is the worker
always calling `RetryPolicy::DEFAULT` (`base_delay = 1s`, never `0`).
That's a real, provable dependency, not a hand-wave.

## Testing retry behavior needs a downstream that misbehaves on command

You can't honestly test "retries 3 times then dies" against a
downstream that always succeeds. fake-charge grew a small,
deliberately-boring chaos control:

```rust
POST /v1/test/control
{ "mode": "fail_next", "n": 2, "status": 503 }
```

Disabled by default — the route isn't even mounted unless
`FAKE_CHARGE_ENABLE_TEST_CONTROL` is explicitly set, which no
deployment should ever set. With it, the worker-level tests exercise
the real HTTP path, not a mock:

```rust
harness.set_chaos_fail_next(1, 503).await;
// ... submit, claim, execute ...
assert_eq!(job.status, JobStatus::Pending, "transient failure with budget left must reschedule");
assert!(job.next_attempt_at > job.created_at);
```

And the exhaustion case — repeated transient failures past
`max_attempts` — lands on `DEAD`, but with a reason
(`RETRY_BUDGET_EXHAUSTED`) distinguishable from a genuine permanent
rejection. That distinction isn't decoration: an operator staring at a
dashboard needs to know whether jobs are dying because the business
logic is rejecting them or because a downstream has been down long
enough to burn through everyone's retry budget. Making that
distinguishable in the data now means Part 5's dead-job tooling doesn't
have to guess.

## What's still missing

`Retry-After` on `429`/`503` isn't honored — those failures get the
same computed backoff as any other transient one, not the server's
suggested wait. That's a real gap, not an oversight I'm hiding; it's
recorded plainly in ADR 0005 rather than left implicit.
