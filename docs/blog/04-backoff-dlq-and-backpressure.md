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

## What's still missing after M4

`Retry-After` on `429`/`503` isn't honored — those failures get the
same computed backoff as any other transient one, not the server's
suggested wait. That's a real gap, not an oversight I'm hiding; it's
recorded plainly in ADR 0005 rather than left implicit.

## Dead jobs are a state, not a second queue (M5)

`DEAD` had been a real, guarded, terminal state since the very first
milestone — the actual gap was purely operator-facing. There was no
way to list dead jobs specifically and no way to replay one. The fix
is almost embarrassingly small: `GET /v1/dead-jobs` is the same
list/pagination code as `GET /v1/jobs` with the status fixed, and
`POST /v1/jobs/{id}/retry` is one guarded SQL statement —
`WHERE status = 'DEAD'` — that resets scheduling and clears the lease
and error, but deliberately **keeps `attempts`** so the historical
`job_attempts` audit trail survives and numbering continues instead of
resetting to 1.

The interesting part is what falls out for free. Retry reuses the same
job ID. The charge idempotency key is `reliableq:charge:<job_id>` —
job-scoped, from Part 3. So a job that already charged before dying for
some unrelated reason will *replay*, not double-charge, on manual
retry — with zero new idempotency code:

```rust
// same job ID, same idempotency key, second execute_charge call
let outcome2 = execute_charge(&client, &charge_url, id, &payload).await;
assert!(outcome2.is_ok(), "replay must still report success");

let charge_count = /* query charges table */;
assert_eq!(charge_count, 1, "retry of an already-charged dead job must not double-charge");
```

That test passing isn't luck — it's the direct, provable payoff of a
design decision made two milestones earlier for an unrelated reason.

## Bounded concurrency, and a bug the tests actually caught (M6)

Every worker through M5 processed its claimed batch sequentially — one
job at a time, `await`ed in a loop. Safe, but it left the
`concurrency` config field sitting unused since M1. Making execution
actually concurrent needed two things done together: a bound (so a
delayed downstream can't have unlimited calls piled against it) and
lease renewal (so a job whose real work outlives one lease period
doesn't get its lease pulled out from under it mid-flight).

Measuring the "before" honestly required a downstream that's
*actually* slow for long enough to observe overlap — so fake-charge
grew a delay-injection mode and an in-flight peak counter. Spawning 20
claimed jobs with no bound at all produced a measured peak of exactly
20 concurrent calls. Routing the same 20 jobs through
`spawn_bounded_batch` with `concurrency = 4` produced a measured peak
of ≤ 4, with all 20 still succeeding.

That part went fine on the first try. What didn't was graceful
shutdown — and this is worth walking through because the bug was real,
not a flaky test:

```text
test shutdown_waits_for_in_flight_work_within_grace_period ... FAILED
  left: Running
 right: Succeeded
```

The shutdown drain logic polled `semaphore.available_permits() ==
concurrency` to decide "is anything still in flight." That's wrong,
and the failure proves it: `tokio::spawn` schedules a task, it doesn't
guarantee that task has been polled even once by the time the spawning
code moves on. A permit isn't decremented until the spawned task
actually reaches its `acquire_owned().await` line. On this test's
timing, shutdown fired before that first poll happened — the drain
loop saw "4 of 4 permits free," concluded nothing was running, and
returned immediately, abandoning a job that had been claimed but never
even started.

The fix removes the inference. Instead of asking whether the semaphore
*looks* idle, shutdown reuses the exact same `JoinHandle` future the
loop was already holding, and races *that* against the grace-period
timeout:

```rust
match tokio::time::timeout(worker_config.shutdown_grace, &mut batch).await {
    Ok(()) => tracing::info!("in-flight batch finished within the grace period"),
    Err(_) => tracing::warn!("grace period elapsed; abandoning in-flight work"),
}
```

A `JoinHandle` can't lie about whether its task has finished. A
permit count can, for one specific window right after spawning, and
that window is exactly where a real shutdown signal is likely to land.
Re-run five times in a row to make sure it wasn't a coincidence: clean
every time.

## Where this leaves the project

Bounded, retryable, idempotent, replayable, concurrency-safe — every
guarantee in `DESIGN.md` now has a test that would fail if it stopped
being true. What's left is making all of this *observable* from the
outside without reading test output (metrics, structured logs with
real correlation IDs — no dedicated post, see `docs/operations.md`),
and a chaos suite that runs every one of these failure modes together,
under seeded randomness, instead of one at a time. That's Part 5.
