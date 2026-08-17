# The naive queue, and why it's not done

*Part 1 of the ReliableQ series.*

ReliableQ starts as the simplest thing that could plausibly be called a
job queue: an HTTP endpoint that inserts a row, and a worker that polls
for rows and does something with them. That's it. No leases beyond the
bare columns, no retries, no idempotency. On purpose.

## The contract, stated precisely

Before writing any of it, `DESIGN.md` pins down what "durable" actually
means here:

> Once `POST /v1/jobs` returns `202 Accepted`, the job row has committed
> and will not silently disappear.

That's a narrow, checkable claim, and the API test that backs it up is
just as narrow — submit a job, then immediately `GET` it back, no
sleep, no retry loop:

```rust
#[tokio::test]
async fn submit_is_visible_immediately_after_response() {
    let (_, submitted) = post_json(db.app(), "/v1/jobs", valid_submission()).await;
    let id = submitted["id"].as_str().unwrap();
    let (status, fetched) = get_json(db.app(), &format!("/v1/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["status"], "PENDING");
}
```

That passes because the handler doesn't return `202` until after the
`INSERT ... RETURNING *` has actually committed — there's no
fire-and-forget step in between.

## Claiming without a broker

The worker doesn't call the API and doesn't talk to any message broker.
It claims work directly out of PostgreSQL:

```sql
WITH due AS (
    SELECT id FROM jobs
    WHERE status = 'PENDING' AND next_attempt_at <= now() AND attempts < max_attempts
    ORDER BY next_attempt_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE jobs SET status = 'RUNNING', attempts = attempts + 1, ...
FROM due WHERE jobs.id = due.id
RETURNING jobs.*
```

`FOR UPDATE SKIP LOCKED` is doing all the concurrency work: two workers
running this at the same time will never claim the same row, and
neither one blocks waiting for the other. A repository test proves it
directly — twenty jobs, two workers claiming concurrently, zero
overlap:

```rust
let (a, b) = tokio::join!(
    jobs::claim_pending_jobs(&db.pool, "worker-a", 15, Duration::from_secs(30)),
    jobs::claim_pending_jobs(&db.pool, "worker-b", 15, Duration::from_secs(30)),
);
assert!(a_ids.is_disjoint(&b_ids));
assert_eq!(a.len() + b.len(), 20);
```

The claim, the attempt-row insert, and the commit all happen before any
network call — invariant 13 from the spec, and one of the easier ones
to get right by just... not writing the code that would violate it.

## What's deliberately missing

Two things are conspicuously absent from M1, and both are load-bearing
omissions, not oversights:

- **No lease recovery.** If a worker dies after claiming a job, that
  job stays `RUNNING` forever. There's no reaper, no expiry check on
  claim. This is real and it's the first thing Part 2 breaks on
  purpose.
- **No retry policy.** Any execution failure — a validation error, a
  downstream rejection, a network timeout — sends the job straight to
  `DEAD`. No backoff, no distinguishing "try again" from "give up."

Neither of these is because they're hard. They're missing because
adding them now, before anything demonstrates why they're needed, would
mean shipping a mechanism nobody's proven is load-bearing. The project
resolves that in order: reproduce the failure, then add the smallest
fix that closes it. Part 2 is the first one.

## What actually works right now

End to end, for the happy path, it's real: submit a job over HTTP,
watch a real worker process claim it, watch it call a real (if naive)
charge service, watch it land as `SUCCEEDED` with one attempt and one
charge row — verified with the actual processes running, not mocked:

```text
$ curl -s -X POST http://127.0.0.1:8080/v1/jobs -d '{...}'
{"id":"ce90fd4a-...","status":"PENDING","attempts":0,"max_attempts":5,...}

$ curl -s http://127.0.0.1:8080/v1/jobs/ce90fd4a-...
{"status":"SUCCEEDED","attempts":[{"attempt_number":1,"outcome":"SUCCEEDED",...}],...}
```

That's the baseline. Everything from here is about what happens when
it stops being the happy path.
