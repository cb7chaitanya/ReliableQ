# Chaos results

*Part 5 (final) of the ReliableQ series. Part 4: [`04-backoff-dlq-and-backpressure.md`](04-backoff-dlq-and-backpressure.md).*

Seven milestones built seven mechanisms, each proven in isolation: one
crashed worker, one dropped response, one slow downstream, one
shutdown signal. Real systems don't fail one thing at a time. This
part is about proving the mechanisms still hold when they're all
happening together, under randomness, with nobody choosing exactly
which job fails how.

## What "chaos" means here, concretely

120 jobs. Three simulated worker processes running concurrent
claim-and-execute rounds. A 250ms lease — short on purpose, so a
"crash" (see below) recovers fast enough to run the whole thing in a
couple of seconds instead of minutes. A seeded RNG deciding, for every
single job execution, whether to trigger one of three named crash
points:

```rust
enum FailpointName {
    AfterClaimBeforeEffect,      // M2's stranded-RUNNING scenario
    AfterEffectBeforeFinalize,   // M3's duplicate-charge scenario
    DuringFinalize,              // crash after the DB call, before reacting to it
}
```

Plus, early in the run, a real burst of 40 consecutive `503`s from
fake-charge — not simulated, an actual HTTP layer failure — so some
jobs have to survive a genuinely degraded downstream, not just a
worker that vanishes.

## The failpoint interface, and why production code doesn't know it exists

```rust
pub trait Failpoints: Send + Sync {
    fn should_trigger(&self, name: FailpointName, job_id: Uuid) -> bool;
}
```

`execute_and_finalize` — the one function the real poll loop calls —
is a two-line wrapper around `execute_and_finalize_with_failpoints`
that always passes `NoopFailpoints`. Nothing about the production path
changed to make this possible. The chaos suite calls the
`_with_failpoints` variant directly; everything else in the codebase,
including every earlier milestone's test, is unaware this trait
exists. That's what "prefer a trait-based failpoint interface with a
no-op production implementation" (spec sec. 12) is actually for: not
just "disabled by default," but structurally absent from any code path
that didn't explicitly ask for it.

## What actually happens when AfterEffectBeforeFinalize fires

This is the scenario worth walking through, because it's the one that
would actually be dangerous if something upstream of it were broken:

1. Simulated worker A claims job J, lease expires in 250ms.
2. A calls fake-charge. The charge commits — a real row exists in
   `charges` now.
3. The failpoint fires. A "crashes" — returns immediately, does not
   finalize.
4. J sits `RUNNING` with a lease that's now ticking down.
5. 250ms later (well within the next couple of rounds), the lease
   expires. Some other simulated worker's claim query picks J back up
   as a reclaim — M2's mechanism, not anything chaos-specific.
6. That worker retries the charge call. Same job ID, same
   `reliableq:charge:<job_id>` idempotency key (M3/ADR 0004). fake-charge
   recognizes it and replays instead of inserting.
7. J finalizes `SUCCEEDED`. Exactly one row exists in `charges` for
   that job.

Nothing in the chaos test *tells* the system to do this. It falls out
of M2 and M3's mechanisms running unattended, because the seeded RNG
happened to land on this exact interleaving for some subset of the 120
jobs, in among two other failpoints and a downstream outage, with two
other simulated workers doing the same thing concurrently.

## The assertions, and what they'd have caught

```rust
let duplicate_charge_keys: i64 = sqlx::query_scalar(
    "SELECT count(*) FROM (SELECT idempotency_key FROM charges \
      GROUP BY idempotency_key HAVING count(*) > 1) dup",
).fetch_one(&pool).await?;
assert_eq!(duplicate_charge_keys, 0);
```

This is the sharpest one. It doesn't ask "did the AfterEffectBeforeFinalize
scenario happen correctly" — it asks a strictly weaker, strictly more
useful question: across *every* job, under *every* combination of
failpoints and downstream failures the seed happened to produce, did
idempotency hold even once? A single duplicate row anywhere fails the
whole run. The other three checks are just as unconditional: every job
terminal, none over its retry budget, none lost or duplicated as a
*job* (separate from the charge-duplication check).

## Results

```text
seeded_chaos_suite: SEED=20260817 JOB_COUNT=120 WORKER_COUNT=3
seeded_chaos_suite: stopped after 35 rounds, reached_quiescence=true
seeded_chaos_suite: SEED=20260817 rounds=35 succeeded=114 dead=6 total=120
test seeded_chaos_suite_converges_and_holds_every_invariant ... ok
```

Run three more times back to back: 50/35/32 rounds, 115/116/114
succeeded, 1.6-2.5s wall time each. Run again with a second seed
(`424242`, unrelated to the first): 34 rounds, 114 succeeded, 6 dead.
Zero invariant violations across every run. The round count and
succeed/dead split move around because real async scheduling isn't
fully deterministic even with a seeded RNG — the seed makes the
*failpoint decisions* reproducible, not wall-clock timing — which is
the honest thing to report rather than pretending a concurrent chaos
test is bit-for-bit deterministic.

## Bugs this approach found

None, in the sense of "the chaos suite caught something the
per-milestone tests missed." Every mechanism it exercises — lease
reclaim, fencing, idempotency, retry/backoff, dead-job terminal
transitions — was already individually proven with a red-then-green
test at the milestone that introduced it. What the chaos suite adds
isn't new mechanism coverage; it's confidence that those mechanisms
compose under real concurrency and real randomness instead of only
working in the clean, single-job scenarios each milestone's test set
up by hand. The one real bug this project's process *did* catch — the
shutdown-drain permit-counting race — showed up in M6's own test
suite, not here, which is itself worth noting: a bug in code this
project wrote was found by a small, targeted test, not by throwing
randomness at the system. Chaos testing is a check on composition, not
a substitute for reasoning about each mechanism on its own.

## What's still not covered

- No true process kills (`SIGKILL` on a real OS process) — the
  failpoints simulate a crash by returning early within the same
  process, which exercises the exact same "abandoned RUNNING row /
  abandoned in-flight charge" state a real crash produces, but does
  not exercise OS-level concerns (a `SIGKILL` mid-syscall, a corrupted
  in-flight TCP write). ReliableQ's design doesn't depend on avoiding
  those — the whole point of lease expiry is not needing to know
  exactly how a worker died — but it's worth being precise that this
  suite proves the queue's reaction to abandonment, not literal
  process-kill survival.
- No network partition between the worker and PostgreSQL specifically
  (only between the worker and fake-charge, via the `503` burst).
- Concurrency is bounded per simulated worker in this test the same
  way it is in production (M6) — the chaos suite doesn't add a fourth
  kind of failure for exceeding that bound, since M6's own tests
  already measure it directly against a real delayed downstream.
