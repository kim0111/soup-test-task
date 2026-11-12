# Silent failures

Six ways a job queue can be wrong without raising an error. For each one: the risk, the mechanism
that prevents it, and the evidence. Evidence comes in three kinds:

- **Tests against real Postgres** (`tests/test_races.py`, `test_repo.py`, `test_worker.py`). Lock
  races are forced: one session holds an uncommitted statement, and the test polls
  `pg_stat_activity` until the other session waits on a `Lock`. Claim and create races start
  50 sessions on a barrier, with no forced order. The zombie test is a fixed sequence. The worker-loop
  tests use sub-second timings.
- **Chaos runs** (`results/chaos/INDEX.md`): 10k keys, 8 workers, `kill -9` every ~2 s, duplicate
  POSTs, cancels and (run Z) `SIGSTOP` zombies, checked by invariants I1–I8 (SPEC §8.2).
- **Mutations** (`MUTATION=M1..M4`, one `if` per query in `app/repo.py`) put back the exact bug the
  mechanism prevents. The named test and the named chaos invariant must then fail. This shows the
  evidence can detect the bug at all. A check that also passes on broken code proves nothing.

"Exactly one effect" below means one committed `job_effects` row. It holds because the effect is
in the same database as the job's status (SPEC §6). The work itself can run more than once (a
re-execution after a kill, or a double claim). An external effect, such as an S3 checkpoint or a
webhook, would degrade to at-least-once unless it is keyed by `job_id`/`attempt` or goes through
an outbox.

All runs below are full 10k-key runs on commit `b1d4a56` (code hash `30c76023c67a` in each INDEX
row), one try each:

| Run | id | Result |
|---|---|---|
| A seed 1 / 2 / 3 | `20261112-101436-s1`, `-101954-s2`, `-102520-s3` | PASS ×3: 436 kills (401 on busy workers), 351 re-executions, 940 cancels of running jobs |
| Z seed 1 / 2 | `20261112-105615-s1-z`, `-110215-s2-z` | PASS ×2: 64 freezes, 58 zombie chances |
| M1 | `20261112-103031-s1-M1` | FAIL I8 (525), all else PASS |
| M2 + `--zombie` | `20261112-103608-s1-z-M2` | FAIL I6 (26 of 27 chances), all else PASS |
| M3 | `20261112-104155-s1-M3` | FAIL I7 (27) + I8 (27), all else PASS |
| M4 | `20261112-104722-s1-M4` | FAIL I2 (4152) + I1 (4152), all else PASS |

| # | Failure | Mechanism | Test | Chaos | Mutation |
|---|---|---|---|---|---|
| 1 | Double claim | one `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED)` | `test_concurrent_claims` | I8, runs A | M1 → I8 |
| 2 | Zombie worker | fencing token `attempt` on every worker write | `test_zombie_cannot_complete` | I6, runs Z | M2 → I6 |
| 3 | Cancel / complete race | row lock + `status` predicate, re-checked after the wait | `test_cancel_complete_race_*` | I7, I5 | M3 → I7 + I8 |
| 4 | Duplicate on retry | `UNIQUE(idempotency_key)` + `ON CONFLICT` + request hash | `test_idempotent_create*` | I1, I2 | M4 → I2 + I1 |
| 5 | Isolation level | READ COMMITTED + single conditional statements | `test_naive_pattern_breaks_under_rc` | (all) | M1 |
| 6 | Clock skew | leases use DB `now()` only; workers use `time.monotonic()` | `test_no_wall_clock_in_app` | — | — |

---

## 1. Double claim

**Risk.** Two workers read the same `queued` row and both run it. Each one finishes "successfully",
so the effect happens twice. Nothing errors; the only trace is a duplicate effect.

**Mechanism.** The claim is a single statement:
`UPDATE jobs SET status='running', attempt=attempt+1 … WHERE id = (SELECT id … WHERE status='queued'
… FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING …`. The row lock is taken in the same statement that
changes the status. A second worker skips the locked row instead of waiting for it. Each claim
increments `attempt`, and that value becomes the claimant's fencing token (§2).

**Evidence.**
- `test_concurrent_claims`: 50 threads drain 20 jobs. Every job is claimed exactly once, with
  `attempt = 1`.
- Chaos I8: every `job_transitions` edge is legal (SPEC §3), so a double claim shows up as
  `running → running`. Claims per job are numbered 1..n, and every claim after the first follows a
  reaper requeue. Runs A × 3 and Z × 2: I8 PASS.
- **M1** (claim = plain `SELECT … LIMIT 1`, then `UPDATE … WHERE id=$1`):
  `test_concurrent_claims` fails, with every one of the 20 jobs claimed several times (5–42× per
  job over three runs; the counts vary from run to run). Chaos `20261112-103031-s1-M1`:
  I8 FAIL on 525 jobs, I1–I7 PASS, with 667 re-executions vs 124 in A seed 1 (same workload).
- Why I4 (≤ 1 effect) doesn't catch M1: the second claimant's `UPDATE … SET attempt=attempt+1`
  waits for the first one's row lock and then applies to the new row version, so the two claimants
  hold attempts 1 and 2 (e.g. `002f3835…`: claimed a=1 and a=2 by two workers 1 ms apart; a=1's
  complete rejected, a=2 succeeded). Both *execute*, and fencing lets only the newer one commit.
  So fencing hides the double execution, but not the double *claim*. That is why the audit trigger
  has no `WHEN old <> new` filter, and why I8 requires every claim after the first to follow a
  reaper requeue.

## 2. Zombie worker

**Risk.** A worker pauses (GC, `SIGSTOP`, VM steal) for longer than its lease. The reaper requeues
the job and another worker claims it. When the first worker resumes, it still believes it owns
the job: its heartbeat keeps the lease alive, and its complete writes a second effect, or the
first one, for a run that no longer owns the job.

**Mechanism.** Every worker write names the fencing token it was given at claim time:
`… WHERE id=$id AND status='running' AND attempt=$a`. After a re-claim the row's `attempt` is
higher, so the zombie's heartbeat, complete and fail all update 0 rows, and the worker abandons
the job without writing anything. The effect row records the *worker's* `$a`, not the row's
`attempt`, so if the fence were broken the stale token would be visible (I6). The worker holds no
transaction across its pause (autocommit, single statements), so a frozen worker holds no locks.

**Evidence.**
- `test_zombie_cannot_complete`: claim (a=1), expire the lease, reap, then try the zombie's writes
  while the job is queued; claim again (a=2), then try the zombie's writes once more. Every zombie
  write returns `False`, and the single effect carries `attempt = 2`.
- `test_worker.py::test_lost_lease_abandons_and_new_owner_completes`: the heartbeat half of the
  same check, through `run_job`. The old owner abandons on a 0-row heartbeat and writes nothing.
- Chaos I6 (effect `attempt` = final `attempt`) and I4/I5. Runs Z `20261112-105615-s1-z` and
  `-110215-s2-z`: PASS, with 28 and 30 zombie chances, i.e. a frozen worker resumed while a newer
  attempt was `running`. A Z run with 0 chances counts as INCOMPLETE, never PASS.
- **M2** (heartbeat + complete without `attempt`): `test_zombie_cannot_complete` fails (the zombie
  heartbeat returns `True`). Chaos `20261112-103608-s1-z-M2`: I6 FAIL on 26 jobs, I1–I5/I7/I8 PASS.
  All 27 chances are accounted for from the worker logs. 26 zombies completed with their stale
  token. The stale complete commits first; the new owner's next write then finds `succeeded` and
  updates 0 rows. That write was its heartbeat in 24 cases and its complete in 2. So the job has one
  effect with the wrong token: that is I6, not I4. The 27th chance was a `fail=true` job, whose
  `fail` M2 leaves fenced.
- **What run Z does not exercise** (found in the raw logs): in both Z runs, every frozen worker's
  first write after SIGCONT was a heartbeat. It updated 0 rows: in 58 freezes the fence rejected
  it (a newer attempt was running), and in 6 the new owner had already finished. None of the 64
  reached complete. The worker spends almost all its time in `time.sleep` (`run_job`, one chunk at
  a time), so a freeze almost always lands there, and the heartbeat check right after that chunk
  is overdue. A freeze during the heartbeat or reap round trip, or between the last chunk and
  `complete`, would reach `complete` with the stale token. Those windows are milliseconds per
  second of run time, and 0 of 64 freezes hit one. So run Z effectively tests only the heartbeat
  fence: a bug that removed the fence from complete *only* would very probably pass it. That bug
  is caught by `test_zombie_cannot_complete` and
  `test_repo.py::TestComplete::test_stale_attempt_writes_nothing_and_new_owner_completes`, not by
  chaos. M2 removes both fences, so its zombies got past the heartbeat and completed.
- Finding from Phase 3: M3 alone (complete without `status='running'`) also lets a zombie complete
  a *requeued* job, because `attempt` only moves on the next claim. The fence needs both predicates.

## 3. Cancel / complete race

**Risk.** A cancel answers 200 while the worker commits its result at the same moment. The client
believes the job was cancelled, but the effect happened. The reverse can also go wrong: a
`succeeded` job gets overwritten to `cancelled`.

**Mechanism.** Both are single conditional `UPDATE`s on the same row. Cancel requires
`status IN ('queued','running')`; complete requires `status='running' AND attempt=$a` and writes
the effect in the same statement (a CTE). Whichever takes the row lock first wins. Under READ
COMMITTED, Postgres re-evaluates the loser's `WHERE` against the committed row after the lock wait,
so the loser updates 0 rows:
- complete first → the cancel sees `succeeded` → **409**, and the effect stands;
- cancel first → the complete sees `cancelled` → 0 rows, **no effect**, and the cancel is a 200.

No statement names a terminal state as its source, so `succeeded`/`failed`/`cancelled` are never
left.

**Evidence.**
- `test_cancel_complete_race_complete_first` / `_cancel_first`: one session holds its `UPDATE`
  uncommitted, and the test waits until the other session is blocked on the lock. Asserts
  409 + 1 effect and 200 + 0 effects respectively.
- `test_worker.py::test_cancel_mid_run_abandons_within_a_heartbeat`.
- Chaos I7 (a cancel answered 200 ⇒ final `cancelled`; final `cancelled` ⇒ a cancel was sent) and
  I5. Runs A: 290 / 339 / 311 cancels of running jobs, PASS. Most of them reach the worker through
  a 0-row heartbeat. The race itself, a cancel committed between the last heartbeat and the
  complete, happened 18 / 26 / 28 times: every `complete_rejected` in the A runs belongs to a job
  whose cancel was answered 200. That is 72 real cancel-before-complete races, all rejected with
  0 effects.
- **M3** (complete without `status='running'`): `test_cancel_complete_race_cancel_first` fails.
  Chaos `20261112-104155-s1-M3`: I7 FAIL (27) + I8 FAIL (27, the illegal edge
  `cancelled → succeeded`), I1–I6 PASS. 901 cancels were answered 200, but only 874 jobs ended
  `cancelled`; the other 27 are exactly the cancelled jobs whose worker then logged `succeeded`.
  M3 is timing-dependent in chaos: a cancel only exposes it if it lands after the job's last
  heartbeat (otherwise the worker abandons) and before its complete (otherwise the cancel is a
  409). There is no chance counter, but the A runs measure the same window: 18–28 races per run
  on this workload, and M3 hit 27.

## 4. Duplicate on retry

**Risk.** A client retries a POST after a timeout, or sends it twice concurrently, and two jobs
are created. Or a retry with a *different* body silently returns the old job.

**Mechanism.** `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`. The unique index
arbitrates concurrent requests: exactly one inserts. A loser re-reads by key in a separate
statement, which sees the winner's committed row. If the `request_hash` matches, it returns 200
with the same job; if not, 409. The hash covers the validated, defaults-filled body, so
`{"fail": false}` and an omitted `fail` count as the same request.

**Evidence.**
- `test_idempotent_create`: 50 concurrent creates with one key give 1 × created, 49 × replay, one
  row. A different body afterwards gets 409.
- `test_idempotent_create_different_bodies_first_committer_wins`: two bodies race on one key, and
  exactly one row is created.
- `test_api.py`: an explicit default vs an omitted field on one key gives 200, not 409.
- Chaos I1 (every acknowledged key has exactly one row, with the right final state) and I2 (all
  2xx for a key return the same id; a different body gets 409). Each key is POSTed 1–3 times, partly
  concurrently. Runs A: PASS.
- **M4** (SELECT-then-INSERT, unique constraint dropped): `test_idempotent_create` fails with
  50 × created. Chaos `20261112-104722-s1-M4`: I2 FAIL + I1 FAIL on 4152 keys, I3–I8 PASS:
  14,915 rows for 10,000 keys. Every duplicate came from copies the workload sends concurrently
  (the client log stamps answers, and the duplicate 201s were answered ≤ 5 ms apart). No copy
  answered later than 50 ms after its key's first one got a 201. So the bug shows only in the
  check-then-insert window. 45 keys sent with two different bodies got two 2xx instead of a 409.
  The workers ran 14,187 distinct jobs, vs 9,392 in A seed 1: about 4,800 extra executions (the
  rest of the extra rows were cancelled while queued), and wall time 508 s vs ~300 s. A duplicate
  wastes real compute.

## 5. Isolation level

**Risk.** READ COMMITTED (Postgres' default) lets two transactions read the same row and both act
on what they read. Code that loads, checks in Python and then saves races under it, and nothing
errors.

**Mechanism.** There are no read-then-write paths. Every state change is one conditional `UPDATE`
that names the expected source state (and the fencing token), and Postgres re-checks that `WHERE`
after any lock wait. A stale decision therefore updates 0 rows instead of overwriting. A higher
isolation level would add `40001` serialization failures to retry without making these statements
any safer. That is argued, not tested: nothing runs under REPEATABLE READ or SERIALIZABLE
(SPEC §8.5). `SHOW transaction_isolation` is recorded in every chaos header.

**Evidence.**
- `test_naive_pattern_breaks_under_rc`: two sessions under READ COMMITTED each read the oldest
  queued job and update it by id. Both "claim" it (`attempt = 2`, `running → running` in the audit
  log). Under the same overlap, `repo.claim` gives the job to exactly one of them.
- Every chaos header records `isolation: read committed`, and the five unmutated runs (A × 3, Z × 2)
  pass I8. **M1** is exactly this naive pattern, and I8 catches it on 525 jobs.

## 6. Clock skew

**Risk.** Worker clocks drift or jump (NTP step, suspended VM). If a lease is computed from a
worker's clock, a fast clock steals live jobs and a slow clock keeps dead jobs leased.

**Mechanism.** Every timestamp used for correctness comes from the database: the lease is
`now() + interval` and expiry is `lease_expires_at < now()`, both evaluated by Postgres. Workers
use `time.monotonic()` only to *schedule* heartbeats and reaps, never as a SQL parameter. A worker
whose clock runs slow just heartbeats late, which is the zombie case (§2), and the fence covers it.
Server-side clock skew (DB failover to a host with a different clock) is not covered (SPEC §8.5).

**Evidence.**
- `test_worker.py::test_no_wall_clock_in_app`: there is no `time.time(`, `datetime.now/utcnow/today(`,
  `date.today(` or `from time import` under `app/`. Proven by adding one by hand: the test fails.
- No test or chaos run injects skew: worker and DB share one host clock in every test, so a
  wall-clock lease would pass them too. The guard above is textual, and the rest is argued here,
  not measured. A cheap real test would jump the worker's wall clock by ±1 h during `run_job` and
  assert the job is neither reaped nor extended. It isn't written.
