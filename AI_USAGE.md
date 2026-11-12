# AI usage

TASK.pdf asks where AI was used, which suggestions I accepted, and what I verified, changed or
redid myself. This file is written from my working journal (`Docs/NOTES.md`, kept local and not
committed), where every phase recorded the AI's suggestions, the rulings and how each was checked.

## Tools

- **Claude Code** with **Claude Opus 5.5**. It wrote the code and tests, ran the chaos and load
  runs, reconciled the raw logs and drafted the docs.
- **Fresh-context AI reviews**, one at the end of each of Phases 4–9 (Opus subagents from Phase 5
  on; Phase 8's review covered `Docs/SILENT_FAILURES.md`). Each got the diff or the docs plus the
  raw results, without the context of the session that wrote them.
- **claude.ai chat** for drafting and reviewing the spec.
- **superpowers skills** for Claude Code, which shaped the workflow: test-driven development, plan
  execution, verification before completion, code review.
- Not AI: a peer's independent design note for the same task, compared line by line in v2 → v3.

## Who did what

| Area | AI | Me |
|---|---|---|
| Spec and design | Drafted the spec text (`Docs/SPEC.md`, `Docs/DESIGN.md`), reviewed it | Chose the design, cut the scope (v3 → v5), compared it with the peer note, accepted or rejected every review point |
| Code and tests | Wrote `app/`, `chaos/`, `loadtest/` and the 155 tests, test-first | Set the plan per phase, reviewed the diffs, asked for the hand-breaks, committed every step |
| Runs and docs | Ran the matrix and the load runs, reconciled the raw logs, drafted the INDEX notes and the docs | Decided the run matrix, the SHIP criteria (committed before any load code) and the verdict, and reviewed the text |

## How the work was checked

1. **Design before code.** `Docs/DESIGN.md` was committed in 3caf1a2 before any code, and the SHIP
   criteria in fd8ea5f before the load test existed.
2. **Tests first, and the failure has to be the right one.** Each phase's tests were watched
   failing on a stub. A `NotImplementedError` failure proves little, so each guard was then broken
   by hand. The named test had to fail with an *assertion*, not an error, and the code was then
   restored. Later this became the `MUTATION=M1..M4` switches.
3. **A fresh reviewer per phase.** Before anything was fixed, the AI re-checked each Critical
   finding against the code or the raw logs, and I reviewed the result. Each code fix got a test
   that failed first, where one could be written. The crash row was checked with a deliberate
   `docker stop`, and doc fixes were re-checked against the raw logs. Minor findings were deferred
   and written down, not silently fixed.
4. **Raw results are never edited.** The harness and the load test append every run, failed ones
   included. Only the notes column is written by hand, from the raw logs.

## The spec, v1 → v5 (summarised)

| Step | Source | What changed (selection) | How it was checked |
|---|---|---|---|
| v1 → v2 | AI review, 15 issues | A three-step cancel became one statement (the reaper could requeue between the steps). Complete no longer holds a client-side `BEGIN … COMMIT` (a SIGSTOPped worker would pin the row lock). The final reap pass before the check was removed (it could hide a stuck job). "Exactly-once effect" was reworded to "at-least-once execution, exactly-once committed effect" | Each issue against Postgres semantics (lock waits re-check `WHERE` under READ COMMITTED; SKIP LOCKED skips locked rows) before it was accepted |
| v2 → v3 | Peer's design note | **Taken:** cancel of a running job is immediate (the `cancel_requested` column, the 202 and the worker's cancel path were removed), the note's structure, the arguments against Redis and an ORM. **Not taken:** reclaiming expired leases inside the claim query (no bound on stuck time), complete as `BEGIN; UPDATE; check in Python; INSERT; COMMIT` (holds the lock across a round trip), a "remove SKIP LOCKED" mutation (`FOR UPDATE` waits and stays correct, so it can't fail), invariants from the DB only (can't see a lost 201) | The rejected items are argued in SPEC §1, §2, §8.2 and §8.4. Immediate cancel is tested by `test_cancel_complete_race_*` and M3 |
| v3 → v4 | Me, cutting scope | Removed: libfaketime clock-skew runs, RR/SERIALIZABLE runs with `40001` retries, random SIGSTOP, a short-lease scenario | Each cut cost more time than the evidence it added. The clock-skew and isolation cuts are listed in the README under what the harness does not catch |
| v4 → v5 | Me, ~1,000 → ~300 lines | Removed the `job_executions` table, failpoints, the extra columns and enum, a harness-owned API, `/stats`, I9–I11. Kept the decisions (reaper, immediate cancel, one-statement complete, READ COMMITTED) | Phase 7 later put the harness-owned API back, for a reason found in Phase 6 (see below) |
| v5 | AI review, 4 fixes before any code | The effect records the worker's own `$a`, not `RETURNING attempt`. The audit trigger has no `WHEN old<>new` filter. I1 accepts a job that legitimately ran out of attempts. `.gitignore` entries | The mutation runs depend on both fixes; neither was removed to test it. M2 tripped I6 (26) because the effect records `$a`, and I8 caught M1 (525 jobs) through the `running → running` rows the unfiltered trigger writes |

## Per phase: suggestion → taken? → how checked

**Phase 2: data layer.**

| Suggestion or finding | Taken? | How checked |
|---|---|---|
| Break each guard in `repo.py` by hand (fence, status, cancel source, poison job) | Yes | All 6 caught by named tests. One hand-mutation was itself broken (it raised `ProgrammingError`) and was redone: an error is not a "caught" |
| pytest's long tracebacks print the conninfo with the DB password | Fixed | `--tb=short` |
| Concurrent stress script with zombies (not committed) | Yes, as a check | I1–I8 PASS on seeds 1–3. With the fence removed: I6 FAIL, n=18, so the checker can fail |

**Phase 3: race tests.**

| Suggestion or finding | Taken? | How checked |
|---|---|---|
| Force interleavings: one session holds an uncommitted statement and the test polls `pg_stat_activity` until the other waits on a `Lock` | Yes | Hand-breaks M1–M4 all caught as assertions. 20 repeats, no flakes |
| M3 alone lets a zombie complete a *requeued* job, because `attempt` hasn't moved between reap and re-claim | Finding | `test_zombie_cannot_complete` fails under M3 too. The fence needs both predicates |

**Phase 4: API.**

| Suggestion or finding | Taken? | How checked |
|---|---|---|
| Two "unknown id → 404" tests passed against a stub, because the *route* was missing (Starlette's 404) | Fixed | They now assert `detail == "job not found"` |
| Reviewer: the pooled connection was taken in a sync `Depends` (see the first mistake below) | Fixed | 200 concurrent GETs: 147 × 503 before, 200/200 after, on a real uvicorn |

**Phase 5: worker.**

| Suggestion or finding | Taken? | How checked |
|---|---|---|
| The cancel and lost-lease tests passed against a no-op `run_job` | Fixed | The tests now wait for a heartbeat (lease moved) before acting |
| Hand-break "a DB error escapes the loop" was not caught | Fixed | Added a test that kills the session while the worker is idle |
| Reviewer: any non-`psycopg` exception killed the worker for good (no restart policy, so one poison row could take down all 4) | Fixed | `test_loop_survives_poison_row`: RED (the thread died), then GREEN (5 claims → `failed`) |

**Phase 6: mutations.**

| Suggestion or finding | Taken? | How checked |
|---|---|---|
| RED before the switches existed | Yes | `MUTATION=M1` → 7/7 passed, so nothing catches a mutation that isn't there |
| The AI first wired the M4 schema sync into the API lifespan | Reverted | The suite caught it: the "bad input → 400 while the DB is down" test failed |
| Reviewer: M4's constraint drop outlived the run, so the next correct run would 500 on every create | Fixed | The helper restores the constraint. Its test went RED, then GREEN |
| Reviewer: the API process could run a different `MUTATION` than the harness | Deferred to Phase 7, then fixed | The harness starts its own API and workers |

**Phase 7: chaos harness.**

| Suggestion or finding | Taken? | How checked |
|---|---|---|
| Invariants first, each forced to 0 violations in turn | Yes | Every invariant has a test that then fails with an assertion |
| The first debug run's numbers didn't add up (16 busy kills, 7 re-executions) | Investigated | 9 orphans had been cancelled by the targeted canceller. Retuned (`results/chaos/INDEX.md`, run `091214`) |
| Reviewer: a vacuous PASS was possible (0 zombie chances, 0 busy kills) | Fixed | Coverage gaps now give INCOMPLETE. The first zombie debug run would have been INCOMPLETE |
| Reviewer: a crash left no INDEX row | Fixed | Deliberate `docker stop` mid-run. The first try hung, after the fix it took 1 s (the two ERROR rows) |
| An M3 "chance counter" (suggested by the Phase 6 reviewer, written into PLAN by the AI) | Rejected in a scope check after Phase 7 ("is any of this overengineering?") | It isn't in SPEC. M3 was caught anyway (I7 + I8, 27 each, run `104155-s1-M3`) |

**Phase 8: run matrix.** The AI reconciled every run's worker logs with the DB (written in the
notes column). The review of `Docs/SILENT_FAILURES.md` found 2 Critical and 7 Important points.
The AI re-checked both Critical ones against the raw logs before fixing them: the fourth mistake
below and the M2 line under "Also caught". Numbers the reviewer couldn't verify were
re-run. The M1 race test claims each job 5–42 times over three runs, which replaced a single
earlier range of "15–33×". The hand-break of `test_no_wall_clock_in_app` was redone.

**Phase 9: load test.**

| Suggestion or finding | Taken? | How checked |
|---|---|---|
| Criteria before code | Yes | Committed as fd8ea5f before `loadtest/` existed |
| Removing the warm-up filter from `summarize` failed no test | Fixed the test | 10 slow warm-up requests in 1,000 sat exactly at p99's 1 %. With 30: an assertion |
| Reviewer: latency measured from the *scheduled* time had no test | Fixed | 3 tests. Each hand-break (latency from the send, …) gives an AssertionError |
| Reviewer: the run used an uncommitted `load.py`, outside the code hash | Fixed | sha256 `4021413d…` written before the first run, then committed unchanged in 2d0861c |

## Where the AI was wrong, and what caught it

The five with the biggest effect:

1. **A deadlock in AI-written API code.** The pooled connection was taken in a sync FastAPI
   dependency. FastAPI runs the dependency and the endpoint as separate threadpool jobs, so under a
   burst the threads waiting for a connection starve the ones holding one. The tests passed,
   because TestClient doesn't reproduce the threadpool. **Caught by** the Phase 4 reviewer.
   Measured: 147 of 200 concurrent GETs got a 503, and bad input got a 503 instead of a 400 while
   the DB was down. Without the fix, Phase 9 would have measured this bug, not Postgres.
2. **A wrong ruling about provenance.** The AI wrote that a stale API image "shows as a C1 FAIL".
   It doesn't: the code hash is computed in the load container. **Caught by** the Phase 9
   reviewer. `docker inspect` confirmed that the api in L1 and L2 ran the right image with
   `MUTATION` empty, and the check is now a README step.
3. **"Double claimants share one `attempt`."** This was in the AI's draft of SILENT_FAILURES.
   **Caught by** the raw logs of the M1 run: 0 job/attempt pairs were claimed twice in 10,068 claim
   lines. The second claim's UPDATE waits for the row lock and applies to the committed version, so
   the pair gets attempt 1 and 2, and the fence then rejects the older one.
4. **"The workers ran all 4,915 extra M4 jobs."** **Caught by** the Phase 8 reviewer. They ran
   14,187 distinct jobs against 9,392 in A seed 1, so ~4,800 extra; the rest were cancelled while
   queued.
5. **The same provenance mistake, twice.** The Phase 7 runs record commit 55f353d, which has no
   `chaos/` directory: they ran an uncommitted tree. That led to the code hash. Phase 9 then ran an
   uncommitted `load.py` that the hash doesn't cover. Both were caught by reviewers, and the old
   rows are annotated rather than rewritten.

Also caught, one line each:
- "In M2 the real owner's complete then finds `succeeded`" (Phase 8 review, Critical): true for 2 of
  the 26. In the other 24, the new owner's heartbeat noticed first.
- "Duplicate copies sent ≤ 5 ms apart": the client log's `t` is the answer time, so the INDEX notes
  were corrected.
- "Run Z never reaches complete" was stated as structural. It is very probable, not structural.
- A hand-mutation raised an error instead of failing an assertion.
- A PLAN step was ticked while half of it was missing.
- Tests passed against stubs in Phases 4 and 5.
- The AI's own load test didn't catch a missing warm-up filter.
