# Verdict at 100 rps

**I wrote these criteria on 2026-11-12, before any load run.** The commit that adds this file is
older than every run in `results/load/`, and the criteria are not changed after the numbers exist
(`git log -p Docs/VERDICT.md` shows this). The decision at the end is filled in from the raw
results, in SPEC §10's format.

## Setup (fixed before running)

- **Generator:** `loadtest/load.py` in the `load` compose container sends 100 requests/s for 5 min.
  It is open-loop: request *i* goes out at `t0 + i/100` s no matter what earlier answers do. Latency
  is measured from that scheduled time to the answer, so a stalled API can't slow the generator down
  and hide its own delay (no coordinated omission). Client timeout is 2 s. The generator never
  retries on its own.
- **Request mix** (one seeded RNG):
  - 60 % `GET /jobs/{id}` for a job already created in this run
  - 25 % `POST /jobs`; ~10 % of these repeat an earlier key with the same body (a client retry)
  - 10 % `GET /jobs?limit=50`, half of them with `status=queued`
  - 5 % `POST /jobs/{id}/cancel` for a job already created in this run

  That is ≈ 25 POST/s, or ≈ 22.5 new jobs/s.
- **System:** the compose `db` and `api` (one uvicorn process, pool of 20 connections). The compose
  `workers` are stopped. Instead the load test spawns 4 workers with the same code and every
  4 ± 1 s sends `kill -9` to a random one, which is respawned at once. That is the chaos
  per-worker rate (each worker every ~16 s), ~75 kills per run. The kills stop when the load stops.
- **Two runs.** A worker spends the job's duration + ~5 ms (claim + complete) per job.

| Run | Job bodies (3 % `fail=true` in both) | Mean duration | 4 workers can do | Role |
|---|---|---|---|---|
| L1 | 20–200 ms | 110 ms | ≈ 35 jobs/s, so ~65–70 % busy with kills | **The verdict run.** It is sized to be stable, so a growing queue here is the system's fault, not a capacity limit |
| L2 | chaos mix: 95 % 20–200 ms, 5 % 2–5 s | ≈ 280 ms | ≈ 14 jobs/s, below the 22.5 that arrive | Capacity probe, **not a SHIP gate.** Measures the real throughput ceiling, the API under a growing backlog (~+8 jobs/s by arithmetic, less what cancels remove), heartbeats, and long jobs killed mid-run |

L1 can't show heartbeats (no job reaches the 1 s heartbeat) or long jobs killed mid-run. The chaos
runs A and Z and run L2 cover both.

## SHIP criteria (run L1)

**SHIP only if every row holds.** One failing row means DON'T SHIP.

| # | Criterion | How it is measured | Threshold |
|---|---|---|---|
| C1 | Chaos matrix green, same code | `results/chaos/INDEX.md`, and the code hash (sha256 of `app/`, `chaos/`, `migrations/`) in each load run's header | A × 3 PASS, Z × 2 PASS, M1–M4 each FAIL their named invariant (already true in Phase 8), **and** the load runs report code hash `30c76023c67a`, the matrix's |
| C2 | Error rate | Errors / requests scheduled, over the whole run including the warm-up. **Error:** any 5xx (503 with `Retry-After` included), no answer within 2 s, a connection error, a 404 (we only ask for ids we created), any other unexpected status. **Not an error:** 201 or 200 (replay) on create, 200 on get and list, 200 or 409 (already finished) on cancel | < 0.1 % |
| C3 | p99 latency per endpoint | Scheduled send time → answer, for requests scheduled at t ≥ 30 s. Failed requests stay in: a timeout counts as slower than any answer | create, get, list, cancel: **each** p99 < 200 ms |
| C4 | Queue depth bounded | `SELECT count(*) FROM jobs WHERE status = 'queued'`, sampled every 1 s | Every sample at t ≥ 30 s ≤ 100 (~4 s of arrivals), **and** after the load stops, `queued + running = 0` within 60 s |
| C5 | Invariants after the drain | I1–I8 (SPEC §8.2), the chaos harness's own `chaos/invariants.py`, against the DB and the run's client log | All PASS. I2's different-body clause is vacuous here: the load mix sends no different bodies |

**Validity** is a precondition, like the chaos harness's INCOMPLETE. A run that misses any of these
is INVALID: its row is kept and the run is repeated. It is never read as a PASS.
- ≥ 99 % of requests are sent within 50 ms of their scheduled time. Otherwise the numbers measure
  the generator, which shares the machine's CPU with the API, DB and workers.
- At least one kill hit a busy worker, and at least one job was re-executed.

**Reported without a threshold** (SPEC gives none): p50 per endpoint, jobs finished per second,
dead tuples on `jobs` (`n_dead_tup`, `n_live_tup`, autovacuum count, every 10 s), kills (busy),
re-executions, exhausted attempts, 503 count, send lag.

## Run L2 (capacity probe)

Same measurements as L1. It drains until the queue is empty (≤ 10 min), then checks the invariants.
L2 is not a SHIP gate, with one exception: **an invariant FAIL in L2 is a correctness bug and
means DON'T SHIP.** Expected before running: ~13–14 jobs finished/s, and `queued` growing at
~+8/s less the cancels that hit queued jobs.

## Decision (from the runs, 2026-11-12)

Raw output is in `results/load/<run>/` and the rows are in `results/load/INDEX.md`:
- **L1:** `20261112-120707-short-s1`
- **L2:** `20261112-121319-chaos-s1`
- The 60 s debug run `20261112-120519-short-s1-d60` doesn't count.

Both runs used code hash `30c76023c67a`. C1's hash is computed in the load container, so for the
`api` container I checked separately with `docker inspect`: it ran the same image
(`sha256:8144206a…`), with `MUTATION` empty, and was started before the runs.

| # | Threshold | L1 (verdict run) | L2 (capacity probe) |
|---|---|---|---|
| C1 | matrix green, code `30c76023c67a` | PASS | PASS |
| C2 | errors < 0.1 % | **0** / 29,998 | 0 / 29,997 |
| C3 | p99 < 200 ms per endpoint | create **19.0**, get **17.5**, list **18.8**, cancel **17.7** ms | 17.7 / 17.1 / 19.6 / 16.9 ms |
| C4 | queued ≤ 100 after 30 s, drain ≤ 60 s | max **6**, drain **0.5 s** | max 1,628, drain 122.7 s: FAIL, as predicted |
| C5 | I1–I8 PASS | **PASS** | PASS |
| V1–V3 | on time ≥ 99 %, a busy kill, a re-execution | 99.99 %, 51, 41 | 99.99 %, 74, 74 |

### Decision: SHIP

At 100 rps with 4 workers and jobs of L1's size, every criterion holds. L2 fails only on capacity
(C4). Its invariants pass, so the L2 exception ("an invariant FAIL in L2 means DON'T SHIP") doesn't
apply. The ship comes with a sizing rule: the worker fleet has to match the job mix. L1 ran with
the workers ~65–70 % busy and was stable. With the chaos mix, 4 workers finish 12.3 jobs/s of the
22.3 that arrive, so that mix needs ≈ 9–10 workers at the same load.

### Top 3 evidence

1. **The criteria hold with a wide margin while workers are being killed.** L1 had 0 errors in
   29,998 requests (no 503, no timeout). Over the run, `kill -9` hit a worker 74 times, 51 of them
   holding a job. p99 was 17.5–19.0 ms on every endpoint, about 10× under the bound, and `queued`
   never went above 6. The 0.5 s drain was luck: the generator happened to cancel the last
   orphaned job. Structurally, a late kill drains in ≤ lease 5 s + reap 1 s.
2. **Correctness held while serving 100 rps.** Every busy kill reconciles in the DB:
   - L1: 51 = 41 re-run as attempt 2, plus 10 orphans cancelled before the reaper got to them.
   - L2: 74 = 74 re-run.

   Across both runs: 0 jobs failed by exhausted attempts, 0 worker tracebacks, I1–I8 PASS. This is
   the same code hash as the chaos matrix (A × 3 and Z × 2 PASS, M1–M4 each caught by its named
   invariant).
3. **Nothing in front of the workers degrades under a backlog.** In L2, `queued` grew at
   +5.4 jobs/s to 1,628 and then drained in 122.7 s. p99 stayed at 16.9–19.6 ms with 0 errors,
   the same as L1. The limit was the 4 workers: 12.3 jobs/s under kills, against 14.3 by
   arithmetic without them, which is where the arithmetic put it. That is a sizing number, not a
   code defect.

### First fix: make the backlog visible and act on it

In L2 the queue grew by ~5 jobs/s for 5 minutes. Meanwhile every create returned 201 in ~5 ms and
p99 never moved. Nothing reported the growth: no queue depth or queue age appears anywhere in the
API or the logs. For training jobs, which run minutes to hours, demand above the fleet's capacity
is the normal state, so a silent backlog is the first thing to fix:
- export the `queued` count and the age of the oldest queued job (`min(created_at)` comes from the
  head of the `ix_jobs_queued` partial index);
- alert on that age;
- scale workers on it;
- optionally, reject creates above a bound (503 + `Retry-After`) if callers should be told.

Not first: the 14–19 ms tail mode (~2 % of requests, harmless against 200 ms) and the non-HOT
updates (point 3 below).

### What breaks first at 10× (1,000 rps, ~225 new jobs/s)

Not run. This is arithmetic on measured inputs.

1. **The API process, already at ~5×.**
   - One uvicorn process used ~20 % of a core at 100 rps, which is ~2 ms of CPU per request
     (host `docker stats`, `results/load/docker-stats/`).
   - A single Python process tops out near one core, so it saturates around 500 rps.
   - Past that, requests queue in front of its threadpool, p99 grows without bound under an open
     loop, and then come timeouts and 503s.
   - Fix: several uvicorn processes or `api` replicas.
2. **Then Postgres connections.** `max_connections` is 100. Each `api` process keeps a pool of up
   to 20 connections, and each worker holds 1.
   - At 10× the workers alone number ~37 (short mix: 225 × 0.115 s ÷ 0.7 busy) to ~90 (chaos mix).
   - 4 `api` processes add another 80, which is past 100 in either case.
   - Fix: a pooler such as PgBouncer, or smaller pools. Every session is autocommit, which suits
     transaction pooling. The per-session timeouts, now passed as startup `options`, would have to
     move to per-role settings.
3. **Then the write path on `jobs`.** Not one UPDATE is HOT: 1 in 260,736 over the DB's lifetime.
   `status` appears in the partial indexes' predicates and `lease_expires_at` is indexed, so every
   state change writes a dead tuple and a new entry in all 5 indexes.
   - At 1×, autovacuum kept up: at most ~2,400 dead tuples, 5 autovacuum runs in 5 min.
   - At 10× that becomes ~450 dead tuples/s against default autovacuum (naptime 60 s, scale
     factor 0.2), and they pile up at the head of `ix_jobs_queued`, which is exactly where every
     claim scans.
   - Not measured. It's the next experiment, with autovacuum tuning or a narrow leases table
     (SPEC §12) as the candidate fixes.

The worker fleet itself is a sizing knob, not the wall. L2's ceiling matched the arithmetic, and
10× load needs ~10× workers, which is what runs into point 2.

## What surprised me / what still concerns me

**Surprised.** Mostly by what the evidence can and can't see. Under M1 the fence hid the double
claim. The second claimant re-read the locked row and got attempt 2, both ran, the older complete
was rejected, and only the audit trail showed it (I8, 525 jobs, `20261112-103031-s1-M1`). Run Z,
built to test the zombie fence, never reached complete. All 64 zombies stopped at their first
heartbeat after `SIGCONT`: 58 were rejected by the fence, 6 found the job already done
(`20261112-105615-s1-z`, `20261112-110215-s2-z`). So the complete fence rests on two SQL-level
tests (`test_zombie_cannot_complete`, `TestComplete::test_stale_attempt_…`). Dropping only `status='running'` (M3) is enough for a zombie to
complete a requeued job, because `attempt` hasn't moved yet. Kills landed between a commit and its
log line, on both sides (`20261112-101436-s1`, `20261112-101954-s2`): the DB is the truth, not the
log. And a duplicate is paid compute. M4 ran ~4,800 extra jobs (`20261112-104722-s1-M4`), which
for training jobs means GPU hours.

**Still concerns me.** The backlog is silent. In L2 the queue grew by 5.4 jobs/s to 1,628 while
every new job still got a 201, with a create p50 of 4.7 ms (`20261112-121319-chaos-s1`). About 2.5 % of L1's requests fall in
a 14–19 ms latency mode. GIL hand-offs are only my hypothesis, not verified. Provenance is partly
manual: the code hash doesn't cover the api container, so I checked its image and `MUTATION` with
`docker inspect`. And the hash depends on line endings, which the fresh-clone run
`20261112-130632-s1-j1000` showed.
And the failure modes I never tested: DB failover, half-open TCP, injected clock skew, and effects
outside Postgres, where exactly-once becomes at-least-once.
