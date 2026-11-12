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

## Decision

*Filled in from `results/load/` after the runs: decision → top 3 evidence → first fix → what
breaks first at 10×.*
