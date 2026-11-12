# Training-job queue: will it survive production?

FastAPI + psycopg 3 + PostgreSQL 16, Python 3.12. API and workers share nothing and coordinate only
through Postgres. Every state change is one conditional SQL statement that names the expected source
state and the worker's fencing token (`attempt`). Lease times come only from the database clock.

## The answer

- **SHIP at 100 rps** (`Docs/VERDICT.md`). The verdict run `20261112-120707-short-s1` sent 29,998
  requests with 0 errors while `kill -9` hit a worker 74 times. p99 per endpoint was 17.5–19.0 ms,
  against a 200 ms bound, and `queued` never went above 6. The criteria were committed in fd8ea5f,
  before the load test existed.
- **The chaos harness passes on correct code and fails on broken code.** That is 9 full runs
  (10,000 keys, 8 workers, `kill -9` every ~2 s, duplicate POSTs, cancels), one try each, no
  reruns, all on commit b1d4a56 (code hash `30c76023c67a`):
  - run A × 3 and run Z (zombies) × 2: PASS;
  - each of the deliberate bugs M1–M4 FAILs exactly the invariant named for it.
- **Guarantee:** at-least-once execution, exactly-once committed effect, because the effect row
  commits in the same statement as `running → succeeded`. A job can run twice, but it can never
  record two effects.

## Quick start

```sh
cp .env.example .env
docker compose up -d --build     # db + api on :8000 + 4 workers, in the background
```

```sh
curl -s -X POST localhost:8000/jobs -H 'Idempotency-Key: k1' -H 'Content-Type: application/json' \
     -d '{"duration_ms": 500, "fail": false}'
curl -s 'localhost:8000/jobs?limit=5'
curl -s -X POST localhost:8000/jobs/<id>/cancel
```

API (SPEC §4): `POST /jobs` (201 new, 200 replay, 409 same key with a different body),
`GET /jobs/{id}`, `GET /jobs?limit=&cursor=&status=` (keyset pagination), `POST /jobs/{id}/cancel`
(200, or 409 if the job already finished). Bad input gets a 400, a database timeout a 503 with
`Retry-After`.

## Results at a glance

Chaos matrix: the full rows are in `results/chaos/INDEX.md`, the raw output of each run in
`results/chaos/<run>/`.

| Run | id | Verdict | Failed invariant | Kills (busy) | Re-exec | Wall s |
|---|---|---|---|---|---|---|
| A seed 1 | `20261112-101436-s1` | PASS | – | 147 (136) | 124 | 302 |
| A seed 2 | `20261112-101954-s2` | PASS | – | 146 (134) | 110 | 302 |
| A seed 3 | `20261112-102520-s3` | PASS | – | 143 (131) | 117 | 293 |
| Z seed 1 (32 freezes, 28 zombie chances) | `20261112-105615-s1-z` | PASS | – | 162 (154) | 163 | 337 |
| Z seed 2 (32 freezes, 30 zombie chances) | `20261112-110215-s2-z` | PASS | – | 156 (141) | 157 | 330 |
| M1: claim without lock or status check | `20261112-103031-s1-M1` | FAIL | I8 (525) | 151 (129) | 667 | 315 |
| M2 + `--zombie`: no `attempt` fence | `20261112-103608-s1-z-M2` | FAIL | I6 (26) | 159 (149) | 159 | 328 |
| M3: complete without `status='running'` | `20261112-104155-s1-M3` | FAIL | I7 (27), I8 (27) | 149 (140) | 129 | 307 |
| M4: SELECT-then-INSERT, no unique key | `20261112-104722-s1-M4` | FAIL | I1 (4152), I2 (4152) | 245 (233) | 227 | 508 |

Over the 5 green runs, `kill -9` hit a worker 754 times, 696 of them while it held a job, with
671 re-executions and 0 jobs that ran out of attempts. Every invariant other than the named one
passed in each mutation run.

In all there are 19 chaos runs and 3 load runs. Besides the 9 matrix runs and the fresh-clone run
below, `results/chaos/INDEX.md` keeps the 9 other rows, none of them hidden:
- the Phase 7 debug runs, including an M1 smoke test at 1,000 jobs;
- the first full run A, made before the review fixes;
- two deliberate `docker stop` crash tests (ERROR rows);
- a zombie debug run and a gzip check.

**Fresh-clone test** (`20261112-130632-s1-j1000`, PASS). The repository at 51d90d4 was cloned into a
new folder, then: `cp .env.example .env`, `docker compose up -d --build --wait` (healthz 200, 4
workers connected, one job submitted with curl succeeded), and a 1,000-job chaos run with 15 kills,
all on busy workers, and 11 re-executions. Its code hash differs from the matrix's, although the
code is identical (see "The code hash depends on line endings" below).

Load (`results/load/INDEX.md`, criteria and decision in `Docs/VERDICT.md`):

| Run | id | Verdict | Requests / errors | p99 ms create / get / list / cancel | Max queued | Drain | Kills (busy) |
|---|---|---|---|---|---|---|---|
| L1 (verdict run): 20–200 ms jobs, 4 workers | `20261112-120707-short-s1` | PASS | 29,998 / 0 | 19.0 / 17.5 / 18.8 / 17.7 | 6 | 0.5 s | 74 (51) |
| L2 (capacity probe): chaos job mix | `20261112-121319-chaos-s1` | FAIL C4 (expected) | 29,997 / 0 | 17.7 / 17.1 / 19.6 / 16.9 | 1,628 (C4's value; the INDEX column, which stops at 300 s, says 1,621) | 122.7 s | 74 (74) |

L2 is not a SHIP gate. It shows that 4 workers finish 12.3 of the 22.3 jobs/s that arrive with that
mix, while the API stays unaffected. I1–I8 pass in both runs. A 60 s debug run
(`20261112-120519-short-s1-d60`) is also listed.

## Where things are

| Path | What |
|---|---|
| `Docs/DESIGN.md` | Design note: why Postgres and not SQLite, the claim, dead-worker detection, cancel while running, the guarantee. Committed in 3caf1a2 (2026-11-11), before any code. The first code commit, fc3812f, corrected two points in it (the effect records the worker's own `attempt`; the trigger logs `running → running`). About 2 pages (1,082 words), so over the 1-page target |
| `Docs/SILENT_FAILURES.md` | The six silent failures. For each: the mechanism, the test, the chaos run and the mutation that proves the evidence can fail |
| `Docs/VERDICT.md` | The 100 rps criteria (written before the runs), the decision, what breaks at 10×, what surprised me and what still concerns me |
| `Docs/SPEC.md` | The full spec. Code comments cite its section numbers |
| `AI_USAGE.md` | Where AI was used, what was accepted or rejected, and how each point was checked |
| `app/` | `repo.py` (all SQL), `api.py`, `worker.py` (loop, heartbeat, reaper), `db.py`, `config.py` |
| `migrations/001_init.sql` | `jobs`, `job_effects` (deliberately no `UNIQUE(job_id)`), `job_transitions` audit trigger |
| `chaos/` | `harness.py` (actors, report), `invariants.py` (I1–I8) |
| `loadtest/load.py` | Open-loop 100 rps generator with its own `kill -9`ed workers |
| `tests/` | 155 tests against real Postgres (`test_races.py`: forced lock-wait interleavings plus 50-thread claim and create races; `test_worker.py`, `test_invariants.py`, …) |

## Reproduce

All of it runs in Linux containers. `kill -9` and `SIGSTOP` need Linux, and the DB host only exists
inside compose.

**Tests** (~20 s, 155 tests, in a separate `<db>_test` database):

```sh
docker compose --profile test run --rm tests
docker compose --profile test run --rm tests pytest tests/test_races.py
```

**Chaos.** The harness starts its own API and 8 workers inside the chaos container, and it refuses
to start while compose workers are connected:

```sh
docker compose stop workers
docker compose --profile chaos run --rm chaos --seed 1 --jobs 1000   # smoke, ~40 s
docker compose --profile chaos run --rm chaos --seed 1               # run A, ~5 min
docker compose --profile chaos run --rm chaos --seed 1 --zombie      # run Z: SIGSTOP a busy worker past its lease
```

Each run writes `results/chaos/<run>/` (`report.txt`, `report.json`, `harness.log`, `api.log`,
`client.jsonl.gz`, `workers/*.log.gz`) and the harness appends its row to
`results/chaos/INDEX.md`. Read the gzipped logs with `zcat` / `zgrep`. A coverage gap, such as no
kill hitting a busy worker or `--zombie` with no chance to fail, makes the verdict INCOMPLETE, never
PASS.

**Mutations** (SPEC §8.4). Each one puts back the bug a mechanism prevents, and the named test and
invariant must then fail:

```sh
docker compose --profile test run --rm -e MUTATION=M1 tests pytest tests/test_races.py
docker compose --profile chaos run --rm -e MUTATION=M2 chaos --seed 1 --zombie
```

| Mutation | Test that fails | Invariant that fails |
|---|---|---|
| M1 | `test_concurrent_claims` | I8 |
| M2 (chaos needs `--zombie`) | `test_zombie_cannot_complete` | I6 |
| M3 | `test_cancel_complete_race_cancel_first` | I7 + I8 |
| M4 | `test_idempotent_create` | I2 + I1 |

M4's preflight drops the unique constraint, and the next run without `MUTATION` restores it.

**Load.** The load container doesn't mount `app/`, so it runs the image's code against the compose
`api`. It spawns and `kill -9`s its own 4 workers:

```sh
docker compose up -d --build db api
docker compose stop workers
docker inspect $(docker compose ps -q api) --format '{{.Config.Image}} {{.Image}} {{.State.StartedAt}}'
docker image inspect soup-queue --format '{{.Id}}'
docker compose exec api printenv MUTATION
docker compose --profile load run --rm load --mix short --seed 1   # L1, ~6 min
docker compose --profile load run --rm load --mix chaos --seed 1   # L2, ~7 min
```

The run's code hash is computed in the load container, not in `api`. So before trusting a run,
check that the api container runs the `soup-queue` image you just built (the two ids match), that
it started before the run, and that `MUTATION` prints an empty line. Output goes to
`results/load/<run>/`, plus a row in `results/load/INDEX.md`.

## What the harness does NOT catch

From SPEC §8.5:
- Database crash or failover. There is one Postgres, and failover is not tested.
- Network partitions and half-open TCP. `kill -9` closes sockets cleanly, and keepalives are left
  at their defaults.
- Clock skew on the database server. Every lease time comes from that one clock.
- External side effects. The exactly-once effect holds only because the effect row lives in the
  same database. An S3 write or a webhook would be at-least-once without dedup on
  `(job_id, attempt)` or an outbox.
- Long-run bloat and pool exhaustion. A run lasts ~5 minutes.
- Narrow timing windows that random kills rarely hit. `tests/test_races.py` covers them instead:
  it forces the lock-wait interleavings (cancel against complete, the zombie sequence), and the
  claim and create races run 50 threads on a barrier.
- Isolation levels other than READ COMMITTED. Argued in `Docs/SILENT_FAILURES.md` §5, not run.
- Bugs in the harness itself. Mitigated by M1–M4, which fail as predicted.

Found in the runs and reviews:
- **Run Z never reached the complete fence.** In the two Z runs (`20261112-105615-s1-z`,
  `20261112-110215-s2-z`), every one of the 64 zombies stopped at its first write after `SIGCONT`,
  which was a heartbeat. 58 were rejected by the fence because a newer attempt was running, and 6
  because the new owner had already finished. 0 reached complete. The reason is that `run_job`
  heartbeats right after the sleep chunk the freeze interrupted. So a bug that removed only
  complete's `attempt` check would very probably pass run Z. Only a freeze that landed in the
  heartbeat or reap round trip, or between the last chunk and complete, would reach it, and 0 of 64
  did. That bug is caught by `test_zombie_cannot_complete` and
  `TestComplete::test_stale_attempt_writes_nothing_and_new_owner_completes`.
- **In the M1 run, effect counting didn't see the double claims.** The two claimants got `attempt`
  1 and 2, because the second UPDATE re-reads the locked row. Both ran, and the fence rejected the
  older complete, so each job had exactly one effect and I4–I6 passed. Only the audit trail (I8)
  caught it: 525 jobs, and 524 `complete_rejected` against 18–28 in the A runs
  (`20261112-103031-s1-M1`).
- **Clock skew is argued, not injected.** Leases use the database's `now()`, and
  `test_no_wall_clock_in_app` greps `app/` for wall-clock calls. The grep misses some forms, such
  as `time_ns` and `fromtimestamp`.
- **Load provenance is partly manual.** The load run's code hash covers `app/`, `chaos/` and
  `migrations/` in the load container. It covers neither the long-lived `api` container nor
  `loadtest/load.py`. For L1 and L2 the api was checked with `docker inspect` (image
  `sha256:8144206a…`, `MUTATION` empty). `load.py` was committed unchanged in 2d0861c (sha256
  `4021413d…`, written before the first run; see the L1 notes in `results/load/INDEX.md`). An api
  left running with M4 would very probably pass L1 without any warning.
- **The code hash depends on line endings.** Found by the fresh-clone test. `code_hash()` hashes
  raw bytes. The matrix's working tree has a mix of LF and CRLF files, while a Windows clone with
  `core.autocrlf=true` gets CRLF everywhere. So the clone reported `c5f8814a2d31`, not
  `30c76023c67a`, for code that is identical ignoring CR. The committed blobs are LF, so a clone
  without autocrlf (Linux, macOS) gets `612a11cf4565`. That is also the hash of both trees, and of
  b1d4a56's files, with line endings normalised to LF. So `30c76023c67a` can be reproduced only in
  the original working tree, and only until git rewrites one of its two LF files. The portable
  reference is the commit (b1d4a56 for the matrix) or the LF hash.

## Would do next

1. **Make the backlog visible** (VERDICT's first fix). In L2 the queue grew by 5.4 jobs/s while
   every new job still got a 201, with a create p50 of 4.7 ms. Export the queued count and the age of the oldest queued job,
   alert on the age, and scale workers on it.
2. **Run the 10× experiment** instead of arguing it from arithmetic: several uvicorn processes, a
   connection pooler, and dead tuples measured at the head of `ix_jobs_queued`.
3. **Make updates on `jobs` HOT-capable.** Today 1 update in 260,736 is HOT, because `status` sits
   in partial-index predicates and `lease_expires_at` is indexed. Options: a narrow leases table,
   `fillfactor`, autovacuum tuning.
4. **A zombie actor that freezes a worker between its last chunk and complete**, so that run Z
   exercises the complete fence too.
5. **Put the api's code hash in `/healthz`** so the load preflight checks it instead of a person.
   Hash the LF-normalised content (or the git blob ids) so a clone reproduces it, and add a
   `.gitattributes` with `eol=lf`.
6. **Profile the 14–19 ms latency tail.** About 2.5 % of requests in L1 fall there. Thread
   hand-offs under the GIL in the single uvicorn process are a hypothesis, not verified.
7. Failover, half-open TCP and real clock-skew tests, and `LISTEN/NOTIFY` instead of polling.
