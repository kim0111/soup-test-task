# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Training-job queue (Soup backend take-home): FastAPI + psycopg 3 + PostgreSQL 16, Python 3.12, deps via uv.
The goal is to prove it **survives production** (crashes, zombies, races), not just happy paths.

## Source of truth

- `Docs/SPEC.md`: authoritative design. Section numbers (§3 states, §5 worker SQL, §8 chaos harness and
  invariants I1–I8, §8.4 mutations M1–M4, §9 race tests) are referenced from code comments and docstrings.
- `Docs/DESIGN.md`: the 1-page design note (a deliverable).
- `Docs/PLAN.md` (build order, phase checklist) and `Docs/NOTES.md` (journal used later for `AI_USAGE.md`) are
  **gitignored, local only**. Check PLAN.md for the current phase; record surprises, rejected/accepted AI
  suggestions and failed runs in NOTES.md.
- Not built yet (per SPEC §11): `chaos/`, `loadtest/`, `results/`, `tests/test_api.py`, `tests/test_races.py`,
  the worker loop, the API endpoints, and the `MUTATION` branches in `repo.py`.

## Commands

Everything runs in Linux containers (the host is Windows; SIGKILL/SIGSTOP and the DB host only exist in compose).
Requires `.env` (copy `.env.example`); compose refuses to start without it.

```sh
docker compose up --build                                   # db + api (:8000) + 4 workers
docker compose --profile test run --rm tests                # full test suite
docker compose --profile test run --rm tests pytest tests/test_repo.py::TestClaim::test_empty_queue_returns_none
MUTATION=M1 docker compose --profile test run --rm tests    # run tests against a deliberate bug
docker compose stop workers && docker compose --profile chaos run --rm chaos --seed 1 [--zombie]
docker compose --profile load run --rm load
```

- The `tests` service bind-mounts `app/`, `tests/`, `migrations/`: code changes need no rebuild; dependency
  changes (`pyproject.toml`/`uv.lock`) do (`docker compose build`). The Docker build uses `uv sync --locked`,
  so run `uv lock` after editing dependencies.
- The DB runs on tmpfs and loads `migrations/*.sql` at first start: schema changes need the db container recreated.
- Tests use a separate `<POSTGRES_DB>_test` database dropped and rebuilt from `migrations/` once per session
  (`tests/conftest.py`); the `conn` fixture truncates all tables per test.
- `MUTATION` must be empty or `M1`..`M4`; anything else raises at import (`app/config.py`).

## Architecture

API and N worker processes share nothing and coordinate only through Postgres. The core rules (all
correctness depends on them; don't break them when adding code):

- **All SQL lives in `app/repo.py`.** No ORM, no read-then-write in Python. Each state change is **one
  conditional statement** naming the expected source status and, for worker writes, the fencing token
  `attempt`; an illegal/stale transition updates 0 rows and the caller gets `False`/an outcome literal.
- **Connections are autocommit** (`app/db.py`, per-role `statement_timeout`/`lock_timeout`): no transaction is
  ever held across a round trip, so a frozen or killed process holds no locks. Postgres also runs with
  `idle_in_transaction_session_timeout=2s`.
- **Only the DB clock (`now()`) is used for correctness.** Worker clocks may only schedule heartbeats
  (monotonic); no `time.time()`/`datetime.now()` in lease logic or as SQL params.
- **Claim** = `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED LIMIT 1)`, incrementing `attempt`.
  **Heartbeat** runs in the same loop as the work (not a thread). **Complete** writes the `job_effects` row
  and `running → succeeded` in one CTE statement, recording the *worker's* `attempt`, not the row's.
- **Reaper** (every worker, every second) requeues expired leases, or fails the job after `MAX_ATTEMPTS`.
  Cancel of a running job is immediate; the worker discovers it via a 0-row heartbeat/complete.
- Terminal states (`succeeded`/`failed`/`cancelled`) are never named as a source state in any statement.

Schema details that are deliberate, not oversights (`migrations/001_init.sql`):
- `job_effects` has **no** `UNIQUE(job_id)`: a fencing bug must show as a countable duplicate (invariants I4–I6).
- The `job_transitions` trigger fires on every `UPDATE OF status` **without** a `WHEN old<>new` filter, so a
  double claim is logged as `running → running` (I8 / M1). Updates that don't set `status` (heartbeat,
  test helper `expire_lease`) log nothing.
- `CHECK ((status = 'running') = (lease_expires_at IS NOT NULL))`: every transition out of `running` must
  null the lease.

## Testing conventions

- Always against real Postgres, never mocks or SQLite (SKIP LOCKED, lock-wait re-checks, ON CONFLICT).
- `tests/helpers.py` drives states directly (`expire_lease`, `reclaimed`, `effects`, `transitions`) and is
  shared by `test_repo.py` and the future race tests.
- Mutations (SPEC §8.4) are implemented as one `if config.MUTATION == "Mx"` per query in `repo.py`; each must
  make its named test and chaos invariant fail. A mutation that isn't caught is a harness finding, reported as is.
- Never edit chaos/load results; every run, including failed ones, goes into `results/chaos/INDEX.md`.
