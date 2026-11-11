# Design Note: Training-Job Queue

**Stack:** Python 3.12 · FastAPI · psycopg 3 · PostgreSQL 16
**Guarantee:** at-least-once *execution*, exactly-once *committed effect*

```
 client ──▶ API ──┐
                  ▼
             Postgres   ← single source of truth, single clock
                  ▲
   worker 1 … worker N   (separate OS processes; each also runs the reaper)
```

Workers share nothing and coordinate only through Postgres. Every state change is **one
conditional SQL statement** that names the expected source state and the caller's fencing token, so
a stale or illegal transition updates zero rows instead of corrupting state. All times used for
correctness come from the database's `now()`.

## Storage: Postgres, not SQLite

The design rests on three Postgres features: `FOR UPDATE SKIP LOCKED` (workers claim without
blocking each other), row locks with a re-check of `WHERE` after a lock wait (serialises cancel vs
complete), and `INSERT … ON CONFLICT` (idempotent create). SQLite has a single writer and no
`SKIP LOCKED`: 8 workers would queue on one file lock. No Redis/Celery either: a queue outside the
database is a dual write with no shared transaction, and 100 rps is easy for a Postgres queue.
Raw SQL, no ORM: load → check in Python → save is exactly the read-modify-write race to avoid.

## Data model

`jobs(id, idempotency_key UNIQUE, request_hash, payload, status, attempt, lease_expires_at, …)`.
`attempt` is incremented on every claim and is the **fencing token**.
`job_effects(job_id, attempt, worker_id)` is the emulated side effect, deliberately without
`UNIQUE(job_id)`, so a double effect is countable rather than hidden by a constraint.
`job_transitions` is an audit log written by a trigger in the same transaction as each status change.
It also logs writes that leave the status unchanged, so a double claim appears as `running → running`.

States: `queued → running → succeeded / failed / cancelled`; `running → queued` only by the reaper
after lease expiry. Terminal states are never a source state in any statement.

## How a worker claims a job

```sql
UPDATE jobs SET status='running', attempt=attempt+1, worker_id=$w,
       lease_expires_at = now() + interval '5 s'
 WHERE id = (SELECT id FROM jobs WHERE status='queued'
              ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING id, payload, attempt;
```

One atomic statement: two workers never receive the same row, and none waits on another. The
returned `attempt` must accompany every later write. Order is approximately FIFO.

## How a dead worker is detected

By **lease**, passively. The worker executes in ~100 ms chunks and between them extends its lease
every second (`UPDATE … WHERE id=$id AND status='running' AND attempt=$a`), in the same loop as the
work, so a hung job stops heartbeating. A worker killed with `kill -9` closes its connection,
Postgres rolls back anything open, and the job stays `running` until the lease (5 s) passes. A reaper
in every worker then, once a second, moves expired jobs back to `queued`, or to `failed` after 5
attempts (poison job). Worst-case recovery ≈ 6 s.

A worker that froze and came back (zombie) is treated as dead: its heartbeat or completion matches
zero rows because `attempt` has moved on or the status changed, so it abandons the job and writes
nothing. A frozen worker holds no locks: every worker write is a single autocommit statement.

## What happens when a running job is cancelled

```sql
UPDATE jobs SET status='cancelled', finished_at=now(), lease_expires_at=NULL
 WHERE id=$1 AND status IN ('queued','running');
```

The job is `cancelled` immediately and the API answers 200; an already terminal job gets 409. The
worker learns at its next heartbeat (≤ 1 s) or when its completion matches zero rows, and stops
without writing. Cancel and complete lock the same row, so whichever commits first wins and the
other's `WHERE` fails: the result is either *succeeded with one effect* or *cancelled with none*.
Cost: up to ~1 s of wasted work after the cancel.

## Guarantee: at-least-once execution, exactly-once effect

Completion writes the effect and the state change **in one statement**:

```sql
WITH done AS (
  UPDATE jobs SET status='succeeded', finished_at=now(), lease_expires_at=NULL
   WHERE id=$id AND status='running' AND attempt=$a
  RETURNING id, attempt)
INSERT INTO job_effects (job_id, attempt, worker_id) SELECT id, $a, $w FROM done;
```

The effect stores the worker's own token `$a`, so a broken fence shows up as an effect whose attempt
differs from the job's, instead of silently borrowing the new owner's token.

- **Execution can repeat.** A worker can be killed after doing the work and before committing; the
  job then runs again. Exactly-once *execution* is impossible with crash-prone workers: nothing can
  atomically "do the work" and "record that it did".
- **The effect cannot repeat.** It commits atomically with `running → succeeded`, fenced by
  `attempt` and `status`, so only one attempt can ever record it and never after a cancel:
  exactly one effect per succeeded job, zero otherwise.
- **Caveat:** this holds because the effect lives in the same database. An external effect (S3, a
  webhook, a payment) degrades to at-least-once unless the receiver dedupes on `(job_id, attempt)`
  or the effect goes through an outbox.

**Idempotent create:** `INSERT … ON CONFLICT (idempotency_key) DO NOTHING`; on conflict, same body
hash → 200 with the existing job, different body → 409. The unique index arbitrates concurrent retries.

**READ COMMITTED** is enough: no decision is made from an earlier read, and Postgres re-checks each
`UPDATE`'s `WHERE` against the newest row version after a lock wait. **Clock skew** on workers
cannot move leases: the worker clock only schedules the next heartbeat.

## Failure scenarios

| Scenario | Outcome | Protected by |
|---|---|---|
| Two workers claim at once | Different rows | `SKIP LOCKED` |
| `kill -9` mid-run or mid-commit | Rolled back, requeued after lease, runs again | lease + reaper + atomic complete |
| Zombie worker resumes | Its writes match 0 rows | fencing `attempt` |
| Cancel vs complete | One wins | row lock + `status` predicate |
| Client retries `POST` | Same job | `UNIQUE(idempotency_key)` |
| Job crashes every worker | `failed` after 5 attempts | max attempts in reaper |

## Known weaknesses

- **Polling** by idle workers grows with the fleet; `LISTEN/NOTIFY` would help.
- **Heartbeat churn:** one `UPDATE` per running job per second on the hot table → dead tuples, vacuum load.
- **Single database:** single point of failure and the throughput ceiling; failover not covered.
- **Lease length (5 s) is a guess:** too short → needless re-runs, too long → slow recovery.
- **Exactly-once effect is local** to Postgres (see the caveat above).
