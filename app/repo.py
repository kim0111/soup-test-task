"""All SQL (SPEC §4–5).

Every state change is ONE conditional statement that names the expected source state and, for
worker writes, the caller's fencing token (`attempt`). A stale or illegal transition matches zero
rows instead of corrupting state. Times come from the DB's now(), never from a client clock.
Connections are autocommit (app/db.py), so each statement is its own transaction.
"""

import hashlib
import json
from datetime import datetime
from typing import Literal
from uuid import UUID

from psycopg.types.json import Jsonb

from app import config

CreateOutcome = Literal["created", "replay", "conflict"]
CancelOutcome = Literal["cancelled", "already_cancelled", "terminal", "not_found"]


# --- API side ------------------------------------------------------------------------------------

def request_hash(payload: dict) -> str:
    """sha256 of the payload in canonical form. The API passes the *validated* body (defaults
    filled in), so an explicit default and an omitted one hash the same."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def create_job(conn, key: str, payload: dict) -> tuple[CreateOutcome, dict]:
    """created → 201, replay → 200 with the existing job, conflict → 409.

    The unique index arbitrates concurrent requests for one key: ON CONFLICT waits for an
    in-flight insert of the same key and then does nothing. The follow-up SELECT is a separate
    autocommit statement, so it sees the winner's committed row.
    """
    h = request_hash(payload)
    job = conn.execute(
        """
        INSERT INTO jobs (idempotency_key, request_hash, payload) VALUES (%s, %s, %s)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *
        """,
        (key, h, Jsonb(payload)),
    ).fetchone()
    if job is not None:
        return "created", job
    job = conn.execute("SELECT * FROM jobs WHERE idempotency_key = %s", (key,)).fetchone()
    return ("replay" if job["request_hash"] == h else "conflict"), job


def get_job(conn, job_id: UUID | str) -> dict | None:
    return conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()


def list_jobs(
    conn, limit: int, after: tuple[datetime, UUID] | None = None, status: str | None = None
) -> list[dict]:
    """Newest first. Keyset pagination: `after` is the (created_at, id) of the previous page's
    last row, so inserts during paging can't shift rows between pages the way OFFSET does."""
    where, params = [], []
    if after is not None:
        where.append("(created_at, id) < (%s, %s)")
        params += after
    if status is not None:
        where.append("status = %s")
        params.append(status)
    query = "SELECT * FROM jobs"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY created_at DESC, id DESC LIMIT %s"
    return conn.execute(query, (*params, limit)).fetchall()


def cancel_job(conn, job_id: UUID | str) -> tuple[CancelOutcome, dict | None]:
    """cancelled / already_cancelled → 200, terminal → 409, not_found → 404.

    One statement for both queued and running, so a reaper requeue in between can't make it miss.
    A running job is cancelled at once; its worker finds out at its next heartbeat or from a
    0-row complete, and writes nothing.
    """
    job = conn.execute(
        """
        UPDATE jobs SET status = 'cancelled', finished_at = now(), lease_expires_at = NULL
         WHERE id = %s AND status IN ('queued', 'running')
        RETURNING *
        """,
        (job_id,),
    ).fetchone()
    if job is not None:
        return "cancelled", job
    # 0 rows: the job was already terminal, and terminal states are never left, so this read
    # can't see it back in queued/running.
    job = get_job(conn, job_id)
    if job is None:
        return "not_found", None
    return ("already_cancelled" if job["status"] == "cancelled" else "terminal"), job


# --- worker side ---------------------------------------------------------------------------------

def claim(conn, worker_id: str, lease_s: float = config.LEASE_SECONDS) -> dict | None:
    """Take the oldest queued job. SKIP LOCKED: two workers never get the same row, and neither
    waits for the other. The returned `attempt` is the fencing token for every later write."""
    return conn.execute(
        """
        UPDATE jobs SET status = 'running', attempt = attempt + 1, worker_id = %(w)s,
               lease_expires_at = now() + make_interval(secs => %(lease)s),
               started_at = coalesce(started_at, now())
         WHERE id = (SELECT id FROM jobs WHERE status = 'queued'
                      ORDER BY created_at, id
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1)
        RETURNING id, payload, attempt
        """,
        {"w": worker_id, "lease": lease_s},
    ).fetchone()


def heartbeat(conn, job_id: UUID, attempt: int, lease_s: float = config.LEASE_SECONDS) -> bool:
    """False → the job was cancelled or the lease was lost: abandon it and write nothing."""
    cur = conn.execute(
        """
        UPDATE jobs SET lease_expires_at = now() + make_interval(secs => %s)
         WHERE id = %s AND status = 'running' AND attempt = %s
        """,
        (lease_s, job_id, attempt),
    )
    return cur.rowcount == 1


def complete(conn, job_id: UUID, attempt: int, worker_id: str) -> bool:
    """Effect and running → succeeded in ONE statement, so they commit together or not at all.

    The effect records the worker's own token, not the row's `attempt`: they are equal while the
    fence holds; if it breaks (M2), a zombie's effect keeps its stale token and invariant I6 sees it.
    False → cancelled or lease lost; nothing was written.
    """
    cur = conn.execute(
        """
        WITH done AS (
          UPDATE jobs SET status = 'succeeded', finished_at = now(), lease_expires_at = NULL
           WHERE id = %(id)s AND status = 'running' AND attempt = %(a)s
          RETURNING id)
        INSERT INTO job_effects (job_id, attempt, worker_id)
        SELECT id, %(a)s, %(w)s FROM done
        """,
        {"id": job_id, "a": attempt, "w": worker_id},
    )
    return cur.rowcount == 1


def fail(conn, job_id: UUID, attempt: int, error: str) -> bool:
    """The job itself errored (`fail=true`). Not retried. Same fence as complete, no effect."""
    cur = conn.execute(
        """
        UPDATE jobs SET status = 'failed', error = %s, finished_at = now(), lease_expires_at = NULL
         WHERE id = %s AND status = 'running' AND attempt = %s
        """,
        (error, job_id, attempt),
    )
    return cur.rowcount == 1


def reap(conn, max_attempts: int = config.MAX_ATTEMPTS, limit: int = config.REAP_BATCH) -> int:
    """Expired leases → queued, or failed once `max_attempts` claims have been used (poison job).
    Run by every worker every second; SKIP LOCKED keeps concurrent reapers off each other's rows."""
    cur = conn.execute(
        """
        UPDATE jobs SET
          status           = CASE WHEN attempt >= %(max)s THEN 'failed' ELSE 'queued' END,
          error            = CASE WHEN attempt >= %(max)s THEN 'lease expired; attempts exhausted' END,
          finished_at      = CASE WHEN attempt >= %(max)s THEN now() END,
          lease_expires_at = NULL
         WHERE id IN (SELECT id FROM jobs
                       WHERE status = 'running' AND lease_expires_at < now()
                       FOR UPDATE SKIP LOCKED
                       LIMIT %(limit)s)
        """,
        {"max": max_attempts, "limit": limit},
    )
    return cur.rowcount
