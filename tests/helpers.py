"""Test-only helpers: set up states and read the tables. Shared with the race tests (Phase 3)."""

from uuid import uuid4

from app import repo

PAYLOAD = {"duration_ms": 0, "fail": False}


def new_job(conn, key: str | None = None, payload: dict | None = None) -> dict:
    outcome, job = repo.create_job(conn, key or str(uuid4()), payload or PAYLOAD)
    assert outcome == "created"
    return job


def expire_lease(conn, job_id) -> None:
    """Simulate time passing: move the lease into the past. Doesn't touch status, so no transition."""
    conn.execute(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second'"
        " WHERE id = %s AND status = 'running'",
        (job_id,),
    )


def row(conn, job_id) -> dict:
    return conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()


def lease_left(conn, job_id) -> float:
    """Seconds until the lease expires, measured by the DB clock."""
    r = conn.execute(
        "SELECT extract(epoch FROM lease_expires_at - now()) AS s FROM jobs WHERE id = %s",
        (job_id,),
    ).fetchone()
    return float(r["s"])


def effects(conn, job_id) -> list[tuple[int, str]]:
    rows = conn.execute(
        "SELECT attempt, worker_id FROM job_effects WHERE job_id = %s ORDER BY id", (job_id,)
    ).fetchall()
    return [(r["attempt"], r["worker_id"]) for r in rows]


def transitions(conn, job_id) -> list[tuple[str | None, str, int]]:
    rows = conn.execute(
        "SELECT from_status, to_status, attempt FROM job_transitions WHERE job_id = %s ORDER BY seq",
        (job_id,),
    ).fetchall()
    return [(r["from_status"], r["to_status"], r["attempt"]) for r in rows]


def reclaimed(conn, job_id, worker: str = "w2") -> dict:
    """Lease of the current holder expires, the reaper requeues, `worker` claims the next attempt."""
    expire_lease(conn, job_id)
    assert repo.reap(conn) == 1
    job = repo.claim(conn, worker)
    assert job is not None and job["id"] == job_id
    return job
