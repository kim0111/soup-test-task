"""Race tests at the SQL level (SPEC §9): real Postgres, one session per simulated process.

Interleavings are forced, not hoped for: one session holds an uncommitted statement inside an
explicit transaction, and the other is observed in pg_stat_activity waiting on that row lock
before the first commits. Concurrency without a forced order (claims, creates) uses a barrier so
all threads hit the database together.

Each test names the deliberate break (SPEC §8.4) that must make it fail.
"""

import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Barrier

import pytest

from app import repo
from tests.helpers import PAYLOAD, effects, expire_lease, new_job, row, transitions

THREADS = 50


@pytest.fixture
def pool():
    with ThreadPoolExecutor(max_workers=THREADS) as p:
        yield p


def blocked_on_lock(observer, pid: int, fut: Future, timeout: float = 5.0) -> bool:
    """Wait until session `pid` is waiting on a lock (True) or its call has returned (False).

    Keep this short: the session holding the lock is idle in transaction, and the server kills
    such sessions after idle_in_transaction_session_timeout (2 s).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fut.done():
            return False
        r = observer.execute(
            "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,)
        ).fetchone()
        if r is not None and r["wait_event_type"] == "Lock":
            return True
        time.sleep(0.005)
    raise AssertionError(f"session {pid} neither returned nor blocked within {timeout}s")


def all_at_once(pool, conns, call):
    """Run call(i, conn) on every connection at the same moment; return the results in order."""
    barrier = Barrier(len(conns))

    def run(i, c):
        barrier.wait(timeout=10)
        return call(i, c)

    return [f.result(timeout=30) for f in [pool.submit(run, i, c) for i, c in enumerate(conns)]]


# --- double claim --------------------------------------------------------------------------------

def test_concurrent_claims(conn, connect, pool):
    """50 workers race for 20 jobs: every job is claimed exactly once, with attempt 1.
    Breaks if the claim stops being one locked statement (M1: SELECT, then UPDATE by id)."""
    jobs = {new_job(conn)["id"] for _ in range(20)}
    conns = [connect(name=f"w{i}") for i in range(THREADS)]

    def drain(i, c):
        # Bounded: a claim that ignores status would never return None and hang the suite.
        mine = []
        for _ in range(len(jobs) + 1):
            if (job := repo.claim(c, f"w{i}")) is None:
                break
            mine.append((job["id"], job["attempt"]))
        return mine

    claims = [claim for mine in all_at_once(pool, conns, drain) for claim in mine]

    per_job = Counter(job_id for job_id, _ in claims)
    assert set(per_job) == jobs
    assert [n for n in per_job.values() if n != 1] == []
    assert {attempt for _, attempt in claims} == {1}
    for job_id in jobs:
        assert row(conn, job_id)["status"] == "running"
        assert transitions(conn, job_id) == [(None, "queued", 0), ("queued", "running", 1)]


# --- zombie worker -------------------------------------------------------------------------------

def test_zombie_cannot_complete(conn, connect):
    """A worker that froze past its lease comes back: none of its writes land, whether the job is
    queued again or already re-claimed, and the new owner's single effect carries attempt 2.
    Breaks if heartbeat/complete/fail stop fencing on `attempt` (M2)."""
    zombie, owner = connect(name="w1"), connect(name="w2")
    job_id = new_job(conn)["id"]

    stale = repo.claim(zombie, "w1")
    assert stale["attempt"] == 1
    expire_lease(conn, job_id)  # w1 freezes; its lease runs out
    assert repo.reap(conn) == 1

    # w1 wakes up while the job is queued again
    assert not repo.heartbeat(zombie, job_id, stale["attempt"])
    assert not repo.complete(zombie, job_id, stale["attempt"], "w1")
    assert row(conn, job_id)["status"] == "queued"

    fresh = repo.claim(owner, "w2")
    assert (fresh["id"], fresh["attempt"]) == (job_id, 2)
    lease = row(conn, job_id)["lease_expires_at"]

    # w1 keeps going while w2 owns the job
    assert not repo.heartbeat(zombie, job_id, stale["attempt"])
    assert row(conn, job_id)["lease_expires_at"] == lease  # w2's lease not touched
    assert not repo.complete(zombie, job_id, stale["attempt"], "w1")
    assert not repo.fail(zombie, job_id, stale["attempt"], "late")
    assert effects(conn, job_id) == []

    assert repo.heartbeat(owner, job_id, fresh["attempt"])
    assert repo.complete(owner, job_id, fresh["attempt"], "w2")
    r = row(conn, job_id)
    assert (r["status"], r["attempt"], r["worker_id"]) == ("succeeded", 2, "w2")
    assert effects(conn, job_id) == [(2, "w2")]
    assert transitions(conn, job_id) == [
        (None, "queued", 0),
        ("queued", "running", 1),
        ("running", "queued", 1),
        ("queued", "running", 2),
        ("running", "succeeded", 2),
    ]


# --- cancel vs complete --------------------------------------------------------------------------

def test_cancel_complete_race_complete_first(conn, connect, pool):
    """Complete holds the row lock; a cancel arrives and waits. After the commit the cancel's
    WHERE is re-checked against `succeeded`: 409 (terminal), and the effect stands."""
    worker, api = connect(name="w1"), connect("api", "api")
    job_id = new_job(conn)["id"]
    job = repo.claim(worker, "w1")

    with worker.transaction():
        assert repo.complete(worker, job_id, job["attempt"], "w1")
        cancel = pool.submit(repo.cancel_job, api, job_id)
        assert blocked_on_lock(conn, api.info.backend_pid, cancel)

    outcome, r = cancel.result(timeout=5)
    assert outcome == "terminal"  # → 409
    assert r["status"] == "succeeded"
    assert effects(conn, job_id) == [(1, "w1")]


def test_cancel_complete_race_cancel_first(conn, connect, pool):
    """Cancel holds the row lock; the worker's complete arrives and waits. After the commit the
    complete's WHERE is re-checked against `cancelled`: 0 rows, so no effect; the cancel was a 200.
    Breaks if complete stops requiring status = 'running' (M3)."""
    worker, api = connect(name="w1"), connect("api", "api")
    job_id = new_job(conn)["id"]
    job = repo.claim(worker, "w1")

    with api.transaction():
        outcome, _ = repo.cancel_job(api, job_id)
        assert outcome == "cancelled"  # → 200
        complete = pool.submit(repo.complete, worker, job_id, job["attempt"], "w1")
        assert blocked_on_lock(conn, worker.info.backend_pid, complete)

    assert complete.result(timeout=5) is False
    assert row(conn, job_id)["status"] == "cancelled"
    assert effects(conn, job_id) == []


# --- duplicate create on client retry ------------------------------------------------------------

def test_idempotent_create(conn, connect, pool):
    """50 concurrent POSTs with one key and one body: one 201, 49 × 200, all the same job, one row.
    The same key with another body afterwards is a 409 and changes nothing.
    Breaks if create becomes SELECT-then-INSERT without the unique index (M4)."""
    conns = [connect("api", f"api{i}") for i in range(THREADS)]

    results = all_at_once(pool, conns, lambda i, c: repo.create_job(c, "key-1", PAYLOAD))

    assert Counter(outcome for outcome, _ in results) == {"created": 1, "replay": THREADS - 1}
    ids = {job["id"] for _, job in results}
    assert len(ids) == 1
    job_id = ids.pop()
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1
    assert transitions(conn, job_id) == [(None, "queued", 0)]

    outcome, job = repo.create_job(conn, "key-1", {"duration_ms": 1, "fail": True})
    assert (outcome, job["id"]) == ("conflict", job_id)  # → 409
    assert row(conn, job_id)["payload"] == PAYLOAD


def test_idempotent_create_different_bodies_first_committer_wins(conn, connect, pool):
    """Two bodies race on one key: exactly one insert wins; requests with the winner's body get
    its job, requests with the other body get 409 — never a second row."""
    bodies = [PAYLOAD, {"duration_ms": 1, "fail": True}]
    conns = [connect("api", f"api{i}") for i in range(THREADS)]

    results = all_at_once(pool, conns, lambda i, c: repo.create_job(c, "key-1", bodies[i % 2]))

    created = [i for i, (outcome, _) in enumerate(results) if outcome == "created"]
    assert len(created) == 1
    winner = bodies[created[0] % 2]
    for i, (outcome, job) in enumerate(results):
        expected = {"created", "replay"} if bodies[i % 2] == winner else {"conflict"}
        assert outcome in expected, (i, outcome)
    assert len({job["id"] for _, job in results}) == 1
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1
    assert row(conn, results[0][1]["id"])["payload"] == winner


# --- isolation level -----------------------------------------------------------------------------

def naive_read(c):
    return c.execute(
        "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at, id LIMIT 1"
    ).fetchone()["id"]


def naive_write(c, job_id, worker: str) -> int:
    return c.execute(
        "UPDATE jobs SET status = 'running', attempt = attempt + 1, worker_id = %s,"
        "       lease_expires_at = now() + interval '5 seconds'"
        " WHERE id = %s",
        (worker, job_id),
    ).rowcount


def test_naive_pattern_breaks_under_rc(conn, connect, pool):
    """READ COMMITTED alone doesn't make read-then-write safe: two workers that each read the
    oldest queued job and then update it by id (no status predicate) both 'claim' it.
    Our claim, under the same overlap, gives the job to exactly one of them.
    The second half breaks if the claim becomes read-then-write (M1)."""
    a, b = connect(name="A"), connect(name="B")
    for c in (a, b):
        assert c.execute("SHOW transaction_isolation").fetchone()["transaction_isolation"] == (
            "read committed"
        )

    # Naive: both read before either writes.
    x = new_job(conn)["id"]
    with b.transaction():
        with a.transaction():
            assert naive_read(a) == x
            assert naive_read(b) == x
            assert naive_write(a, x, "A") == 1
        # A committed. Under RC, B's UPDATE simply applies to the newest row version.
        assert naive_write(b, x, "B") == 1
    r = row(conn, x)
    assert (r["attempt"], r["worker_id"]) == (2, "B")
    assert transitions(conn, x) == [
        (None, "queued", 0),
        ("queued", "running", 1),
        ("running", "running", 2),  # double claim: the job is now processed twice
    ]

    # Ours: A's claim is uncommitted when B claims.
    y = new_job(conn)["id"]
    with a.transaction():
        mine = repo.claim(a, "A")
        assert mine["id"] == y
        theirs = pool.submit(repo.claim, b, "B")
        blocked_on_lock(conn, b.info.backend_pid, theirs)  # returned (SKIP LOCKED) or waiting on A
    assert theirs.result(timeout=5) is None
    r = row(conn, y)
    assert (r["attempt"], r["worker_id"]) == (1, "A")
    assert transitions(conn, y) == [(None, "queued", 0), ("queued", "running", 1)]
