"""Worker loop (SPEC §5, PLAN Phase 5): real Postgres, short timings.

`run_job` is driven directly with a claimed job; the main loop runs in a thread (`worker.run`) or as
a real `python -m app.worker` subprocess for signal handling. Each test names the hand-break that
must make it fail.
"""

import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest

from app import repo, worker
from tests.helpers import effects, expire_lease, new_job, reclaimed, row, transitions

ROOT = Path(__file__).resolve().parent.parent
WID = "test-worker"

# Short timings so a test takes well under a second; lease > heartbeat + chunk with margin.
FAST = worker.Timings(
    lease=0.5, heartbeat=0.1, reap=0.1, chunk=0.05, idle_min=0.02, idle_max=0.05, max_attempts=5
)
# Upper bound for noticing a cancel / lost lease. Expected ≈ heartbeat + chunk (0.15 s); the slack
# is for a loaded CI box. The broken behavior (ignoring a False heartbeat) takes ~2.8 s.
NOTICE_SECONDS = 1.0


def payload(ms: int, fail: bool = False) -> dict:
    return {"duration_ms": ms, "fail": fail}


def wait_for(predicate, timeout: float = 5.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


def claims(conn, job_id) -> list[int]:
    """The attempt of every claim (any transition into running), in order."""
    return [a for _, to, a in transitions(conn, job_id) if to == "running"]


def wait_for_heartbeat(conn, job_id) -> None:
    """Proof the worker is inside its loop: a heartbeat has moved the lease past the claim's."""
    first = row(conn, job_id)["lease_expires_at"]
    wait_for(lambda: row(conn, job_id)["lease_expires_at"] > first, what="a heartbeat")


@pytest.fixture
def background():
    with ThreadPoolExecutor(max_workers=2) as p:
        yield p


# --- run_job -------------------------------------------------------------------------------------

def test_success_writes_one_effect_with_workers_attempt(conn, connect):
    """Breaks if complete isn't called, or is called with the row's attempt instead of the token."""
    job = new_job(conn, payload=payload(50))
    w = connect(name=WID)
    claimed = repo.claim(w, WID, FAST.lease)

    assert worker.run_job(w, claimed, WID, timings=FAST) == "succeeded"
    assert row(conn, job["id"])["status"] == "succeeded"
    assert effects(conn, job["id"]) == [(1, WID)]


def test_fail_flag_fails_the_job_without_effect(conn, connect):
    """Breaks if fail=true is completed like a normal job (would write an effect)."""
    job = new_job(conn, payload=payload(20, fail=True))
    w = connect(name=WID)
    claimed = repo.claim(w, WID, FAST.lease)

    assert worker.run_job(w, claimed, WID, timings=FAST) == "failed"
    r = row(conn, job["id"])
    assert r["status"] == "failed" and r["error"]
    assert effects(conn, job["id"]) == []


def test_job_longer_than_lease_survives_concurrent_reaper(conn, connect, background):
    """Job runs 1 s on a 0.5 s lease while another session reaps nonstop: heartbeats keep the lease,
    so it is claimed once and succeeds. Breaks if the worker stops heartbeating."""
    job = new_job(conn, payload=payload(1000))
    w = connect(name=WID)
    claimed = repo.claim(w, WID, FAST.lease)

    done = threading.Event()
    reaped = []

    def reaper(c):
        while not done.is_set():
            reaped.append(repo.reap(c))
            time.sleep(0.02)

    other = background.submit(reaper, connect(name="other-reaper"))
    try:
        outcome = worker.run_job(w, claimed, WID, timings=FAST)
    finally:
        done.set()
        other.result(timeout=5)

    assert outcome == "succeeded"
    assert len(reaped) > 10 and sum(reaped) == 0  # the reaper ran all along, never found it expired
    assert claims(conn, job["id"]) == [1]
    assert effects(conn, job["id"]) == [(1, WID)]


def test_cancel_mid_run_abandons_within_a_heartbeat(conn, connect, background):
    """Breaks if a False heartbeat is ignored: the worker would run the full 3 s before noticing."""
    job = new_job(conn, payload=payload(3000))
    w = connect(name=WID)
    claimed = repo.claim(w, WID, FAST.lease)

    fut = background.submit(worker.run_job, w, claimed, WID, timings=FAST)
    wait_for_heartbeat(conn, job["id"])
    assert repo.cancel_job(conn, job["id"])[0] == "cancelled"
    t0 = time.monotonic()
    outcome = fut.result(timeout=10)
    elapsed = time.monotonic() - t0

    assert outcome == "abandoned"
    assert elapsed < NOTICE_SECONDS, f"took {elapsed:.2f}s to notice the cancel"
    assert row(conn, job["id"])["status"] == "cancelled"
    assert effects(conn, job["id"]) == []


def test_lost_lease_abandons_and_new_owner_completes(conn, connect, background):
    """While the worker runs, its lease is expired, reaped and re-claimed by w2 in one transaction
    (so no heartbeat lands in between): the old owner gives up within a heartbeat and writes
    nothing; only w2's effect exists. Breaks if a False heartbeat is ignored."""
    job = new_job(conn, payload=payload(3000))
    w = connect(name=WID)
    claimed = repo.claim(w, WID, FAST.lease)

    fut = background.submit(worker.run_job, w, claimed, WID, timings=FAST)
    wait_for_heartbeat(conn, job["id"])
    with conn.transaction():
        assert reclaimed(conn, job["id"], "w2")["attempt"] == 2
    t0 = time.monotonic()
    outcome = fut.result(timeout=10)
    elapsed = time.monotonic() - t0

    assert outcome == "abandoned"
    assert elapsed < NOTICE_SECONDS, f"took {elapsed:.2f}s to notice"
    assert effects(conn, job["id"]) == []
    assert repo.complete(conn, job["id"], 2, "w2")
    assert effects(conn, job["id"]) == [(2, "w2")]


def test_busy_worker_still_reaps(conn, connect):
    """A dead worker's expired lease is requeued by a worker that is busy with another job.
    Breaks if reaping happens only between jobs (recovery would wait for a free worker)."""
    dead = new_job(conn, payload=payload(0))
    assert repo.claim(conn, "ghost", 30)["id"] == dead["id"]
    expire_lease(conn, dead["id"])
    busy = new_job(conn, payload=payload(400))
    w = connect(name=WID)
    claimed = repo.claim(w, WID, FAST.lease)
    assert claimed["id"] == busy["id"]

    assert worker.run_job(w, claimed, WID, timings=FAST) == "succeeded"
    assert row(conn, dead["id"])["status"] == "queued"
    assert ("running", "queued", 1) in transitions(conn, dead["id"])


# --- main loop -----------------------------------------------------------------------------------

@pytest.fixture
def wid():
    """A worker id unique to this test: pg_stat_activity can still list a previous test's session
    for a moment after it closed, so a shared name could match the wrong backend."""
    return f"{WID}-{uuid4().hex[:8]}"


@pytest.fixture
def loop(db_url, wid):
    """worker.run in a thread with FAST timings; stopped (and required to stop) at teardown."""
    stop = threading.Event()
    thread = threading.Thread(
        target=worker.run, args=(db_url, wid, stop), kwargs={"timings": FAST}, daemon=True
    )
    thread.start()
    yield thread
    stop.set()
    thread.join(timeout=5)
    assert not thread.is_alive(), "loop ignored stop"


def backend_pid(conn, name: str) -> int | None:
    r = conn.execute(
        "SELECT pid FROM pg_stat_activity WHERE application_name = %s", (name,)
    ).fetchone()
    return r and r["pid"]


def test_loop_survives_killed_backend_mid_job(conn, loop, wid, caplog):
    """pg_terminate_backend mid-job: the job is abandoned, the loop reconnects, the lease expires,
    the job is re-run (attempt 2) and the next job is done too. Breaks if the loop doesn't reopen
    a broken connection."""
    caplog.set_level(logging.INFO, logger="worker")
    first = new_job(conn, payload=payload(800))
    wait_for(lambda: row(conn, first["id"])["status"] == "running", what="first claim")
    conn.execute("SELECT pg_terminate_backend(%s)", (backend_pid(conn, wid),))
    second = new_job(conn, payload=payload(50))
    wait_for(
        lambda: {row(conn, j["id"])["status"] for j in (first, second)} == {"succeeded"},
        timeout=10,
        what="both jobs succeeded",
    )

    assert claims(conn, first["id"]) == [1, 2]
    assert effects(conn, first["id"]) == [(2, wid)]
    assert effects(conn, second["id"]) == [(1, wid)]
    assert "abandoned" in caplog.text and "reconnect" in caplog.text


def test_loop_survives_killed_backend_while_idle(conn, loop, wid, caplog):
    """pg_terminate_backend while the queue is empty: the next reap/claim raises, the loop backs
    off, reconnects and processes a job submitted afterwards. Breaks if a DB error in reap/claim
    escapes the loop (the thread would die)."""
    caplog.set_level(logging.INFO, logger="worker")
    wait_for(lambda: backend_pid(conn, wid) is not None, what="worker connected")
    old = backend_pid(conn, wid)
    conn.execute("SELECT pg_terminate_backend(%s)", (old,))
    wait_for(lambda: backend_pid(conn, wid) not in (None, old), what="worker reconnected")
    job = new_job(conn, payload=payload(20))
    wait_for(lambda: row(conn, job["id"])["status"] == "succeeded", what="job after reconnect")

    assert loop.is_alive()
    assert effects(conn, job["id"]) == [(1, wid)]
    assert "db_error" in caplog.text and "reconnect" in caplog.text


def test_loop_survives_poison_row(conn, loop, wid, caplog):
    """A row the API would have rejected (no duration_ms) makes run_job raise KeyError. The loop logs
    it and goes on; lease expiry + max_attempts turn the row into `failed` (SPEC §7 poison job).
    Breaks if a non-DB exception escapes the loop: the worker dies, and in compose (no restart
    policy) one such row kills every worker that claims it."""
    caplog.set_level(logging.INFO, logger="worker")
    bad = new_job(conn, payload={"fail": False})
    good = new_job(conn, payload=payload(20))
    wait_for(lambda: row(conn, good["id"])["status"] == "succeeded", what="good job")
    wait_for(lambda: row(conn, bad["id"])["status"] == "failed", timeout=15, what="poison row failed")

    assert loop.is_alive()
    assert claims(conn, bad["id"]) == [1, 2, 3, 4, 5]
    assert row(conn, bad["id"])["error"] == "lease expired; attempts exhausted"
    assert effects(conn, bad["id"]) == []
    assert "reason=crash" in caplog.text


def test_subprocess_sigterm_finishes_current_job_and_claims_nothing_more(conn, db_url):
    """`python -m app.worker` drains short jobs; SIGTERM during a long job → that job still succeeds,
    the job queued after it is left alone, exit code 0. Breaks if SIGTERM is swallowed (no exit),
    aborts the current job, or the loop claims again after the signal."""
    shorts = [new_job(conn, payload=payload(20))["id"] for _ in range(5)]
    env = {**os.environ, "DATABASE_URL": db_url, "WORKER_ID": "sub-worker", "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.worker"],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        wait_for(
            lambda: all(row(conn, j)["status"] == "succeeded" for j in shorts),
            timeout=15, what="short jobs drained",
        )
        long = new_job(conn, payload=payload(1500))["id"]
        wait_for(lambda: row(conn, long)["status"] == "running", what="long job claimed")
        after = new_job(conn, payload=payload(20))["id"]
        proc.send_signal(signal.SIGTERM)
        try:
            out, _ = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            pytest.fail("worker did not exit within 10 s of SIGTERM")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()

    assert proc.returncode == 0, out
    assert row(conn, long)["status"] == "succeeded", out
    assert row(conn, after)["status"] == "queued", out
    assert all(effects(conn, j) == [(1, "sub-worker")] for j in shorts)


# --- clock discipline (SPEC §9 clock skew) -------------------------------------------------------

WALL_CLOCK = re.compile(r"\btime\.time\(|\bdatetime\.(now|utcnow|today)\(|\bdate\.today\(|from time import")


def test_no_wall_clock_in_app():
    """Leases use the DB's now(); a worker clock may only schedule (time.monotonic).
    Guard test: passes on any code without wall-clock calls; proven by adding one by hand."""
    hits = [
        f"{p.relative_to(ROOT)}:{n}: {line.strip()}"
        for p in sorted((ROOT / "app").rglob("*.py"))
        for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
        if WALL_CLOCK.search(line)
    ]
    assert hits == []
