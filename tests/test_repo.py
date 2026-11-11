"""Single-threaded tests: every edge of the SPEC §3 state diagram, and the illegal ones rejected.
Concurrency is Phase 3 (test_races.py)."""

import pytest

from app import repo
from tests.helpers import (
    PAYLOAD,
    effects,
    expire_lease,
    lease_left,
    new_job,
    reclaimed,
    row,
    transitions,
)


# --- [*] -> queued: create -----------------------------------------------------------------------

class TestCreate:
    def test_new_key_creates_queued_job(self, conn):
        outcome, job = repo.create_job(conn, "k1", PAYLOAD)
        assert outcome == "created"
        assert job["status"] == "queued"
        assert job["attempt"] == 0
        assert job["payload"] == PAYLOAD

    def test_same_key_same_body_replays_existing_job(self, conn):
        _, first = repo.create_job(conn, "k1", PAYLOAD)
        outcome, again = repo.create_job(conn, "k1", PAYLOAD)
        assert outcome == "replay"
        assert again["id"] == first["id"]
        assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1

    def test_same_key_different_body_conflicts_and_keeps_original(self, conn):
        _, first = repo.create_job(conn, "k1", PAYLOAD)
        outcome, existing = repo.create_job(conn, "k1", {"duration_ms": 999, "fail": True})
        assert outcome == "conflict"
        assert existing["id"] == first["id"]
        assert row(conn, first["id"])["payload"] == PAYLOAD

    def test_body_key_order_does_not_change_the_hash(self, conn):
        repo.create_job(conn, "k1", {"duration_ms": 5, "fail": False})
        outcome, _ = repo.create_job(conn, "k1", {"fail": False, "duration_ms": 5})
        assert outcome == "replay"


# --- queued -> running: claim --------------------------------------------------------------------

class TestClaim:
    def test_claims_oldest_queued_job_with_a_lease(self, conn):
        first = new_job(conn)
        new_job(conn)
        job = repo.claim(conn, "w1", lease_s=5)
        assert job["id"] == first["id"]
        assert job["attempt"] == 1
        assert job["payload"] == PAYLOAD
        r = row(conn, first["id"])
        assert (r["status"], r["worker_id"]) == ("running", "w1")
        assert 4 < lease_left(conn, first["id"]) <= 5

    def test_empty_queue_returns_none(self, conn):
        assert repo.claim(conn, "w1") is None

    def test_running_job_is_not_claimed_again(self, conn):
        new_job(conn)
        assert repo.claim(conn, "w1") is not None
        assert repo.claim(conn, "w2") is None

    def test_started_at_is_kept_from_the_first_attempt(self, conn):
        job = new_job(conn)
        repo.claim(conn, "w1")
        started = row(conn, job["id"])["started_at"]
        reclaimed(conn, job["id"])
        assert row(conn, job["id"])["started_at"] == started


# --- heartbeat (running, lease extended) ---------------------------------------------------------

class TestHeartbeat:
    def test_extends_lease(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1", lease_s=1)
        assert repo.heartbeat(conn, job["id"], job["attempt"], lease_s=5)
        assert lease_left(conn, job["id"]) > 4

    def test_stale_attempt_is_rejected(self, conn):
        new_job(conn)
        old = repo.claim(conn, "w1")
        reclaimed(conn, old["id"])
        before = row(conn, old["id"])["lease_expires_at"]
        assert not repo.heartbeat(conn, old["id"], old["attempt"])
        assert row(conn, old["id"])["lease_expires_at"] == before

    def test_cancelled_job_is_rejected(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        repo.cancel_job(conn, job["id"])
        assert not repo.heartbeat(conn, job["id"], job["attempt"])
        assert row(conn, job["id"])["lease_expires_at"] is None


# --- running -> succeeded: complete --------------------------------------------------------------

class TestComplete:
    def test_marks_succeeded_and_writes_one_effect(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        assert repo.complete(conn, job["id"], job["attempt"], "w1")
        r = row(conn, job["id"])
        assert r["status"] == "succeeded"
        assert r["finished_at"] is not None and r["lease_expires_at"] is None
        assert effects(conn, job["id"]) == [(1, "w1")]

    def test_stale_attempt_writes_nothing_and_new_owner_completes(self, conn):
        new_job(conn)
        old = repo.claim(conn, "w1")
        new = reclaimed(conn, old["id"], worker="w2")
        assert not repo.complete(conn, old["id"], old["attempt"], "w1")
        assert row(conn, old["id"])["status"] == "running"
        assert effects(conn, old["id"]) == []
        assert repo.complete(conn, new["id"], new["attempt"], "w2")
        assert effects(conn, old["id"]) == [(2, "w2")]

    def test_cancelled_job_writes_nothing(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        repo.cancel_job(conn, job["id"])
        assert not repo.complete(conn, job["id"], job["attempt"], "w1")
        assert row(conn, job["id"])["status"] == "cancelled"
        assert effects(conn, job["id"]) == []

    def test_second_complete_writes_no_second_effect(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        assert repo.complete(conn, job["id"], job["attempt"], "w1")
        assert not repo.complete(conn, job["id"], job["attempt"], "w1")
        assert effects(conn, job["id"]) == [(1, "w1")]


# --- running -> failed: job error ----------------------------------------------------------------

class TestFail:
    def test_marks_failed_with_error_and_no_effect(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        assert repo.fail(conn, job["id"], job["attempt"], "boom")
        r = row(conn, job["id"])
        assert (r["status"], r["error"]) == ("failed", "boom")
        assert r["finished_at"] is not None and r["lease_expires_at"] is None
        assert effects(conn, job["id"]) == []

    def test_stale_attempt_is_rejected(self, conn):
        new_job(conn)
        old = repo.claim(conn, "w1")
        reclaimed(conn, old["id"])
        assert not repo.fail(conn, old["id"], old["attempt"], "boom")
        assert row(conn, old["id"])["status"] == "running"

    def test_cancelled_job_stays_cancelled(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        repo.cancel_job(conn, job["id"])
        assert not repo.fail(conn, job["id"], job["attempt"], "boom")
        assert row(conn, job["id"])["status"] == "cancelled"


# --- running -> queued / failed: reaper ----------------------------------------------------------

class TestReap:
    def test_requeues_expired_lease(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        expire_lease(conn, job["id"])
        assert repo.reap(conn) == 1
        r = row(conn, job["id"])
        assert (r["status"], r["lease_expires_at"], r["attempt"]) == ("queued", None, 1)
        assert repo.claim(conn, "w2")["attempt"] == 2

    def test_ignores_live_lease(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        assert repo.reap(conn) == 0
        assert row(conn, job["id"])["status"] == "running"

    def test_fails_job_when_attempts_are_exhausted(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        expire_lease(conn, job["id"])
        assert repo.reap(conn, max_attempts=2) == 1
        job = repo.claim(conn, "w2")
        assert job["attempt"] == 2
        expire_lease(conn, job["id"])
        assert repo.reap(conn, max_attempts=2) == 1
        r = row(conn, job["id"])
        assert (r["status"], r["error"]) == ("failed", "lease expired; attempts exhausted")
        assert r["finished_at"] is not None and r["lease_expires_at"] is None


# --- queued / running -> cancelled ---------------------------------------------------------------

class TestCancel:
    def test_queued_job(self, conn):
        job = new_job(conn)
        outcome, r = repo.cancel_job(conn, job["id"])
        assert outcome == "cancelled"
        assert r["status"] == "cancelled" and r["finished_at"] is not None

    def test_running_job_is_cancelled_at_once_and_lease_dropped(self, conn):
        new_job(conn)
        job = repo.claim(conn, "w1")
        outcome, r = repo.cancel_job(conn, job["id"])
        assert outcome == "cancelled"
        assert (r["status"], r["lease_expires_at"]) == ("cancelled", None)

    def test_cancel_twice_is_idempotent(self, conn):
        job = new_job(conn)
        repo.cancel_job(conn, job["id"])
        outcome, r = repo.cancel_job(conn, job["id"])
        assert outcome == "already_cancelled"
        assert r["status"] == "cancelled"

    @pytest.mark.parametrize("final", ["succeeded", "failed"])
    def test_finished_job_is_rejected(self, conn, final):
        job = finish(conn, final)
        outcome, r = repo.cancel_job(conn, job["id"])
        assert outcome == "terminal"
        assert r["status"] == final

    def test_unknown_job(self, conn):
        assert repo.cancel_job(conn, "00000000-0000-0000-0000-000000000000") == ("not_found", None)


# --- terminal states are never left --------------------------------------------------------------

def finish(conn, final: str) -> dict:
    """A job that went queued -> running -> `final`."""
    new_job(conn)
    job = repo.claim(conn, "w1")
    if final == "succeeded":
        assert repo.complete(conn, job["id"], job["attempt"], "w1")
    elif final == "failed":
        assert repo.fail(conn, job["id"], job["attempt"], "boom")
    else:
        assert repo.cancel_job(conn, job["id"])[0] == "cancelled"
    return job


@pytest.mark.parametrize("final", ["succeeded", "failed", "cancelled"])
def test_terminal_job_is_never_changed(conn, final):
    job = finish(conn, final)
    before = (row(conn, job["id"]), effects(conn, job["id"]), transitions(conn, job["id"]))

    assert repo.claim(conn, "w2") is None
    assert not repo.heartbeat(conn, job["id"], job["attempt"])
    assert not repo.complete(conn, job["id"], job["attempt"], "w1")
    assert not repo.fail(conn, job["id"], job["attempt"], "late")
    assert repo.cancel_job(conn, job["id"])[0] in ("terminal", "already_cancelled")
    assert repo.reap(conn) == 0

    assert (row(conn, job["id"]), effects(conn, job["id"]), transitions(conn, job["id"])) == before


# --- audit log: one job_transitions row per status change ----------------------------------------

def test_every_status_change_is_logged_once(conn):
    job = new_job(conn)
    first = repo.claim(conn, "w1")
    repo.heartbeat(conn, job["id"], first["attempt"])  # no status change -> no row
    second = reclaimed(conn, job["id"])
    repo.complete(conn, job["id"], second["attempt"], "w2")
    assert transitions(conn, job["id"]) == [
        (None, "queued", 0),
        ("queued", "running", 1),
        ("running", "queued", 1),
        ("queued", "running", 2),
        ("running", "succeeded", 2),
    ]


def test_cancel_is_logged(conn):
    job = new_job(conn)
    repo.cancel_job(conn, job["id"])
    assert transitions(conn, job["id"]) == [(None, "queued", 0), ("queued", "cancelled", 0)]


# --- reads ---------------------------------------------------------------------------------------

def test_get_job(conn):
    job = new_job(conn)
    assert repo.get_job(conn, job["id"])["id"] == job["id"]
    assert repo.get_job(conn, "00000000-0000-0000-0000-000000000000") is None


def test_list_jobs_pages_newest_first_without_gaps(conn):
    for _ in range(5):
        new_job(conn)
    expected = [
        r["id"] for r in conn.execute("SELECT id FROM jobs ORDER BY created_at DESC, id DESC")
    ]
    seen, after = [], None
    while page := repo.list_jobs(conn, limit=2, after=after):
        seen += [r["id"] for r in page]
        after = (page[-1]["created_at"], page[-1]["id"])
    assert seen == expected


def test_list_jobs_filters_by_status(conn):
    keep = new_job(conn)
    gone = new_job(conn)
    repo.cancel_job(conn, gone["id"])
    assert [r["id"] for r in repo.list_jobs(conn, limit=10, status="queued")] == [keep["id"]]
