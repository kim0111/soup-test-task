"""The chaos checker must be able to fail (SPEC §8.2, PLAN Phase 7.1).

Histories are built with the real repo functions, then corrupted by hand the way each bug would
corrupt them. A checker that only ever said PASS would prove nothing about a green chaos run.
"""

from chaos import invariants
from chaos.invariants import EXHAUSTED
from app import repo
from tests.helpers import expire_lease, new_job, reclaimed

OK = {"duration_ms": 0, "fail": False}
BAD = {"duration_ms": 0, "fail": True}


def created(job: dict, status: int = 201, body: dict | None = None) -> dict:
    return {"op": "create", "key": job["idempotency_key"], "body": body or job["payload"],
            "status": status, "id": str(job["id"])}


def cancel(job: dict, status: int | None = 200) -> dict:
    return {"op": "cancel", "key": job["idempotency_key"], "id": str(job["id"]), "status": status}


def check(conn, log: list[dict]) -> dict[str, invariants.Result]:
    return {r.id: r for r in invariants.check(conn, log, max_attempts=5)}


def failing(results: dict[str, invariants.Result]) -> set[str]:
    return {i for i, r in results.items() if not r.ok}


def succeeded_job(conn, key: str = "s") -> dict:
    job = new_job(conn, key)
    claimed = repo.claim(conn, "w1")
    assert repo.complete(conn, job["id"], claimed["attempt"], "w1")
    return job


def exhausted_job(conn, key: str, claims: int, max_attempts: int) -> dict:
    """`claims` claims, each lost to lease expiry; the last reap fails the job."""
    job = new_job(conn, key)
    for _ in range(claims):
        assert repo.claim(conn, "w1")["id"] == job["id"]
        expire_lease(conn, job["id"])
        assert repo.reap(conn, max_attempts=max_attempts) == 1
    return job


# --- the legal history passes ------------------------------------------------------------------

def test_every_legal_history_passes_all_invariants(conn):
    log = []
    s = succeeded_job(conn, "s")
    log += [created(s), created(s, 200), created(s, 409, body=BAD)]  # retry + other body

    f = new_job(conn, "f", BAD)
    assert repo.fail(conn, f["id"], repo.claim(conn, "w1")["attempt"], "boom")
    log.append(created(f))

    cq = new_job(conn, "cq")
    assert repo.cancel_job(conn, cq["id"])[0] == "cancelled"
    log += [created(cq), cancel(cq)]

    cr = new_job(conn, "cr")
    repo.claim(conn, "w1")
    assert repo.cancel_job(conn, cr["id"])[0] == "cancelled"
    log += [created(cr), cancel(cr), cancel(cr, None)]  # second cancel timed out

    r = new_job(conn, "r")
    repo.claim(conn, "w1")
    again = reclaimed(conn, r["id"])
    assert repo.complete(conn, r["id"], again["attempt"], "w2")
    log.append(created(r))

    late = succeeded_job(conn, "late")
    log += [created(late), cancel(late, 409)]  # cancel lost the race: 409, effect stands

    x = exhausted_job(conn, "x", claims=5, max_attempts=5)
    log.append(created(x))

    results = check(conn, log)

    assert failing(results) == set()
    assert results["I1"].info["exhausted"] == 1
    assert sorted(results) == [f"I{i}" for i in range(1, 9)]


# --- each invariant fires on the corruption it exists for --------------------------------------

def test_i1_lost_job(conn):
    ghost = {"op": "create", "key": "ghost", "body": OK, "status": 201,
             "id": "00000000-0000-0000-0000-000000000000"}
    results = check(conn, [ghost])
    assert failing(results) == {"I1"}
    assert results["I1"].examples == ["ghost"]


def test_i1_job_that_should_succeed_ended_failed(conn):
    job = new_job(conn, "k")
    assert repo.fail(conn, job["id"], repo.claim(conn, "w1")["attempt"], "worker bug")
    assert failing(check(conn, [created(job)])) == {"I1"}


def test_i1_exhausted_with_too_few_claims(conn):
    # A reaper that gives up early looks like "attempts exhausted" but only used 4 of 5 claims.
    job = exhausted_job(conn, "k", claims=4, max_attempts=4)
    assert job and conn.execute("SELECT error FROM jobs").fetchone()["error"] == EXHAUSTED
    results = check(conn, [created(job)])
    assert failing(results) == {"I1"}
    assert results["I1"].info["exhausted"] == 0


def test_i2_two_ids_for_one_key(conn):
    a = new_job(conn, "k1")
    b = new_job(conn, "k2")
    log = [created(a), created({**b, "idempotency_key": "k1"}, 200)]
    assert "I2" in failing(check(conn, log))


def test_i2_other_body_accepted(conn):
    job = succeeded_job(conn, "k")
    assert failing(check(conn, [created(job), created(job, 200, body=BAD)])) == {"I2"}


def test_i2_same_body_rejected(conn):
    job = succeeded_job(conn, "k")
    assert failing(check(conn, [created(job), created(job, 409)])) == {"I2"}


def test_i3_job_left_queued(conn):
    job = new_job(conn, "k")
    results = check(conn, [])
    assert failing(results) == {"I3"}
    assert results["I3"].examples == [str(job["id"])]


def test_i4_i5_second_effect(conn):
    job = succeeded_job(conn, "k")
    conn.execute("INSERT INTO job_effects (job_id, attempt, worker_id) VALUES (%s, 1, 'w9')",
                 (job["id"],))
    assert failing(check(conn, [created(job)])) == {"I4", "I5"}


def test_i5_succeeded_without_effect(conn):
    job = new_job(conn, "k")
    repo.claim(conn, "w1")
    conn.execute("UPDATE jobs SET status = 'succeeded', lease_expires_at = NULL")
    assert failing(check(conn, [created(job)])) == {"I5"}


def test_i5_effect_on_cancelled_job(conn):
    job = new_job(conn, "k")
    repo.claim(conn, "w1")
    repo.cancel_job(conn, job["id"])
    conn.execute("INSERT INTO job_effects (job_id, attempt, worker_id) VALUES (%s, 1, 'w1')",
                 (job["id"],))
    assert failing(check(conn, [created(job), cancel(job)])) == {"I5"}


def test_i6_effect_with_stale_attempt(conn):
    # What a broken fence (M2) leaves: the zombie's token on the effect, the new owner's on the row.
    job = new_job(conn, "k")
    repo.claim(conn, "w1")
    again = reclaimed(conn, job["id"])
    assert repo.complete(conn, job["id"], again["attempt"], "w2")
    conn.execute("UPDATE job_effects SET attempt = 1")
    results = check(conn, [created(job)])
    assert failing(results) == {"I6"}
    assert results["I6"].examples == [str(job["id"])]


def test_i7_acknowledged_cancel_undone(conn):
    job = succeeded_job(conn, "k")
    assert failing(check(conn, [created(job), cancel(job, 200)])) == {"I7"}


def test_i7_cancelled_without_a_cancel(conn):
    job = new_job(conn, "k")
    repo.cancel_job(conn, job["id"])
    assert failing(check(conn, [created(job)])) == {"I7"}


def test_i8_double_claim(conn):
    # M1: a second claim of a running job is logged running -> running by the trigger.
    job = new_job(conn, "k")
    repo.claim(conn, "w1")
    conn.execute("UPDATE jobs SET status = 'running', attempt = attempt + 1, worker_id = 'w2'")
    assert repo.complete(conn, job["id"], 2, "w2")
    results = check(conn, [created(job)])
    assert failing(results) == {"I8"}
    assert results["I8"].examples == [str(job["id"])]


def test_i8_claim_without_preceding_requeue(conn):
    # All edges legal, but attempt 1 is claimed twice: claims must be 1..n.
    job = succeeded_job(conn, "k")
    conn.execute(
        "INSERT INTO job_transitions (job_id, from_status, to_status, attempt) VALUES"
        " (%(id)s, 'running', 'queued', 1), (%(id)s, 'queued', 'running', 1)",
        {"id": job["id"]},
    )
    assert "I8" in failing(check(conn, [created(job)]))


def test_i8_terminal_state_left(conn):
    job = new_job(conn, "k")
    repo.cancel_job(conn, job["id"])
    conn.execute(
        "INSERT INTO job_transitions (job_id, from_status, to_status, attempt)"
        " VALUES (%s, 'cancelled', 'queued', 0)",
        (job["id"],),
    )
    assert failing(check(conn, [created(job), cancel(job)])) == {"I8"}
