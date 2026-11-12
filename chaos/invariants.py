"""Invariants I1–I8 (SPEC §8.2), checked against the database AND the client log.

A DB-only check can't see a lost 201 or an acknowledged cancel that was undone, so every check that
is about what a client was told reads the log the harness wrote.

Client log records (one dict per HTTP response; extra fields are ignored here):
    {"op": "create", "key": str, "body": dict, "status": int | None, "id": str | None}
    {"op": "cancel", "key": str, "id": str, "status": int | None}
`status` None = no answer (timeout, connection error): the outcome is unknown. The harness always
sends complete bodies (`duration_ms` and `fail`), so a body compares equal to the stored payload.

Read-only: one snapshot of the three tables, no writes.
"""

from collections import defaultdict
from dataclasses import dataclass, field

EXHAUSTED = "lease expired; attempts exhausted"  # the reaper's error text (app/repo.py reap)

# Every edge the state machine allows (SPEC §3); None = the INSERT.
LEGAL_EDGES = {
    (None, "queued"),
    ("queued", "running"),
    ("running", "queued"),
    ("running", "succeeded"),
    ("running", "failed"),
    ("queued", "cancelled"),
    ("running", "cancelled"),
}
EXAMPLES = 5


@dataclass
class Result:
    id: str
    title: str
    ok: bool
    count: int  # violations
    examples: list[str]  # up to EXAMPLES job ids or keys
    info: dict = field(default_factory=dict)


@dataclass
class Snapshot:
    jobs: dict[str, dict]  # id → row
    by_key: dict[str, list[dict]]  # idempotency_key → rows (more than one only without the index)
    effects: dict[str, list[dict]]  # job id → effects, in insert order
    transitions: dict[str, list[tuple]]  # job id → [(from, to, attempt)], in seq order


def load(conn) -> Snapshot:
    jobs, by_key = {}, defaultdict(list)
    for r in conn.execute("SELECT * FROM jobs"):
        r["id"] = str(r["id"])
        jobs[r["id"]] = r
        by_key[r["idempotency_key"]].append(r)
    effects = defaultdict(list)
    for r in conn.execute("SELECT job_id, attempt, worker_id FROM job_effects ORDER BY id"):
        effects[str(r["job_id"])].append(r)
    transitions = defaultdict(list)
    for r in conn.execute(
        "SELECT job_id, from_status, to_status, attempt FROM job_transitions ORDER BY seq"
    ):
        transitions[str(r["job_id"])].append((r["from_status"], r["to_status"], r["attempt"]))
    return Snapshot(jobs, by_key, effects, transitions)


def claims(history: list[tuple]) -> list[int]:
    """Indexes of the claim transitions (anything that enters `running`)."""
    return [i for i, (_, to, _) in enumerate(history) if to == "running"]


def exhausted_legally(history: list[tuple], max_attempts: int) -> bool:
    """Exactly max_attempts claims, each ended by the reaper: requeued, the last one failed."""
    idx = claims(history)
    if len(idx) != max_attempts:
        return False
    for n, i in enumerate(idx):
        expected = ("running", "failed" if n == len(idx) - 1 else "queued")
        if i + 1 >= len(history) or history[i + 1][:2] != expected:
            return False
    return True


def _result(id: str, title: str, bad: list[str], **info) -> Result:
    return Result(id, title, not bad, len(bad), bad[:EXAMPLES], info)


def _2xx(rec: dict) -> bool:
    return rec["status"] is not None and 200 <= rec["status"] < 300


def i1_no_lost_job(s: Snapshot, log: list[dict], max_attempts: int) -> Result:
    keys = sorted({r["key"] for r in log if r["op"] == "create" and _2xx(r)})
    bad, exhausted = [], 0
    for key in keys:
        rows = s.by_key.get(key, [])
        if len(rows) != 1:
            bad.append(key)
            continue
        job = rows[0]
        if job["status"] == "cancelled":
            continue  # I7's business
        if job["status"] == "failed" and job["error"] == EXHAUSTED:
            if exhausted_legally(s.transitions[job["id"]], max_attempts):
                exhausted += 1
            else:
                bad.append(key)
            continue
        if job["status"] != ("failed" if job["payload"]["fail"] else "succeeded"):
            bad.append(key)
    return _result("I1", "no lost job", bad, checked=len(keys), exhausted=exhausted)


def i2_no_duplicates(s: Snapshot, log: list[dict]) -> Result:
    creates = defaultdict(list)
    for r in log:
        if r["op"] == "create":
            creates[r["key"]].append(r)
    bad = []
    for key, recs in sorted(creates.items()):
        if len({r["id"] for r in recs if _2xx(r)}) > 1:
            bad.append(key)
            continue
        rows = s.by_key.get(key, [])
        if len(rows) != 1:
            if len(rows) > 1:
                bad.append(key)
            continue
        winner = rows[0]["payload"]
        for r in recs:
            same = r["body"] == winner
            if (same and r["status"] == 409) or (not same and _2xx(r)):
                bad.append(key)
                break
    return _result("I2", "no duplicates", bad, keys=len(creates))


def i3_none_stuck(s: Snapshot) -> Result:
    bad = sorted(j["id"] for j in s.jobs.values() if j["status"] in ("queued", "running"))
    return _result("I3", "none stuck", bad)


def i4_at_most_one_effect(s: Snapshot) -> Result:
    bad = sorted(job_id for job_id, e in s.effects.items() if len(e) > 1)
    return _result("I4", "at most one effect per job", bad)


def i5_succeeded_iff_one_effect(s: Snapshot) -> Result:
    bad = sorted(
        j["id"] for j in s.jobs.values()
        if (j["status"] == "succeeded") != (len(s.effects.get(j["id"], [])) == 1)
    )
    return _result("I5", "succeeded <=> exactly one effect", bad)


def i6_effect_attempt_is_final(s: Snapshot) -> Result:
    bad = sorted(
        job_id for job_id, effs in s.effects.items()
        if any(e["attempt"] != s.jobs[job_id]["attempt"] for e in effs)
    )
    return _result("I6", "effect attempt = final attempt", bad)


def i7_cancel_respected(s: Snapshot, log: list[dict]) -> Result:
    cancels = defaultdict(list)
    for r in log:
        if r["op"] == "cancel":
            cancels[r["id"]].append(r)
    bad = []
    for j in s.jobs.values():
        sent = cancels.get(j["id"], [])
        acknowledged = any(r["status"] == 200 for r in sent)
        if (acknowledged and j["status"] != "cancelled") or (j["status"] == "cancelled" and not sent):
            bad.append(j["id"])
    return _result("I7", "cancel respected", sorted(bad))


def i8_legal_history(s: Snapshot) -> Result:
    bad = []
    for job_id, history in s.transitions.items():
        legal = all((frm, to) in LEGAL_EDGES for frm, to, _ in history)
        idx = claims(history)
        numbered = [history[i][2] for i in idx] == list(range(1, len(idx) + 1))
        requeued = all(history[i - 1][:2] == ("running", "queued") for i in idx[1:])
        if not (legal and numbered and requeued):
            bad.append(job_id)
    return _result("I8", "legal history, no double claim", sorted(bad))


def check(conn, log: list[dict], max_attempts: int) -> list[Result]:
    return check_snapshot(load(conn), log, max_attempts)


def check_snapshot(s: Snapshot, log: list[dict], max_attempts: int) -> list[Result]:
    return [
        i1_no_lost_job(s, log, max_attempts),
        i2_no_duplicates(s, log),
        i3_none_stuck(s),
        i4_at_most_one_effect(s),
        i5_succeeded_iff_one_effect(s),
        i6_effect_attempt_is_final(s),
        i7_cancel_respected(s, log),
        i8_legal_history(s),
    ]
