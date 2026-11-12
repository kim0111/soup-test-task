"""Chaos harness (SPEC §8): real processes, real kills, then invariants I1–I8.

    docker compose stop workers
    docker compose --profile chaos run --rm chaos --seed 1 [--zombie] [--jobs 1000]
    docker compose --profile chaos run --rm -e MUTATION=M1 chaos --seed 1

The harness starts its own API (uvicorn) and N workers as child processes, so all of them run the
same code and the same MUTATION; the compose `api` container is not used. Actors run at once:
  submitter  every key POSTed 1–3 times (partly concurrent, partly delayed), ~1 % with a second body
  canceller  ~10 % of keys, ~30 % of those aimed at long jobs while they run
  killer     SIGKILL a random worker every 2 ± 0.5 s and respawn it, until the queue is empty
  zombie     (--zombie) SIGSTOP a busy worker for lease + 1–3 s, then SIGCONT, every ~10 s
Then wait ≤ 60 s for queued + running = 0 (no final reap: it could hide a stuck job) and check.

The workload is a pure function of --seed (make_plan). The actors' choices come from RNGs derived
from the seed, but which keys/workers exist at a given moment depends on timing, so two runs with
one seed are similar, not identical.

Output in results/chaos/<run>/: report.txt (= stdout) and report.json, client.jsonl (every HTTP
answer, the invariants' input), harness.log, api.log, workers/*.log. Every run, PASS or FAIL, adds
a row to results/chaos/INDEX.md. Results are never edited afterwards.
"""

import argparse
import hashlib
import json
import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

from app import config, db, repo
from chaos import invariants

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results" / "chaos"
API_URL = "http://127.0.0.1:8000"

# Workload mix (SPEC §8.1).
LONG_SHARE = 0.05  # 2–5 s jobs: heartbeats, mid-run kills and cancels happen
FAIL_SHARE = 0.03
OTHER_BODY_SHARE = 0.01  # keys that also get one copy with a different body → 409
CONCURRENT_COPY_SHARE = 0.5  # a repeat POST goes out together with the first, else 0.1–5 s later
CANCEL_SHARE = 0.10
TARGETED_CANCEL_SHARE = 0.3  # cancels aimed at a long job while it is running

KILL_EVERY = (1.5, 2.5)
ZOMBIE_EVERY = (8.0, 12.0)
ZOMBIE_EXTRA = (1.0, 3.0)  # frozen for lease + this
DRAIN_TIMEOUT = 60.0
STALL_TIMEOUT = 120.0  # queue not shrinking for this long while waiting for it to empty → give up
CLIENT_TIMEOUT = 10.0
TRIES = 3  # a client retries a 503 or a lost answer, with the same key (idempotency at work)


# --- the plan: a pure function of the seed -----------------------------------------------------

@dataclass(frozen=True)
class Submit:
    at: float  # seconds after start
    key: str
    body: dict


@dataclass(frozen=True)
class Cancel:
    at: float
    running_long: bool  # aim at a long job that is running now; else any key created so far


@dataclass(frozen=True)
class Plan:
    submits: list[Submit]
    cancels: list[Cancel]


def make_plan(seed: int, jobs: int, rate: float) -> Plan:
    rng = random.Random(seed)
    submits = []

    def later(t0: float) -> float:
        return t0 if rng.random() < CONCURRENT_COPY_SHARE else t0 + rng.uniform(0.1, 5.0)

    for k in range(jobs):
        key, t0 = f"k{k:05d}", k / rate
        d = rng.randint(2000, 5000) if rng.random() < LONG_SHARE else rng.randint(20, 200)
        body = {"duration_ms": d, "fail": rng.random() < FAIL_SHARE}
        submits.append(Submit(t0, key, body))
        submits += [Submit(later(t0), key, body) for _ in range(rng.randint(1, 3) - 1)]
        if rng.random() < OTHER_BODY_SHARE:
            other = {**body, "duration_ms": d - 1 if d in (200, 5000) else d + 1}
            submits.append(Submit(later(t0), key, other))
    submits.sort(key=lambda s: s.at)

    window = jobs / rate + 5.0
    cancels = sorted(
        (Cancel(rng.uniform(1.0, window), rng.random() < TARGETED_CANCEL_SHARE)
         for _ in range(round(jobs * CANCEL_SHARE))),
        key=lambda c: c.at,
    )
    return Plan(submits, cancels)


# --- the run -------------------------------------------------------------------------------------

@dataclass
class Worker:
    proc: subprocess.Popen
    worker_id: str
    gen: int


class Abort(Exception):
    """Preflight refused to start; nothing was changed."""


def git_commit() -> str:
    """HEAD from a read-only .git mount (no git binary in the image). Working-tree state unknown."""
    git = ROOT / ".git"
    try:
        head = (git / "HEAD").read_text().strip()
        if not head.startswith("ref: "):
            return head
        ref = head[5:]
        if (git / ref).exists():
            return (git / ref).read_text().strip()
        for line in (git / "packed-refs").read_text().splitlines():
            if line.endswith(" " + ref):
                return line.split()[0]
    except OSError:
        pass
    return "unknown"


def code_hash() -> str:
    """sha256 over the code a run executes. The commit alone can't say whether the tree was dirty."""
    h = hashlib.sha256()
    for f in sorted([*ROOT.glob("app/**/*.py"), *ROOT.glob("chaos/**/*.py"),
                     *ROOT.glob("migrations/*.sql")]):
        h.update(f.relative_to(ROOT).as_posix().encode() + b"\0" + f.read_bytes() + b"\0")
    return h.hexdigest()[:12]


EXPECTED = {"create": {200, 201, 409, 503, None}, "cancel": {200, 409, 503, None}}
UNACKED_TOLERANCE = 0.01


def gates(*, requests: Counter, jobs: int, checked: int, kills_busy: int, re_executions: int,
          running_cancels: int, zombie: bool, zombie_chances: int) -> tuple[list[str], list[str]]:
    """(failures, gaps). The invariants judge what happened; these say whether enough happened.
    A failure is an answer no correct system gives (a 500, a 404 for an id it handed out): the
    run FAILs. A gap is a run that never exercised something it claims to test: INCOMPLETE,
    never PASS (e.g. --zombie with no zombie that could have written late)."""
    failures = [f"{op} {st} ×{n}" for (op, st), n in requests.items() if st not in EXPECTED[op]]
    gaps = []
    if jobs - checked > jobs * UNACKED_TOLERANCE:
        gaps.append(f"{jobs - checked} of {jobs} keys never acknowledged (I1 can't check them)")
    if kills_busy == 0:
        gaps.append("no kill hit a busy worker")
    if re_executions == 0:
        gaps.append("no re-execution")
    if running_cancels == 0:
        gaps.append("no cancel of a running job")
    if zombie and zombie_chances == 0:
        gaps.append("--zombie: no zombie woke up next to a newer running attempt")
    return failures, gaps


class Run:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.mutation = config.MUTATION
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")  # a name, not lease logic
        self.name = (f"{stamp}-s{args.seed}" + ("-z" if args.zombie else "")
                     + (f"-{self.mutation}" if self.mutation else "")
                     + (f"-j{args.jobs}" if args.jobs != 10_000 else ""))
        self.dir = RESULTS / self.name
        self.hlog = self.clog = None  # opened after preflight: an aborted start leaves no files
        self.t0 = time.monotonic()
        self.lock = threading.Lock()
        self.records: list[dict] = []
        self.known: list[tuple[str, str]] = []  # (key, id) of every key with a 2xx, in order
        self.known_keys: set[str] = set()
        self.workers: dict[int, Worker] = {}
        self.frozen: set[int] = set()  # slots under SIGSTOP; the killer leaves them alone
        # Jobs held by a frozen worker. Cancellers leave them alone, and the killer leaves alone
        # whoever re-claims them: either would take away the zombie's chance to write late (M2).
        self.frozen_jobs: set[str] = set()
        self.kills: list[dict] = []
        self.zombies: list[dict] = []
        self.errors = 0
        self.api: subprocess.Popen | None = None
        self.submitted = threading.Event()
        self.stop_chaos = threading.Event()
        self._local = threading.local()
        self._clients: list[httpx.Client] = []

    @property
    def http(self) -> httpx.Client:
        """One client per thread: no assumptions about sharing one across threads."""
        if (c := getattr(self._local, "client", None)) is None:
            c = self._local.client = httpx.Client(base_url=API_URL, timeout=CLIENT_TIMEOUT)
            with self.lock:
                self._clients.append(c)
        return c

    # -- logging ----------------------------------------------------------------------------------

    def now(self) -> float:
        return round(time.monotonic() - self.t0, 3)

    def say(self, msg: str) -> None:
        line = f"[{self.now():8.2f}] {msg}"
        print(line, flush=True)
        if self.hlog is not None:
            self.hlog.write(line + "\n")

    def record(self, rec: dict) -> None:
        with self.lock:
            self.records.append(rec)
            self.clog.write(json.dumps(rec) + "\n")

    def guarded(self, name: str, fn, *a) -> None:
        """A harness bug must not pass silently: count it, log it, show it in the report."""
        try:
            fn(*a)
        except Exception:
            with self.lock:
                self.errors += 1
            self.say(f"HARNESS ERROR in {name}:\n{traceback.format_exc()}")

    def thread(self, name: str, fn, *a) -> threading.Thread:
        t = threading.Thread(target=self.guarded, args=(name, fn, *a), name=name, daemon=True)
        t.start()
        return t

    # -- processes --------------------------------------------------------------------------------

    def open_dir(self) -> None:
        (self.dir / "workers").mkdir(parents=True)
        self.hlog = open(self.dir / "harness.log", "w", encoding="utf-8", buffering=1)
        self.clog = open(self.dir / "client.jsonl", "w", encoding="utf-8", buffering=1)

    def preflight(self) -> str:
        with db.connect("worker", "chaos-preflight") as c:
            others = c.execute(
                "SELECT application_name FROM pg_stat_activity WHERE application_name LIKE 'worker-%'"
            ).fetchall()
            if others:
                names = ", ".join(sorted(r["application_name"] for r in others))
                raise Abort(f"workers the harness didn't spawn are connected ({names}); "
                            "run `docker compose stop workers` first")
            c.execute("TRUNCATE jobs, job_effects, job_transitions RESTART IDENTITY")
            repo.sync_schema_with_mutation(c)  # after TRUNCATE: re-adding the key fails on dups
            return c.execute("SHOW transaction_isolation").fetchone()["transaction_isolation"]

    def start_api(self) -> None:
        out = open(self.dir / "api.log", "w", encoding="utf-8")
        self.api = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.api:app", "--host", "127.0.0.1",
             "--port", "8000", "--no-access-log"],
            stdout=out, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        out.close()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if self.http.get("/healthz").status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
        raise RuntimeError("API did not come up within 20 s (see api.log)")

    def spawn(self, slot: int) -> None:
        """Caller holds self.lock (or nothing else runs yet)."""
        gen = self.workers[slot].gen + 1 if slot in self.workers else 0
        out = open(self.dir / "workers" / f"w{slot}-g{gen}.log", "w", encoding="utf-8")
        proc = subprocess.Popen(
            [sys.executable, "-m", "app.worker"],
            stdout=out, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        out.close()
        self.workers[slot] = Worker(proc, f"worker-{socket.gethostname()}-{proc.pid}", gen)

    def stop_children(self, graceful: bool) -> None:
        procs = [w.proc for w in self.workers.values()]
        for p in procs:
            if p.poll() is None:
                try:
                    os.kill(p.pid, signal.SIGCONT)  # a frozen worker can't handle SIGTERM
                    p.send_signal(signal.SIGTERM if graceful else signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        if self.api is not None and self.api.poll() is None:
            self.api.terminate()
            try:
                self.api.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.api.kill()

    # -- client -----------------------------------------------------------------------------------

    def call(self, op: str, method: str, url: str, rec: dict, **kw) -> tuple[int | None, dict]:
        """One logged record per try. Retries a 503 or a lost answer, like a real client would."""
        status, body = None, {}
        for n in range(1, TRIES + 1):
            status, body, error = None, {}, None
            try:
                r = self.http.request(method, url, **kw)
                status = r.status_code
                body = r.json()
            except httpx.HTTPError as e:
                error = type(e).__name__
            except ValueError:
                error = "bad json"
            self.record({"t": self.now(), "op": op, **rec, "status": status, "try": n,
                         "error": error, **({"id": body["id"]} if "id" in body else {})})
            if status is not None and status != 503:
                break
            time.sleep(0.5 * n)
        return status, body

    def create(self, s: Submit) -> None:
        status, body = self.call("create", "POST", "/jobs", {"key": s.key, "body": s.body, "id": None},
                                 json=s.body, headers={"Idempotency-Key": s.key})
        if status is not None and 200 <= status < 300:
            with self.lock:
                if s.key not in self.known_keys:
                    self.known_keys.add(s.key)
                    self.known.append((s.key, body["id"]))

    def cancel(self, key: str, job_id: str) -> None:
        self.call("cancel", "POST", f"/jobs/{job_id}/cancel", {"key": key, "id": job_id})

    # -- actors -----------------------------------------------------------------------------------

    def wait_until(self, at: float) -> None:
        delay = at - (time.monotonic() - self.t0)
        if delay > 0:
            time.sleep(delay)

    def submitter(self, plan: Plan) -> None:
        with ThreadPoolExecutor(64, thread_name_prefix="submit") as pool:
            for s in plan.submits:
                self.wait_until(s.at)
                pool.submit(self.guarded, "create", self.create, s)
        self.say(f"submitter done: {len(plan.submits)} POSTs for {self.args.jobs} keys")
        self.submitted.set()

    def canceller(self, plan: Plan) -> None:
        rng = random.Random(f"{self.args.seed}-cancel")
        done: set[str] = set()
        with db.connect("worker", "chaos-canceller") as c, \
                ThreadPoolExecutor(16, thread_name_prefix="cancel") as pool:
            for p in plan.cancels:
                self.wait_until(p.at)
                target = None
                if p.running_long:
                    rows = [r for r in c.execute(
                        "SELECT idempotency_key AS key, id::text AS id FROM jobs"
                        " WHERE status = 'running' AND (payload->>'duration_ms')::int >= 2000"
                        # Live owner only (heartbeat ≤ 1.5 s ago). Orphans of killed workers
                        # look the same otherwise, and cancelling them pre-empts the
                        # kill → reap → re-run path this run exists to exercise.
                        "   AND lease_expires_at > now() + make_interval(secs => %s)"
                        " ORDER BY id", (config.LEASE_SECONDS - 1.5,)
                    ) if r["key"] not in done and r["id"] not in self.frozen_jobs]
                    if rows:
                        r = rng.choice(rows)
                        target = (r["key"], r["id"])
                if target is None:
                    # Recent keys only (~12 s at 40/s): a cancel should mostly meet a queued or
                    # running job, not one that finished minutes ago (a plain 409).
                    with self.lock:
                        pool_ = [k for k in self.known[-500:]
                                 if k[0] not in done and k[1] not in self.frozen_jobs]
                    if not pool_:
                        continue
                    target = rng.choice(pool_)
                done.add(target[0])
                pool.submit(self.guarded, "cancel", self.cancel, *target)
        self.say(f"canceller done: {len(done)} keys cancelled")

    def killer(self) -> None:
        rng = random.Random(f"{self.args.seed}-killer")
        with db.connect("worker", "chaos-killer") as c:
            while not self.stop_chaos.wait(rng.uniform(*KILL_EVERY)):
                with self.lock:
                    frozen_jobs = list(self.frozen_jobs)
                owners = {r["worker_id"] for r in c.execute(
                    "SELECT worker_id FROM jobs WHERE status = 'running' AND id::text = ANY(%s)",
                    (frozen_jobs,))} if frozen_jobs else set()
                with self.lock:
                    slots = sorted(s for s in set(self.workers) - self.frozen
                                   if self.workers[s].worker_id not in owners)
                    if not slots:
                        continue
                    slot = rng.choice(slots)
                    w = self.workers[slot]
                    w.proc.kill()
                    w.proc.wait()
                    # Dead process, fresh lease: what it held is still `running` under its id.
                    held = c.execute(
                        "SELECT id::text AS id, attempt FROM jobs"
                        " WHERE status = 'running' AND worker_id = %s", (w.worker_id,)
                    ).fetchone()
                    self.spawn(slot)
                self.kills.append({"t": self.now(), "slot": slot, "worker": w.worker_id,
                                   "job": held and held["id"], "attempt": held and held["attempt"]})
                self.say(f"kill w{slot} {w.worker_id}" + (f" holding {held['id']} a={held['attempt']}"
                                                          if held else " idle"))

    def zombie(self) -> None:
        rng = random.Random(f"{self.args.seed}-zombie")
        with db.connect("worker", "chaos-zombie") as c:
            next_at = time.monotonic() + rng.uniform(*ZOMBIE_EVERY)
            while not self.stop_chaos.wait(max(0.0, next_at - time.monotonic())):
                next_at = time.monotonic() + rng.uniform(*ZOMBIE_EVERY)  # freeze included
                running = c.execute(
                    "SELECT worker_id, (payload->>'duration_ms')::int >= 2000 AS long FROM jobs"
                    " WHERE status = 'running' ORDER BY worker_id"
                ).fetchall()
                with self.lock:
                    by_id = {w.worker_id: s for s, w in self.workers.items() if s not in self.frozen}
                    busy = [(by_id[r["worker_id"]], r["long"]) for r in running if r["worker_id"] in by_id]
                    if not busy:
                        self.say("zombie: no busy worker")
                        continue
                    pick = [s for s, long in busy if long] or [s for s, _ in busy]
                    slot = rng.choice(pick)
                    w = self.workers[slot]
                    os.kill(w.proc.pid, signal.SIGSTOP)
                    self.frozen.add(slot)
                # Stopped: what it holds can't change under it any more (except by the reaper).
                held = c.execute(
                    "SELECT id::text AS id, attempt FROM jobs"
                    " WHERE status = 'running' AND worker_id = %s", (w.worker_id,)
                ).fetchone()
                if held:
                    with self.lock:
                        self.frozen_jobs.add(held["id"])
                freeze = config.LEASE_SECONDS + rng.uniform(*ZOMBIE_EXTRA)
                self.say(f"zombie: SIGSTOP w{slot} {w.worker_id} for {freeze:.1f}s"
                         + (f" holding {held['id']} a={held['attempt']}" if held else " (job done)"))
                time.sleep(freeze)  # not interruptible: always resume what we froze
                after = held and c.execute(
                    "SELECT status, attempt FROM jobs WHERE id = %s", (held["id"],)
                ).fetchone()
                with self.lock:
                    os.kill(w.proc.pid, signal.SIGCONT)
                    self.frozen.discard(slot)
                    if held:
                        self.frozen_jobs.discard(held["id"])
                # A broken fence (M2) can only show if a newer attempt is running when it wakes.
                chance = bool(after and after["status"] == "running"
                              and after["attempt"] > held["attempt"])
                self.zombies.append({"t": self.now(), "slot": slot, "worker": w.worker_id,
                                     "job": held and held["id"], "attempt": held and held["attempt"],
                                     "freeze_s": round(freeze, 2), "after": after, "chance": chance})
                self.say(f"zombie: SIGCONT w{slot}" + (f", job now {after['status']} a={after['attempt']}"
                                                       if after else ""))

    # -- main -------------------------------------------------------------------------------------

    def counts(self, c) -> dict:
        return c.execute(
            "SELECT count(*) FILTER (WHERE status = 'queued') AS queued,"
            "       count(*) FILTER (WHERE status = 'running') AS running FROM jobs"
        ).fetchone()

    def execute(self) -> int:
        a = self.args
        isolation = self.preflight()
        self.open_dir()
        header = {
            "run": self.name, "seed": a.seed, "mutation": self.mutation or None, "zombie": a.zombie,
            "jobs": a.jobs, "workers": a.workers, "rate": a.rate, "commit": git_commit(),
            "code": code_hash(),
            "isolation": isolation,
            "config": {"lease_s": config.LEASE_SECONDS, "heartbeat_s": config.HEARTBEAT_SECONDS,
                       "reap_s": config.REAP_SECONDS, "max_attempts": config.MAX_ATTEMPTS},
        }
        self.say("header " + json.dumps(header))
        plan = make_plan(a.seed, a.jobs, a.rate)
        drained, drain_s = False, None
        chaos: list[threading.Thread] = []
        try:
            self.start_api()
            with self.lock:
                for slot in range(a.workers):
                    self.spawn(slot)
            self.t0 = time.monotonic()  # plan times count from here
            actors = [self.thread("submitter", self.submitter, plan),
                      self.thread("canceller", self.canceller, plan)]
            chaos.append(self.thread("killer", self.killer))
            if a.zombie:
                chaos.append(self.thread("zombie", self.zombie))
            for t in actors:
                t.join()

            with db.connect("worker", "chaos-monitor") as c:
                # The killer runs until nothing is left to start.
                best, since = None, time.monotonic()
                while (q := self.counts(c)["queued"]) > 0:
                    if best is None or q < best:
                        best, since = q, time.monotonic()
                    elif time.monotonic() - since > STALL_TIMEOUT:
                        self.say(f"queue stuck at {q} for {STALL_TIMEOUT:.0f}s; stopping chaos")
                        break
                    time.sleep(1)
                self.stop_chaos.set()
                for t in chaos:
                    t.join()
                self.say(f"chaos stopped: {len(self.kills)} kills, {len(self.zombies)} zombies")

                start = time.monotonic()
                while time.monotonic() - start < DRAIN_TIMEOUT:
                    n = self.counts(c)
                    if n["queued"] + n["running"] == 0:
                        drained, drain_s = True, round(time.monotonic() - start, 1)
                        break
                    time.sleep(0.5)
                self.say(f"drain: {'done in %.1fs' % drain_s if drained else 'TIMEOUT'} {self.counts(c)}")
        finally:
            # Also on a crash: no killer respawning workers while they are being stopped.
            self.stop_chaos.set()
            for t in chaos:
                t.join(timeout=15)
            # Drained: let workers exit cleanly. Not drained: freeze the state as it is.
            self.stop_children(graceful=drained)
            for client in self._clients:
                client.close()
        return self.report(header, drained, drain_s)

    def report(self, header: dict, drained: bool, drain_s: float | None) -> int:
        with db.connect("worker", "chaos-report") as c:
            snap = invariants.load(c)
        results = invariants.check_snapshot(snap, self.records, config.MAX_ATTEMPTS)
        claims = [len(invariants.claims(h)) for h in snap.transitions.values()]
        by_op = Counter((r["op"], r["status"]) for r in self.records)
        cancels_by_from = Counter(f for h in snap.transitions.values()
                                  for f, to, _ in h if to == "cancelled")
        failures, gaps = gates(
            requests=by_op, jobs=self.args.jobs, checked=results[0].info["checked"],
            kills_busy=sum(1 for k in self.kills if k["job"]),
            re_executions=sum(max(0, n - 1) for n in claims),
            running_cancels=cancels_by_from.get("running", 0), zombie=self.args.zombie,
            zombie_chances=sum(1 for z in self.zombies if z["chance"]),
        )
        if failures or self.errors or not all(r.ok for r in results):
            verdict = "FAIL"
        else:
            verdict = "INCOMPLETE" if gaps else "PASS"
        rep = {
            **header,
            "wall_s": self.now(),
            "drained": drained, "drain_s": drain_s,
            "requests": {f"{op} {st}": n for (op, st), n in sorted(by_op.items(), key=str)},
            "final_status": dict(Counter(j["status"] for j in snap.jobs.values())),
            "kills": len(self.kills),
            "kills_busy": sum(1 for k in self.kills if k["job"]),
            "zombies": len(self.zombies),
            "zombies_holding_job": sum(1 for z in self.zombies if z["job"]),
            "zombie_chances": sum(1 for z in self.zombies if z["chance"]),
            "re_executions": sum(max(0, n - 1) for n in claims),
            "exhausted": results[0].info["exhausted"],
            "cancels_by_from": dict(cancels_by_from),
            "conflicts_409": by_op[("create", 409)],
            "harness_errors": self.errors,
            "unexpected_answers": failures,
            "coverage_gaps": gaps,
            "invariants": [asdict(r) for r in results],
            "verdict": verdict,
        }
        (self.dir / "report.json").write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")

        lines = [f"run {self.name}  verdict {verdict}"]
        lines += [f"  {k}: {rep[k]}" for k in rep if k not in ("invariants", "verdict", "run")]
        for r in results:
            mark = "PASS" if r.ok else "FAIL"
            ex = f" e.g. {', '.join(r.examples)}" if r.examples else ""
            lines.append(f"  {r.id} {mark:4} {r.title}: {r.count} violations{ex} {r.info or ''}")
        text = "\n".join(lines)
        print(text, flush=True)
        (self.dir / "report.txt").write_text(text + "\n", encoding="utf-8")
        self.index_row(rep, results)
        return 0 if verdict == "PASS" else 1

    def index_row(self, rep: dict, results: list[invariants.Result]) -> None:
        index = RESULTS / "INDEX.md"
        if not index.exists():
            index.write_text(
                "# Chaos runs\n\nEvery run, including failed ones, appended by the harness. "
                "Never edited; notes go in the last column only.\n\n"
                "| run | seed | mutation | zombie | jobs | verdict | failed / gaps | wall s "
                "| kills (busy) | re-exec | exhausted | notes | code |\n"
                "|---|---|---|---|---|---|---|---|---|---|---|---|---|\n",
                encoding="utf-8",
            )
        failed = [f"{r.id}({r.count})" for r in results if not r.ok]
        if rep["harness_errors"]:
            failed.append(f"harness_errors({rep['harness_errors']})")
        failed += rep["unexpected_answers"] + rep["coverage_gaps"]
        self.append_index(
            f"| {rep['run']} | {rep['seed']} | {rep['mutation'] or '-'} "
            f"| {'yes' if rep['zombie'] else '-'} | {rep['jobs']} | {rep['verdict']} "
            f"| {'; '.join(failed) or '-'} | {rep['wall_s']:.0f} | {rep['kills']} ({rep['kills_busy']}) "
            f"| {rep['re_executions']} | {rep['exhausted']} | | {rep['code']} |"
        )

    def crashed(self, exc: BaseException) -> None:
        """A run that died still gets its row (SPEC §8.3: every run is listed)."""
        if self.hlog is None:
            return  # died before the run dir existed: nothing ran
        self.say(f"CRASH: {exc!r}")
        self.say(traceback.format_exc())
        self.append_index(
            f"| {self.name} | {self.args.seed} | {self.mutation or '-'} "
            f"| {'yes' if self.args.zombie else '-'} | {self.args.jobs} | ERROR "
            f"| {type(exc).__name__} (see harness.log) | {self.now():.0f} | {len(self.kills)} | | | "
            f"| {code_hash()} |"
        )

    def append_index(self, row: str) -> None:
        with (RESULTS / "INDEX.md").open("a", encoding="utf-8") as f:
            f.write(row + "\n")


def interrupted(*_) -> None:
    raise KeyboardInterrupt


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--jobs", type=int, default=10_000)
    p.add_argument("--workers", type=int, default=8)
    # 8 workers finish ~28 jobs/s of this mix: 40/s builds a backlog that drains after the
    # submitter stops, so cancels meet queued jobs and claims compete for a real queue.
    p.add_argument("--rate", type=float, default=40.0, help="new keys per second")
    p.add_argument("--zombie", action="store_true")
    args = p.parse_args(argv)
    # docker stop / Ctrl+C: unwind through execute()'s finally (stop actors and children).
    signal.signal(signal.SIGTERM, interrupted)
    run = Run(args)
    try:
        return run.execute()
    except Abort as e:
        run.say(f"ABORT: {e}")
        return 2
    except BaseException as e:
        run.crashed(e)
        # Children are stopped and the row is written. Exit now: the actors' thread pools are
        # non-daemon and would keep retrying against the dead API until docker SIGKILLs us.
        sys.stdout.flush()
        os._exit(3)


if __name__ == "__main__":
    sys.exit(main())
