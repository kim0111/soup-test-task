"""Load test at 100 rps (SPEC §10). The criteria are in Docs/VERDICT.md, committed before any run.

    docker compose up -d --build db api
    docker compose stop workers
    docker compose --profile load run --rm load --mix short --seed 1     # L1, the verdict run
    docker compose --profile load run --rm load --mix chaos --seed 1     # L2, capacity probe

Open loop: request i goes out at t0 + i/rps whatever earlier answers do, and its latency is
measured from that scheduled time, so a stalled API can't slow the generator down and hide its own
delay (no coordinated omission). Client timeout 2 s, no retries by the generator: the ~10 %
retried keys are part of the plan.

Mix per request: 60 % GET /jobs/{id} (any job acknowledged so far), 25 % POST /jobs (~10 % repeat
a recent key with the same body), 10 % GET /jobs?limit=50 (half with status=queued), 5 % cancel
(one of the last few jobs acknowledged: a user cancelling what they just submitted).

System: the compose `api` and `db`. The load test spawns its own 4 workers (compose workers must be
stopped: they have no restart policy, so a kill -9 would leave them dead) and kill -9s a random one
every 4 ± 1 s, respawned at once. Workers and this process run the image's code, as `api` does.

Output in results/load/<run>/: report.json / report.txt, load.log, queue.csv (1 Hz),
pgstat.jsonl (every 10 s), client.jsonl.gz, workers/*.log.gz. Every run, whatever its verdict,
appends a row to results/load/INDEX.md; only the notes column is edited by hand.
"""

import argparse
import asyncio
import json
import math
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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

from app import config, db, repo
from chaos import invariants
from chaos.harness import Worker, code_hash, compress_logs, git_commit, interrupted

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results" / "load"

# Code hash of the Phase 8 chaos matrix (results/chaos/INDEX.md). VERDICT C1: the load runs must
# execute exactly that code.
MATRIX_CODE = "30c76023c67a"

RPS = 100
DURATION = 300.0
WORKERS = 4
KILL_EVERY = (3.0, 5.0)  # the chaos per-worker rate: 8 workers / 2 s there, 4 workers / 4 s here

# Request mix (SPEC §10).
GET_SHARE, CREATE_SHARE, LIST_SHARE = 0.60, 0.25, 0.10  # the rest (5 %) cancels
RETRY_SHARE = 0.10  # creates that repeat a recent key with the same body
RETRY_WINDOW = 50  # ... one of the last 50 new keys (~2 s)
CANCEL_RECENT = 20  # cancel one of the last 20 acknowledged jobs (~1 s): queued, running or done
LIST_LIMIT = 50

# Job bodies. short = L1 (the verdict run), chaos = L2 (the chaos harness's mix).
MIXES = ("short", "chaos")
LONG_SHARE = 0.05
FAIL_SHARE = 0.03

# Criteria (Docs/VERDICT.md).
OPS = ("create", "get", "list", "cancel")
EXPECTED = {"create": {200, 201}, "get": {200}, "list": {200}, "cancel": {200, 409}}
TIMEOUT = 2.0
WARMUP = 30.0
ERROR_LIMIT = 0.001
P99_LIMIT_MS = 200.0
QUEUE_BOUND = 100
DRAIN_LIMIT = 60.0
ON_TIME_LAG = 0.05
ON_TIME_SHARE = 0.99
DRAIN_WAIT = {"short": 60.0, "chaos": 600.0}  # how long to wait for the queue to empty
PGSTAT_EVERY = 10.0


# --- the plan: a pure function of the seed -----------------------------------------------------

@dataclass(frozen=True)
class Req:
    at: float  # seconds after t0
    op: str  # create | get | list | cancel
    key: str | None = None
    body: dict | None = None
    retry: bool = False  # a create that repeats an earlier key with the same body
    params: dict | None = None  # list query
    u: float = 0.0  # get / cancel: picks the target among acknowledged jobs at send time


def job_body(rng: random.Random, mix: str) -> dict:
    long = mix == "chaos" and rng.random() < LONG_SHARE
    return {"duration_ms": rng.randint(2000, 5000) if long else rng.randint(20, 200),
            "fail": rng.random() < FAIL_SHARE}


def make_plan(seed: int, *, rps: float = RPS, duration: float = DURATION, mix: str = "short") -> list[Req]:
    if mix not in MIXES:
        raise ValueError(f"unknown mix {mix!r}")
    rng = random.Random(seed)
    plan: list[Req] = []
    keys: list[tuple[str, dict]] = []
    for i in range(round(rps * duration)):
        at, x = i / rps, rng.random()
        if x < GET_SHARE:
            plan.append(Req(at, "get", u=rng.random()))
        elif x < GET_SHARE + CREATE_SHARE:
            if keys and rng.random() < RETRY_SHARE:
                key, body = rng.choice(keys[-RETRY_WINDOW:])
                plan.append(Req(at, "create", key, body, retry=True))
            else:
                key, body = f"k{len(keys):06d}", job_body(rng, mix)
                keys.append((key, body))
                plan.append(Req(at, "create", key, body))
        elif x < GET_SHARE + CREATE_SHARE + LIST_SHARE:
            params = {"limit": LIST_LIMIT, **({"status": "queued"} if rng.random() < 0.5 else {})}
            plan.append(Req(at, "list", params=params))
        else:
            plan.append(Req(at, "cancel", u=rng.random()))
    return plan


# --- numbers and criteria: pure functions over the records ------------------------------------------

def is_error(op: str, status: int | None) -> bool:
    """VERDICT C2: anything but the expected answers, incl. 503, 404 and no answer at all."""
    return status not in EXPECTED[op]


def percentile(values: list[float], q: float) -> float:
    """Nearest rank: the smallest value with at least q of the values at or below it."""
    s = sorted(values)
    return s[max(0, math.ceil(round(q * len(s), 9)) - 1)]


def _ms(x: float) -> float:
    return round(x * 1000, 1)


def summarize(records: list[dict], *, warmup: float = WARMUP) -> dict:
    """Errors over the whole run; latency over requests scheduled at t ≥ warmup, measured from the
    scheduled time, with no answer counted as slower than any answer (inf)."""
    sent = [r for r in records if not r.get("skipped")]
    errors = sum(is_error(r["op"], r["status"]) for r in sent)
    endpoints = {}
    for op in OPS:
        lats = [math.inf if r["lat"] is None else r["lat"]
                for r in sent if r["op"] == op and r["t"] >= warmup]
        endpoints[op] = {
            "n": len(lats),
            "p50_ms": _ms(percentile(lats, 0.50)) if lats else None,
            "p99_ms": _ms(percentile(lats, 0.99)) if lats else None,
            "max_ms": _ms(max(lats)) if lats else None,
        }
    # Skipped requests (no acknowledged job to aim at yet) were never sent: they count as late.
    on_time = sum(r["lag"] <= ON_TIME_LAG for r in sent) / len(records) if records else 0.0
    return {
        "requests": len(sent),
        "skipped": len(records) - len(sent),
        "errors": errors,
        "error_rate": errors / len(sent) if sent else 1.0,
        "on_time": on_time,
        "lag_p99_ms": _ms(percentile([r["lag"] for r in sent], 0.99)) if sent else None,
        "by_status": dict(sorted(Counter(f"{r['op']} {r['status']}" for r in sent).items())),
        "endpoints": endpoints,
    }


def _row(id: str, name: str, ok: bool, value) -> dict:
    return {"id": id, "name": name, "ok": bool(ok), "value": value}


def criteria(s: dict, *, queue: list[tuple], drain_s: float | None, invariants_ok: bool, code: str,
             kills_busy: int, re_executions: int, warmup: float = WARMUP) -> tuple[str, list[dict]]:
    """(verdict, rows). C1–C5 as VERDICT.md states them; V1–V3 say whether the run measured what it
    claims (else INVALID, never PASS, whatever C1–C5 say). `queue` = [(t, queued, …)] at 1 Hz."""
    eps = s["endpoints"]
    after = [q for t, q, *_ in queue if t >= warmup]
    max_q = max(after) if after else None
    rows = [
        _row("C1", "same code as the chaos matrix", code == MATRIX_CODE, code),
        _row("C2", "error rate < 0.1 %", s["error_rate"] < ERROR_LIMIT,
             f"{s['errors']}/{s['requests']} = {s['error_rate']:.4%}"),
        _row("C3", "p99 < 200 ms on every endpoint",
             all(e["n"] > 0 and e["p99_ms"] is not None and e["p99_ms"] < P99_LIMIT_MS
                 for e in eps.values()),
             {op: e["p99_ms"] for op, e in eps.items()}),
        _row("C4", "queued ≤ 100 after warm-up, drained ≤ 60 s",
             max_q is not None and max_q <= QUEUE_BOUND and drain_s is not None and drain_s <= DRAIN_LIMIT,
             f"max queued {max_q}, drain {drain_s} s"),
        _row("C5", "invariants I1–I8 after the drain", invariants_ok, "PASS" if invariants_ok else "FAIL"),
        _row("V1", "≥ 99 % sent within 50 ms of schedule", s["on_time"] >= ON_TIME_SHARE,
             f"{s['on_time']:.2%}"),
        _row("V2", "a kill hit a busy worker", kills_busy >= 1, kills_busy),
        _row("V3", "a job was re-executed", re_executions >= 1, re_executions),
    ]
    if not all(r["ok"] for r in rows if r["id"].startswith("V")):
        return "INVALID", rows
    return ("PASS" if all(r["ok"] for r in rows) else "FAIL"), rows


# --- the run -------------------------------------------------------------------------------------

COUNTS = (
    "SELECT count(*) FILTER (WHERE status = 'queued') AS queued,"
    "       count(*) FILTER (WHERE status = 'running') AS running,"
    "       count(*) FILTER (WHERE status IN ('succeeded', 'failed')) AS done,"
    "       count(*) FILTER (WHERE status = 'cancelled') AS cancelled FROM jobs"
)
PGSTAT = (
    "SELECT n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_hot_upd, autovacuum_count,"
    "       autoanalyze_count, pg_relation_size('jobs') AS heap_bytes,"
    "       pg_total_relation_size('jobs') AS total_bytes"
    "  FROM pg_stat_user_tables WHERE relname = 'jobs'"
)


class Abort(Exception):
    """Preflight refused to start; nothing was changed."""


class LoadRun:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")  # a name, not lease logic
        self.name = (f"{stamp}-{args.mix}-s{args.seed}"
                     + (f"-d{args.duration:g}" if args.duration != DURATION else ""))
        self.dir = RESULTS / self.name
        self.hlog = self.clog = self.qcsv = None  # opened after preflight
        self.t0 = time.monotonic()
        self.say_lock = threading.Lock()
        self.wlock = threading.Lock()  # workers dict: killer vs. shutdown
        # Written by the event loop only (one thread), read after it has finished.
        self.records: list[dict] = []
        self.known: list[tuple[str, str]] = []  # (key, id) of every acknowledged key, in order
        self.known_keys: set[str] = set()
        self.workers: dict[int, Worker] = {}
        self.kills: list[dict] = []
        self.queue: list[tuple] = []  # (t, queued, running, done, cancelled)
        self.pgstat: list[dict] = []
        self.errors = 0  # load-test bugs: counted and shown, never silent
        self.stop_kills = threading.Event()
        self.stop_monitor = threading.Event()

    # -- logging ----------------------------------------------------------------------------------

    def now(self) -> float:
        return round(time.monotonic() - self.t0, 3)

    def say(self, msg: str) -> None:
        line = f"[{self.now():8.2f}] {msg}"
        with self.say_lock:
            print(line, flush=True)
            if self.hlog is not None:
                self.hlog.write(line + "\n")

    def guarded(self, name: str, fn, *a) -> None:
        try:
            fn(*a)
        except Exception:
            self.errors += 1
            self.say(f"LOAD TEST ERROR in {name}:\n{traceback.format_exc()}")

    def thread(self, name: str, fn, *a) -> threading.Thread:
        t = threading.Thread(target=self.guarded, args=(name, fn, *a), name=name, daemon=True)
        t.start()
        return t

    # -- setup and processes ----------------------------------------------------------------------

    def preflight(self) -> str:
        if config.MUTATION:
            raise Abort(f"MUTATION={config.MUTATION} is set; the load test measures the correct code")
        try:
            httpx.get(f"{self.args.api}/healthz", timeout=5).raise_for_status()
        except httpx.HTTPError as e:
            raise Abort(f"API at {self.args.api} not healthy ({e!r}); `docker compose up -d --build db api`")
        with db.connect("worker", "load-preflight") as c:
            others = c.execute(
                "SELECT application_name FROM pg_stat_activity WHERE application_name LIKE 'worker-%'"
            ).fetchall()
            if others:
                names = ", ".join(sorted(r["application_name"] for r in others))
                raise Abort(f"workers the load test didn't spawn are connected ({names}); "
                            "run `docker compose stop workers` first")
            c.execute("TRUNCATE jobs, job_effects, job_transitions RESTART IDENTITY")
            repo.sync_schema_with_mutation(c)  # restores the unique key if an M4 run dropped it
            return c.execute("SHOW transaction_isolation").fetchone()["transaction_isolation"]

    def open_dir(self) -> None:
        (self.dir / "workers").mkdir(parents=True)
        self.hlog = open(self.dir / "load.log", "w", encoding="utf-8", buffering=1)
        self.clog = open(self.dir / "client.jsonl", "w", encoding="utf-8", buffering=1)
        self.qcsv = open(self.dir / "queue.csv", "w", encoding="utf-8", buffering=1)
        self.qcsv.write("t,queued,running,done,cancelled\n")

    def spawn(self, slot: int) -> None:
        """Caller holds self.wlock (or nothing else runs yet)."""
        gen = self.workers[slot].gen + 1 if slot in self.workers else 0
        out = open(self.dir / "workers" / f"w{slot}-g{gen}.log", "w", encoding="utf-8")
        proc = subprocess.Popen(
            [sys.executable, "-m", "app.worker"],
            stdout=out, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        out.close()
        self.workers[slot] = Worker(proc, f"worker-{socket.gethostname()}-{proc.pid}", gen)

    def wait_workers(self) -> None:
        with db.connect("worker", "load-wait") as c:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                n = c.execute("SELECT count(*) AS n FROM pg_stat_activity"
                              " WHERE application_name LIKE 'worker-%'").fetchone()["n"]
                if n >= self.args.workers:
                    return
                time.sleep(0.2)
        raise RuntimeError(f"{self.args.workers} workers did not connect within 20 s")

    def stop_workers(self, graceful: bool) -> None:
        with self.wlock:
            procs = [w.proc for w in self.workers.values()]
        for p in procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM if graceful else signal.SIGKILL)
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    # -- actors (threads) -------------------------------------------------------------------------

    def killer(self) -> None:
        rng = random.Random(f"{self.args.seed}-killer")
        with db.connect("worker", "load-killer") as c:
            while not self.stop_kills.wait(rng.uniform(*KILL_EVERY)):
                with self.wlock:
                    slot = rng.choice(sorted(self.workers))
                    w = self.workers[slot]
                    w.proc.kill()
                    w.proc.wait()
                    # Dead process, lease still valid: what it held is `running` under its id.
                    held = c.execute(
                        "SELECT id::text AS id, attempt FROM jobs"
                        " WHERE status = 'running' AND worker_id = %s", (w.worker_id,)
                    ).fetchone()
                    self.spawn(slot)
                self.kills.append({"t": self.now(), "slot": slot, "worker": w.worker_id,
                                   "job": held and held["id"], "attempt": held and held["attempt"]})
                self.say(f"kill w{slot} {w.worker_id}"
                         + (f" holding {held['id']} a={held['attempt']}" if held else " idle"))

    def monitor(self) -> None:
        """1 Hz queue sample, pg_stat every 10 s, until stopped after the drain."""
        with db.connect("worker", "load-monitor") as c:
            next_stat = 0.0
            while True:
                t = self.now()
                n = c.execute(COUNTS).fetchone()
                sample = (t, n["queued"], n["running"], n["done"], n["cancelled"])
                self.queue.append(sample)
                self.qcsv.write(",".join(map(str, sample)) + "\n")
                final = self.stop_monitor.is_set()  # set after the drain: one last full sample
                if t >= next_stat or final:
                    stat = {"t": t, **c.execute(PGSTAT).fetchone()}
                    self.pgstat.append(stat)
                    with open(self.dir / "pgstat.jsonl", "a", encoding="utf-8") as f:
                        f.write(json.dumps(stat) + "\n")
                    next_stat = t + PGSTAT_EVERY
                if final:
                    return
                self.stop_monitor.wait(max(0.0, 1.0 - (self.now() - t)))

    # -- the generator (asyncio, one thread) --------------------------------------------------------

    def record(self, rec: dict) -> None:
        self.records.append(rec)
        self.clog.write(json.dumps(rec) + "\n")

    async def generate(self, plan: list[Req]) -> None:
        limits = httpx.Limits(max_connections=None, max_keepalive_connections=200)
        async with httpx.AsyncClient(base_url=self.args.api, timeout=TIMEOUT + 1.0,
                                     limits=limits) as client:
            pending: set[asyncio.Task] = set()
            for req in plan:
                delay = self.t0 + req.at - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                task = asyncio.create_task(self.send(client, req))
                pending.add(task)
                task.add_done_callback(pending.discard)
            if pending:
                await asyncio.wait(pending)

    async def send(self, client: httpx.AsyncClient, req: Req) -> None:
        try:
            await self._send(client, req)
        except Exception:
            self.errors += 1
            self.say(f"LOAD TEST ERROR in send {req}:\n{traceback.format_exc()}")

    async def _send(self, client: httpx.AsyncClient, req: Req) -> None:
        due = self.t0 + req.at
        rec: dict = {"t": round(req.at, 3), "op": req.op}
        if req.op == "create":
            rec.update(key=req.key, body=req.body, id=None, retry=req.retry)
            call = {"method": "POST", "url": "/jobs", "json": req.body,
                    "headers": {"Idempotency-Key": req.key}}
        elif req.op == "list":
            rec["params"] = req.params
            call = {"method": "GET", "url": "/jobs", "params": req.params}
        else:
            pool = self.known if req.op == "get" else self.known[-CANCEL_RECENT:]
            if not pool:
                rec.update(skipped=True, lag=round(time.monotonic() - due, 4))
                self.record(rec)
                return
            key, job_id = pool[int(req.u * len(pool))]
            rec.update(key=key, id=job_id)
            call = ({"method": "GET", "url": f"/jobs/{job_id}"} if req.op == "get"
                    else {"method": "POST", "url": f"/jobs/{job_id}/cancel"})

        rec["lag"] = round(time.monotonic() - due, 4)
        status, error, job = None, None, None
        try:
            r = await asyncio.wait_for(client.request(**call), TIMEOUT)
            status = r.status_code
            if req.op == "create" and 200 <= status < 300:
                job = r.json()
        except TimeoutError:
            error = "timeout"
        except httpx.HTTPError as e:
            error = type(e).__name__
        except ValueError:
            status, error = None, "bad json"
        rec.update(status=status, error=error,
                   lat=None if status is None else round(time.monotonic() - due, 4))
        if job is not None:
            rec["id"] = job["id"]
            if req.key not in self.known_keys:
                self.known_keys.add(req.key)
                self.known.append((req.key, job["id"]))
        self.record(rec)

    # -- main -------------------------------------------------------------------------------------

    def drain(self, wait: float) -> tuple[bool, float | None]:
        """Seconds from the scheduled end of the load until queued + running = 0."""
        end = self.t0 + self.args.duration
        with db.connect("worker", "load-drain") as c:
            while True:
                n = c.execute(COUNTS).fetchone()
                if n["queued"] + n["running"] == 0:
                    return True, round(time.monotonic() - end, 1)
                if time.monotonic() - end > wait:
                    return False, None
                time.sleep(0.5)

    def execute(self) -> int:
        a = self.args
        isolation = self.preflight()
        self.open_dir()
        header = {
            "run": self.name, "mix": a.mix, "seed": a.seed, "rps": a.rps, "duration_s": a.duration,
            "role": "verdict (L1)" if a.mix == "short" else "capacity probe (L2), not a SHIP gate",
            "workers": a.workers, "kill_every_s": KILL_EVERY, "api": a.api,
            "commit": git_commit(), "code": code_hash(), "isolation": isolation,
            "warmup_s": WARMUP, "timeout_s": TIMEOUT,
            "config": {"lease_s": config.LEASE_SECONDS, "heartbeat_s": config.HEARTBEAT_SECONDS,
                       "reap_s": config.REAP_SECONDS, "max_attempts": config.MAX_ATTEMPTS,
                       "api_pool_max": config.POOL_MAX},
        }
        self.say("header " + json.dumps(header))
        plan = make_plan(a.seed, rps=a.rps, duration=a.duration, mix=a.mix)
        drained, drain_s = False, None
        monitor = killer = None
        try:
            with self.wlock:
                for slot in range(a.workers):
                    self.spawn(slot)
            self.wait_workers()
            self.t0 = time.monotonic()  # plan times count from here
            monitor = self.thread("monitor", self.monitor)
            killer = self.thread("killer", self.killer)
            asyncio.run(self.generate(plan))
            self.stop_kills.set()
            killer.join()
            self.say(f"load done: {len(self.records)} requests, {len(self.kills)} kills")
            drained, drain_s = self.drain(a.drain_wait or DRAIN_WAIT[a.mix])
            self.say(f"drain: {'%.1f s after the load' % drain_s if drained else 'TIMEOUT'}")
        finally:
            self.stop_kills.set()
            if killer is not None:
                killer.join(timeout=15)
            # Drained: let workers exit cleanly. Not drained: freeze the state as it is.
            self.stop_workers(graceful=drained)
            self.stop_monitor.set()
            if monitor is not None:
                monitor.join(timeout=15)
        return self.report(header, drained, drain_s)

    def report(self, header: dict, drained: bool, drain_s: float | None) -> int:
        a = self.args
        with db.connect("worker", "load-report") as c:
            snap = invariants.load(c)
        sent = [r for r in self.records if not r.get("skipped")]
        results = invariants.check_snapshot(snap, sent, config.MAX_ATTEMPTS)
        claims = [len(invariants.claims(h)) for h in snap.transitions.values()]
        re_exec = sum(max(0, n - 1) for n in claims)
        kills_busy = sum(1 for k in self.kills if k["job"])
        s = summarize(self.records)
        verdict, rows = criteria(s, queue=self.queue, drain_s=drain_s,
                                 invariants_ok=all(r.ok for r in results), code=header["code"],
                                 kills_busy=kills_busy, re_executions=re_exec)
        if self.errors:
            verdict = "ERROR"  # a load-test bug: the numbers can't be trusted

        steady = [q for q in self.queue if WARMUP <= q[0] <= a.duration]
        done_per_s = None
        if len(steady) >= 2 and steady[-1][0] > steady[0][0]:
            done_per_s = round((steady[-1][3] - steady[0][3]) / (steady[-1][0] - steady[0][0]), 1)
        slope = None
        if len(steady) >= 2:
            ts, qs = [q[0] for q in steady], [q[1] for q in steady]
            mt, mq = sum(ts) / len(ts), sum(qs) / len(qs)
            var = sum((t - mt) ** 2 for t in ts)
            slope = round(sum((t - mt) * (q - mq) for t, q in zip(ts, qs)) / var, 3) if var else None
        first, last = (self.pgstat[0], self.pgstat[-1]) if self.pgstat else ({}, {})
        upd = last.get("n_tup_upd", 0) - first.get("n_tup_upd", 0)
        hot = last.get("n_tup_hot_upd", 0) - first.get("n_tup_hot_upd", 0)
        rep = {
            **header,
            "verdict": verdict,
            "criteria": rows,
            "summary": s,
            "wall_s": self.now(),
            "drained": drained, "drain_s": drain_s,
            "jobs_finished_per_s": done_per_s,
            "queued": {"max_after_warmup": max((q[1] for q in steady), default=None),
                       "at_end_of_load": steady[-1][1] if steady else None,
                       "slope_per_s": slope},
            "dead_tuples": {"max": max((p["n_dead_tup"] for p in self.pgstat), default=None),
                            "at_end": last.get("n_dead_tup"), "live_at_end": last.get("n_live_tup"),
                            "autovacuums": last.get("autovacuum_count", 0) - first.get("autovacuum_count", 0),
                            "updates": upd, "hot_share": round(hot / upd, 3) if upd else None,
                            "heap_bytes": last.get("heap_bytes"), "total_bytes": last.get("total_bytes")},
            "final_status": dict(Counter(j["status"] for j in snap.jobs.values())),
            "kills": len(self.kills), "kills_busy": kills_busy,
            "re_executions": re_exec, "exhausted": results[0].info["exhausted"],
            "cancels_by_from": dict(Counter(f for h in snap.transitions.values()
                                            for f, to, _ in h if to == "cancelled")),
            "load_test_errors": self.errors,
            "invariants": [asdict(r) for r in results],
        }
        (self.dir / "report.json").write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")

        lines = [f"run {self.name}  verdict {verdict}  ({header['role']})"]
        lines += [f"  {r['id']} {'PASS' if r['ok'] else 'FAIL':4} {r['name']}: {r['value']}" for r in rows]
        for op, e in s["endpoints"].items():
            lines.append(f"  {op:6} n={e['n']:6} p50={e['p50_ms']} ms p99={e['p99_ms']} ms max={e['max_ms']} ms")
        for k in ("summary", "jobs_finished_per_s", "queued", "dead_tuples", "final_status", "kills",
                  "kills_busy", "re_executions", "exhausted", "cancels_by_from", "drain_s", "wall_s",
                  "load_test_errors"):
            v = {x: y for x, y in s.items() if x != "endpoints"} if k == "summary" else rep[k]
            lines.append(f"  {k}: {v}")
        for r in results:
            ex = f" e.g. {', '.join(r.examples)}" if r.examples else ""
            lines.append(f"  {r.id} {'PASS' if r.ok else 'FAIL':4} {r.title}: {r.count} violations{ex} {r.info or ''}")
        text = "\n".join(lines)
        print(text, flush=True)
        (self.dir / "report.txt").write_text(text + "\n", encoding="utf-8")
        self.index_row(rep)
        self.close_logs()
        return 0 if verdict == "PASS" else 1

    def index_row(self, rep: dict) -> None:
        s, eps = rep["summary"], rep["summary"]["endpoints"]
        failed = [r["id"] for r in rep["criteria"] if not r["ok"]]
        if rep["load_test_errors"]:
            failed.append(f"load_test_errors({rep['load_test_errors']})")
        inv = [f"{r['id']}({r['count']})" for r in rep["invariants"] if not r["ok"]]
        p99 = " / ".join(str(eps[op]["p99_ms"]) for op in OPS)
        self.append_index(
            f"| {rep['run']} | {rep['mix']} | {rep['seed']} | {rep['duration_s']:g} | {rep['verdict']} "
            f"| {', '.join(failed) or '-'} | {s['requests']} | {s['errors']} ({s['error_rate']:.3%}) "
            f"| {p99} | {rep['queued']['max_after_warmup']} | {rep['drain_s']} "
            f"| {rep['jobs_finished_per_s']} | {rep['kills']} ({rep['kills_busy']}) | {rep['re_executions']} "
            f"| {'PASS' if not inv else 'FAIL ' + ', '.join(inv)} | {rep['dead_tuples']['max']} | | {rep['code']} |"
        )

    def append_index(self, row: str) -> None:
        index = RESULTS / "INDEX.md"
        if not index.exists():
            index.write_text(
                "# Load runs\n\nEvery run, including failed and invalid ones, appended by "
                "`loadtest/load.py`. Never edited; notes go in the notes column only. Criteria: "
                "Docs/VERDICT.md (C1–C5, validity V1–V3).\n\n"
                "| run | mix | seed | s | verdict | failed | requests | errors | "
                "p99 ms create / get / list / cancel | max queued (t ≥ 30 s) | drain s | jobs/s "
                "| kills (busy) | re-exec | invariants | max dead tuples | notes | code |\n"
                "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n",
                encoding="utf-8",
            )
        with index.open("a", encoding="utf-8") as f:
            f.write(row + "\n")

    def close_logs(self) -> None:
        for f in (self.clog, self.qcsv, self.hlog):
            if f is not None and not f.closed:
                f.close()
        compress_logs(self.dir)  # client.jsonl and workers/*.log → .gz (lossless)

    def crashed(self, exc: BaseException) -> None:
        """A run that died still gets its row: every run is listed."""
        if self.hlog is None:
            return  # died before the run dir existed: nothing ran
        self.say(f"CRASH: {exc!r}")
        self.say(traceback.format_exc())
        self.append_index(
            f"| {self.name} | {self.args.mix} | {self.args.seed} | {self.args.duration:g} | ERROR "
            f"| {type(exc).__name__} (see load.log) | {len(self.records)} | | | | | | {len(self.kills)} "
            f"| | | | | {code_hash()} |"
        )
        try:  # best effort: the row above is what must survive a crash
            self.close_logs()
        except Exception as e:
            print(f"close_logs failed: {e!r}", flush=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--mix", choices=MIXES, default="short")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--rps", type=float, default=RPS)
    p.add_argument("--duration", type=float, default=DURATION, help="seconds of load")
    p.add_argument("--workers", type=int, default=WORKERS)
    p.add_argument("--api", default=os.environ.get("API_URL", "http://api:8000"))
    p.add_argument("--drain-wait", type=float, default=None,
                   help="max seconds to wait for an empty queue (default: 60 short, 600 chaos)")
    args = p.parse_args(argv)
    signal.signal(signal.SIGTERM, interrupted)  # docker stop → unwind through execute()'s finally
    run = LoadRun(args)
    try:
        return run.execute()
    except Abort as e:
        run.say(f"ABORT: {e}")
        return 2
    except BaseException as e:
        run.crashed(e)
        # Workers are stopped (execute's finally) and the row is written; don't hang on threads.
        sys.stdout.flush()
        os._exit(3)


if __name__ == "__main__":
    sys.exit(main())
