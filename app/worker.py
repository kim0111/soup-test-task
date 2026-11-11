"""Worker process (SPEC §5): reap → claim → run → complete, in one thread on one autocommit session.

Every write is a fenced statement in app/repo.py. A False return means the job was cancelled or the
lease was lost: the worker abandons it and writes nothing. A DB error also abandons it; the lease
then expires and the job runs again. Heartbeat and reaper run in the same loop as the work, never
in a thread (a heartbeat thread would keep a hung job alive forever).

Only time.monotonic() schedules anything here. Leases are timed by the DB's now() (SPEC §9).

Logs: one line per event, `<event> key=value …` (claimed, succeeded, failed, abandoned, reaped,
reconnect, db_error, connected, stopped); the chaos harness reads them.
"""

import logging
import os
import random
import signal
import socket
import threading
import time
from dataclasses import dataclass
from typing import Literal

import psycopg

from app import config, db, repo

log = logging.getLogger("worker")

Outcome = Literal["succeeded", "failed", "abandoned"]


@dataclass(frozen=True)
class Timings:
    """Parameters rather than globals, so tests can run on a sub-second lease."""

    lease: float = config.LEASE_SECONDS
    heartbeat: float = config.HEARTBEAT_SECONDS
    reap: float = config.REAP_SECONDS
    chunk: float = config.CHUNK_SECONDS
    idle_min: float = config.IDLE_MIN_SECONDS
    idle_max: float = config.IDLE_MAX_SECONDS
    reconnect_max: float = config.RECONNECT_MAX_SECONDS
    max_attempts: int = config.MAX_ATTEMPTS


class Reaper:
    """Requeues expired leases every `timings.reap` seconds, called both between jobs and between
    chunks of a running job: with every worker busy, recovery would otherwise wait for a free
    worker instead of staying ≤ lease + reap interval (SPEC §2)."""

    def __init__(self, timings: Timings):
        self.timings = timings
        self.due = time.monotonic()  # the first call reaps

    def maybe_reap(self, conn) -> None:
        now = time.monotonic()
        if now < self.due:
            return
        self.due = now + self.timings.reap  # set first: a failing reap isn't retried every chunk
        n = repo.reap(conn, self.timings.max_attempts)
        if n:
            log.info("reaped n=%d", n)


def _brief(e: BaseException) -> str:
    first = str(e).strip().splitlines()[0] if str(e).strip() else ""
    return f"{type(e).__name__}: {first}"


def run_job(conn, job: dict, worker_id: str, *, timings: Timings = Timings(),
            reaper: Reaper | None = None) -> Outcome:
    """Execute a claimed job: sleep `duration_ms` in chunks, heartbeating and reaping when due.

    `job["attempt"]` is the fencing token for every write. succeeded/failed → this worker's
    terminal write landed; abandoned → it didn't (cancelled, lease lost, or a DB error).
    """
    job_id, attempt, payload = job["id"], job["attempt"], job["payload"]
    reaper = reaper or Reaper(timings)
    tag = f"job={job_id} attempt={attempt}"
    try:
        start = time.monotonic()
        end = start + payload["duration_ms"] / 1000
        next_heartbeat = start + timings.heartbeat
        while (left := end - time.monotonic()) > 0:
            time.sleep(min(timings.chunk, left))
            now = time.monotonic()
            if now >= next_heartbeat:
                next_heartbeat = now + timings.heartbeat
                if not repo.heartbeat(conn, job_id, attempt, timings.lease):
                    log.info("abandoned %s reason=heartbeat_rejected", tag)
                    return "abandoned"
            reaper.maybe_reap(conn)

        if payload["fail"]:
            outcome: Outcome = "failed"
            ok = repo.fail(conn, job_id, attempt, "job failed (fail=true)")
        else:
            outcome = "succeeded"
            ok = repo.complete(conn, job_id, attempt, worker_id)
    except psycopg.Error as e:
        log.warning("abandoned %s reason=db_error error=%s", tag, _brief(e))
        return "abandoned"

    if not ok:
        log.info("abandoned %s reason=%s_rejected", tag, "fail" if outcome == "failed" else "complete")
        return "abandoned"
    log.info("%s %s", outcome, tag)
    return outcome


def run(url: str, worker_id: str, stop: threading.Event, *, timings: Timings = Timings()) -> None:
    """Main loop until `stop` is set: reap when due → claim → run. Never exits on a DB error: a lost
    connection is reopened with jittered backoff. `stop` is checked right before every claim, so
    once it is set the current job is finished and nothing more is claimed."""
    reaper = Reaper(timings)
    idle = timings.idle_min
    retry = timings.idle_min
    conn: psycopg.Connection | None = None
    try:
        while not stop.is_set():
            try:
                if conn is not None and (conn.closed or conn.broken):
                    conn.close()
                    conn = None
                    log.warning("reconnect reason=connection_lost")
                if conn is None:
                    conn = db.connect("worker", worker_id, url=url)
                    log.info("connected")
                reaper.maybe_reap(conn)
                job = None if stop.is_set() else repo.claim(conn, worker_id, timings.lease)
            except psycopg.Error as e:
                delay = random.uniform(timings.idle_min, retry)
                retry = min(retry * 2, timings.reconnect_max)
                log.warning("db_error retry_in=%.2fs error=%s", delay, _brief(e))
                stop.wait(delay)
                continue
            retry = timings.idle_min

            if job is None:
                stop.wait(random.uniform(timings.idle_min, idle))
                idle = min(idle * 2, timings.idle_max)
                continue
            idle = timings.idle_min
            log.info("claimed job=%s attempt=%d", job["id"], job["attempt"])
            try:
                run_job(conn, job, worker_id, timings=timings, reaper=reaper)
            except Exception as e:
                # A bug or a malformed row must not kill the worker: in compose nothing restarts
                # it, and every worker that claims the row would die the same way. Like any
                # abandon, the lease expires and max_attempts turns the row into `failed`.
                log.error("abandoned job=%s attempt=%d reason=crash error=%s",
                          job["id"], job["attempt"], _brief(e))
    finally:
        if conn is not None:
            conn.close()
    log.info("stopped")


def main() -> None:
    worker_id = os.environ.get("WORKER_ID") or f"worker-{socket.gethostname()}-{os.getpid()}"
    url = os.environ["DATABASE_URL"]
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {worker_id} %(message)s")

    # SIGTERM/SIGINT: finish the current job, claim nothing more, exit 0. As PID 1 in a container,
    # Python ignores SIGTERM unless a handler is installed. A job longer than docker's stop grace
    # period gets SIGKILLed, which is the ordinary lease-expiry path.
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    run(url, worker_id, stop)


if __name__ == "__main__":
    main()
