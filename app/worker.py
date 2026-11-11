"""Worker process. Phase 1 stub: connect to Postgres and idle until SIGTERM."""

import logging
import os
import signal
import socket
import threading

import psycopg

WORKER_ID = os.environ.get("WORKER_ID") or f"worker-{socket.gethostname()}-{os.getpid()}"
DATABASE_URL = os.environ["DATABASE_URL"]

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {WORKER_ID} %(message)s")
log = logging.getLogger("worker")

stop = threading.Event()


def main() -> None:
    # As PID 1 in a container, Python ignores SIGTERM unless a handler is installed.
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    with psycopg.connect(DATABASE_URL, autocommit=True, application_name=WORKER_ID) as conn:
        conn.execute("SELECT 1")
        log.info("idle")
        while not stop.wait(1.0):
            pass
    log.info("stopped")


if __name__ == "__main__":
    main()
