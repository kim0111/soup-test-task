"""Connections. Every session is autocommit: no client holds a transaction open across a round
trip, so a frozen or killed process never holds a row lock (SPEC §1)."""

from typing import Literal

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from app import config

Role = Literal["api", "worker"]

# SPEC §4 Timeouts. API: a blocked request fails fast (→ 503, client retries).
# Worker: a stuck statement errors out, the worker abandons the job and the lease expires.
_SESSION_OPTIONS: dict[Role, str] = {
    "api": "-c statement_timeout=5s -c lock_timeout=2s",
    "worker": "-c statement_timeout=3s",
}


def _session(role: Role, application_name: str) -> dict:
    return {
        "autocommit": True,
        "row_factory": dict_row,
        "application_name": application_name,
        "options": _SESSION_OPTIONS[role],
    }


def connect(role: Role, application_name: str, url: str | None = None) -> psycopg.Connection:
    return psycopg.connect(url or config.DATABASE_URL, **_session(role, application_name))


def pool(role: Role, application_name: str, url: str | None = None) -> ConnectionPool:
    """Opened on construction. Each checkout is health-checked, so a connection the server dropped
    (restart, idle kill) is replaced instead of failing a request. Waiting longer than
    POOL_TIMEOUT_SECONDS for a free connection raises PoolTimeout."""
    return ConnectionPool(
        url or config.DATABASE_URL,
        kwargs=_session(role, application_name),
        min_size=config.POOL_MIN,
        max_size=config.POOL_MAX,
        timeout=config.POOL_TIMEOUT_SECONDS,
        check=ConnectionPool.check_connection,
        open=True,
        name=application_name,
    )
