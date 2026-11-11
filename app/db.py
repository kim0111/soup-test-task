"""Connections. Every session is autocommit: no client holds a transaction open across a round
trip, so a frozen or killed process never holds a row lock (SPEC §1)."""

from typing import Literal

import psycopg
from psycopg.rows import dict_row

from app import config

Role = Literal["api", "worker"]

# SPEC §4 Timeouts. API: a blocked request fails fast (→ 503, client retries).
# Worker: a stuck statement errors out, the worker abandons the job and the lease expires.
_SESSION_OPTIONS: dict[Role, str] = {
    "api": "-c statement_timeout=5s -c lock_timeout=2s",
    "worker": "-c statement_timeout=3s",
}


def connect(role: Role, application_name: str, url: str | None = None) -> psycopg.Connection:
    return psycopg.connect(
        url or config.DATABASE_URL,
        autocommit=True,
        row_factory=dict_row,
        application_name=application_name,
        options=_SESSION_OPTIONS[role],
    )
