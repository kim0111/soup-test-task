"""Session settings per role (SPEC §4 Timeouts). Would fail if db.connect dropped the options."""

import pytest

from app import db


def show(conn, setting: str) -> str:
    return conn.execute(f"SHOW {setting}").fetchone()[setting]


def test_api_session_has_statement_and_lock_timeouts(db_url):
    with db.connect("api", "api-test", url=db_url) as c:
        assert show(c, "statement_timeout") == "5s"
        assert show(c, "lock_timeout") == "2s"


def test_worker_session_has_statement_timeout_only(db_url):
    with db.connect("worker", "worker-test", url=db_url) as c:
        assert show(c, "statement_timeout") == "3s"
        assert show(c, "lock_timeout") == "0"


@pytest.mark.parametrize("role", ["api", "worker"])
def test_session_is_autocommit_and_named(db_url, role):
    with db.connect(role, f"{role}-name", url=db_url) as c:
        assert c.autocommit
        assert show(c, "application_name") == f"{role}-name"
        # dict rows: callers read columns by name
        assert c.execute("SELECT 1 AS one").fetchone() == {"one": 1}


def test_pool_sessions_get_the_role_settings(db_url):
    """The API serves requests from a pool; its sessions need the same timeouts and autocommit."""
    with db.pool("api", "api-pool", url=db_url) as p, p.connection() as c:
        assert c.autocommit
        assert show(c, "statement_timeout") == "5s"
        assert show(c, "lock_timeout") == "2s"
        assert show(c, "application_name") == "api-pool"
