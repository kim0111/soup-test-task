"""Tests run against a real Postgres, in a dedicated database rebuilt from migrations/ each session."""

import os
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from app import db

MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"
BASE_URL = os.environ["DATABASE_URL"]
TEST_DB = conninfo_to_dict(BASE_URL)["dbname"] + "_test"
TEST_URL = make_conninfo(BASE_URL, dbname=TEST_DB)


@pytest.fixture(scope="session")
def db_url() -> str:
    with psycopg.connect(BASE_URL, autocommit=True) as admin:
        name = sql.Identifier(TEST_DB)
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(name))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(name))
    with psycopg.connect(TEST_URL, autocommit=True) as conn:
        for migration in sorted(MIGRATIONS.glob("*.sql")):
            conn.execute(migration.read_text(encoding="utf-8"))
    return TEST_URL


@pytest.fixture
def conn(db_url):
    """A worker-role session on empty tables."""
    with db.connect("worker", "pytest", url=db_url) as c:
        c.execute("TRUNCATE jobs, job_effects, job_transitions RESTART IDENTITY")
        yield c
