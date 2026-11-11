"""HTTP contract (SPEC §4): status codes, validation before SQL, keyset pagination, 503 on timeouts.

Runs the real app (lifespan, pool) against the test database through TestClient.
"""

import base64
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app import api, config, db, repo

BODY = {"duration_ms": 10, "fail": False}
MISSING = "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def client(db_url, conn):
    with TestClient(api.create_app(db_url)) as c:
        yield c


def create(client, key: str | None = None, body=BODY, **kw):
    return client.post("/jobs", json=body, headers={"Idempotency-Key": str(uuid4()) if key is None else key}, **kw)


# --- POST /jobs ----------------------------------------------------------------------------------

class TestCreate:
    def test_new_key_is_201_with_a_queued_job(self, client):
        r = create(client, "k1")
        assert r.status_code == 201
        job = r.json()
        assert (job["status"], job["attempt"], job["payload"]) == ("queued", 0, BODY)
        assert client.get(f"/jobs/{job['id']}").json()["id"] == job["id"]

    def test_retry_with_same_body_is_200_with_the_same_job(self, client):
        first = create(client, "k1").json()
        r = create(client, "k1")
        assert r.status_code == 200
        assert r.json()["id"] == first["id"]

    def test_same_key_different_body_is_409(self, client):
        first = create(client, "k1").json()
        r = create(client, "k1", {"duration_ms": 999, "fail": True})
        assert r.status_code == 409
        assert r.json()["job_id"] == first["id"]

    def test_explicit_default_equals_omitted(self, client):
        assert create(client, "k1", {"duration_ms": 10}).status_code == 201
        assert create(client, "k1", {"duration_ms": 10, "fail": False}).status_code == 200

    def test_equivalent_numbers_are_the_same_body(self, client):
        assert create(client, "k1", {"duration_ms": 1}).status_code == 201
        assert create(client, "k1", {"duration_ms": 1.0}).status_code == 200

    def test_unknown_fields_are_ignored_and_not_stored(self, client):
        r = create(client, "k1", {**BODY, "extra": 1})
        assert r.status_code == 201
        assert r.json()["payload"] == BODY
        assert create(client, "k1", BODY).status_code == 200

    @pytest.mark.parametrize(
        "body",
        [
            {"duration_ms": -1},
            {"duration_ms": 60001},
            {"duration_ms": 1.5},
            {"fail": False},
            [BODY],
            "x",
        ],
        ids=["negative", "too-long", "fractional", "missing-duration", "array", "string"],
    )
    def test_invalid_body_is_400_and_creates_nothing(self, client, conn, body):
        assert create(client, body=body).status_code == 400
        assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 0

    def test_non_json_body_is_400(self, client):
        r = client.post(
            "/jobs",
            content=b"{not json",
            headers={"Idempotency-Key": "k1", "Content-Type": "application/json"},
        )
        assert r.status_code == 400

    def test_missing_key_is_400(self, client):
        assert client.post("/jobs", json=BODY).status_code == 400

    @pytest.mark.parametrize(
        "key", ["", "x" * 256, "a\tb", "a\x7fb"], ids=["empty", "256-chars", "tab", "del"]
    )
    def test_bad_key_is_400_and_creates_nothing(self, client, conn, key):
        assert create(client, key).status_code == 400
        assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 0

    def test_key_of_255_chars_is_accepted(self, client):
        assert create(client, "x" * 255).status_code == 201


# --- GET /jobs/{id} ------------------------------------------------------------------------------

class TestGet:
    def test_existing_job(self, client):
        job = create(client).json()
        r = client.get(f"/jobs/{job['id']}")
        assert r.status_code == 200
        assert r.json() == job

    def test_unknown_id_is_404(self, client):
        r = client.get(f"/jobs/{MISSING}")
        assert (r.status_code, r.json()["detail"]) == (404, "job not found")

    def test_malformed_id_is_400(self, client):
        assert client.get("/jobs/not-a-uuid").status_code == 400


# --- GET /jobs -----------------------------------------------------------------------------------

def ids_newest_first(conn) -> list[str]:
    return [str(r["id"]) for r in conn.execute("SELECT id FROM jobs ORDER BY created_at DESC, id DESC")]


class TestList:
    def test_pages_newest_first_and_ends_with_null_cursor(self, client, conn):
        for _ in range(5):
            create(client)
        pages, cursor = [], None
        while True:
            r = client.get("/jobs", params={"limit": 2, **({"cursor": cursor} if cursor else {})})
            assert r.status_code == 200
            pages.append([j["id"] for j in r.json()["items"]])
            cursor = r.json()["next_cursor"]
            if cursor is None:
                break
        assert [len(p) for p in pages] == [2, 2, 1]
        assert [i for p in pages for i in p] == ids_newest_first(conn)

    def test_inserts_while_paging_cause_no_duplicates_or_gaps(self, client, conn):
        for _ in range(5):
            create(client)
        before = ids_newest_first(conn)
        seen, cursor = [], None
        while True:
            params = {"limit": 2, **({"cursor": cursor} if cursor else {})}
            body = client.get("/jobs", params=params).json()
            seen += [j["id"] for j in body["items"]]
            create(client)  # a new job lands on top of the list between page reads
            cursor = body["next_cursor"]
            if cursor is None:
                break
        assert seen == before

    def test_exact_multiple_of_limit_has_no_empty_last_page(self, client):
        for _ in range(2):
            create(client)
        assert client.get("/jobs", params={"limit": 2}).json()["next_cursor"] is None

    def test_default_limit_is_50(self, client, conn):
        for i in range(51):
            repo.create_job(conn, f"k{i}", BODY)
        body = client.get("/jobs").json()
        assert len(body["items"]) == 50 and body["next_cursor"] is not None

    def test_filters_by_status(self, client):
        keep = create(client).json()
        gone = create(client).json()
        client.post(f"/jobs/{gone['id']}/cancel")
        items = client.get("/jobs", params={"status": "queued"}).json()["items"]
        assert [j["id"] for j in items] == [keep["id"]]

    @pytest.mark.parametrize(
        "params",
        [
            {"limit": 0},
            {"limit": 201},
            {"limit": "x"},
            {"status": "done"},
            {"cursor": "%%%"},
            {"cursor": base64.urlsafe_b64encode(b"garbage").decode()},
            {"cursor": base64.urlsafe_b64encode(f"2026-01-01T00:00:00|{MISSING}".encode()).decode()},
        ],
        ids=["limit-0", "limit-201", "limit-nan", "bad-status", "cursor-not-b64", "cursor-garbage",
             "cursor-naive-time"],
    )
    def test_bad_params_are_400(self, client, params):
        assert client.get("/jobs", params=params).status_code == 400


# --- POST /jobs/{id}/cancel ----------------------------------------------------------------------

class TestCancel:
    def test_queued_job_is_200_cancelled(self, client):
        job = create(client).json()
        r = client.post(f"/jobs/{job['id']}/cancel")
        assert r.status_code == 200
        assert r.json()["status"] == "cancelled"

    def test_cancel_twice_is_200(self, client):
        job = create(client).json()
        client.post(f"/jobs/{job['id']}/cancel")
        r = client.post(f"/jobs/{job['id']}/cancel")
        assert (r.status_code, r.json()["status"]) == (200, "cancelled")

    @pytest.mark.parametrize("final", ["succeeded", "failed"])
    def test_finished_job_is_409(self, client, conn, final):
        job_id = create(client).json()["id"]
        claimed = repo.claim(conn, "w1")
        if final == "succeeded":
            repo.complete(conn, claimed["id"], claimed["attempt"], "w1")
        else:
            repo.fail(conn, claimed["id"], claimed["attempt"], "boom")
        r = client.post(f"/jobs/{job_id}/cancel")
        assert r.status_code == 409
        assert r.json()["status"] == final

    def test_unknown_id_is_404(self, client):
        r = client.post(f"/jobs/{MISSING}/cancel")
        assert (r.status_code, r.json()["detail"]) == (404, "job not found")

    def test_malformed_id_is_400(self, client):
        assert client.post("/jobs/nope/cancel").status_code == 400


# --- timeouts ------------------------------------------------------------------------------------

def test_lock_timeout_is_503_with_retry_after(client, conn, db_url):
    """A row lock held past the API's lock_timeout (2 s) is a retryable 503, not a 500, and the
    cancel did not happen."""
    job_id = create(client).json()["id"]
    with db.connect("worker", "holder", url=db_url) as holder:
        # Outlive the API's 2 s lock wait; the server would kill an idle-in-transaction session at 2 s.
        holder.execute("SET idle_in_transaction_session_timeout = '10s'")
        with holder.transaction():
            holder.execute("SELECT 1 FROM jobs WHERE id = %s FOR UPDATE", (job_id,))
            r = client.post(f"/jobs/{job_id}/cancel")
    assert r.status_code == 503
    assert r.headers["Retry-After"] == "1"
    assert client.get(f"/jobs/{job_id}").json()["status"] == "queued"


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


# --- connection handling -------------------------------------------------------------------------

def test_bad_input_is_400_even_when_the_database_is_unreachable(monkeypatch):
    """Validation happens before a connection is taken: a malformed request is the client's
    error (400), never "database busy, retry" (503)."""
    monkeypatch.setattr(config, "POOL_TIMEOUT_SECONDS", 0.5)
    with TestClient(api.create_app("postgresql://u:p@127.0.0.1:1/nowhere")) as c:
        assert c.get("/jobs/not-a-uuid").status_code == 400
        assert c.get("/jobs", params={"limit": 0}).status_code == 400
        assert create(c, body={"duration_ms": -1}).status_code == 400
        assert c.post("/jobs/nope/cancel").status_code == 400
        # A valid request does need the database, and it is down: retryable 503.
        r = c.get(f"/jobs/{MISSING}")
        assert (r.status_code, r.headers["Retry-After"]) == (503, "1")


@pytest.fixture
def server(db_url, conn):
    """The app under a real uvicorn on a free port: concurrency goes through FastAPI's threadpool
    exactly as in production (TestClient would not reproduce it)."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    srv = uvicorn.Server(uvicorn.Config(api.create_app(db_url), log_level="warning"))
    t = threading.Thread(target=srv.run, kwargs={"sockets": [sock]}, daemon=True)
    t.start()
    while not srv.started:
        t.join(0.01)
    yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    srv.should_exit = True
    t.join(10)


def test_burst_of_concurrent_requests_all_succeed(server, conn):
    """200 simultaneous reads, far more than the 40-thread pool and the 20 pooled connections:
    every one is served. Breaks if requests can hold a pooled connection while waiting for a
    thread (connection taken in a separate dependency step): that deadlocks until PoolTimeout."""
    job_id = new_job_id(conn)
    with ThreadPoolExecutor(max_workers=200) as pool:
        codes = list(pool.map(lambda _: httpx.get(f"{server}/jobs/{job_id}", timeout=30).status_code,
                              range(200)))
    assert {code: codes.count(code) for code in set(codes)} == {200: 200}


def new_job_id(conn) -> str:
    return str(repo.create_job(conn, str(uuid4()), BODY)[1]["id"])

