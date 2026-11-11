"""HTTP API (SPEC §4). Thin layer over app/repo.py: validate everything before it reaches SQL, map
repo outcomes to status codes, and turn database timeouts into a retryable 503."""

import base64
import binascii
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, ConfigDict, Field

from app import db, repo

Status = Literal["queued", "running", "succeeded", "failed", "cancelled"]


class JobIn(BaseModel):
    """The only way a body reaches create_job: unknown fields dropped, values normalized (1.0 → 1),
    defaults filled, so equivalent requests hash the same."""

    model_config = ConfigDict(extra="ignore")

    duration_ms: int = Field(ge=0, le=60_000)
    fail: bool = False


class JobOut(BaseModel):
    id: UUID
    status: Status
    payload: dict[str, Any]
    attempt: int
    worker_id: str | None
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobPage(BaseModel):
    items: list[JobOut]
    next_cursor: str | None


def bad_request(detail: str) -> HTTPException:
    return HTTPException(status_code=400, detail=detail)


def idempotency_key(key: Annotated[str, Header(alias="Idempotency-Key")]) -> str:
    """Non-empty, ≤ 255 chars, printable: a ~10 KB key overflows the btree row and NUL is a
    DataError, both would otherwise be a 500."""
    if not 1 <= len(key) <= 255 or not key.isprintable():
        raise bad_request("Idempotency-Key must be 1-255 printable characters")
    return key


# --- keyset cursor: opaque base64 of "<created_at ISO>|<id>" -----------------------------------------

def encode_cursor(job: dict) -> str:
    raw = f"{job['created_at'].isoformat()}|{job['id']}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        raw = base64.b64decode(cursor.encode("ascii"), altchars=b"-_", validate=True).decode()
        created_at, job_id = raw.split("|")
        after = (datetime.fromisoformat(created_at), UUID(job_id))
    except (ValueError, UnicodeError, binascii.Error):
        raise bad_request("malformed cursor") from None
    if after[0].tzinfo is None:
        raise bad_request("malformed cursor")
    return after


# --- app -----------------------------------------------------------------------------------------

def create_app(database_url: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        with db.pool("api", "api", url=database_url) as pool:
            app.state.pool = pool
            yield

    app = FastAPI(title="Training-job queue", lifespan=lifespan)

    def conn(request: Request):
        """Take a pooled connection inside the endpoint, i.e. after FastAPI has validated the
        input and on the same threadpool thread that uses it. As a (sync) dependency it would be
        taken in a separate threadpool job: under a burst, threads blocked waiting for a connection
        starve the requests holding one, and everything stalls until PoolTimeout. It would also
        turn bad input into a 503 whenever the database is slow or down."""
        return request.app.state.pool.connection()

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # SPEC §4 promises 400 for bad input (FastAPI's default is 422).
        return JSONResponse(status_code=400, content={"detail": jsonable(exc.errors())})

    @app.exception_handler(psycopg.OperationalError)
    @app.exception_handler(PoolTimeout)
    async def database_busy(request: Request, exc: Exception) -> JSONResponse:
        # lock_timeout / statement_timeout / lost connection / no free pooled connection. Every
        # endpoint is safe to retry: create is idempotent, cancel is idempotent, reads are reads.
        return JSONResponse(
            status_code=503,
            content={"detail": "database busy, retry"},
            headers={"Retry-After": "1"},
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/jobs", response_model=JobOut, status_code=201)
    def create_job(
        request: Request, body: JobIn, key: Annotated[str, Depends(idempotency_key)]
    ) -> JSONResponse | dict:
        with conn(request) as c:
            outcome, job = repo.create_job(c, key, body.model_dump())
        if outcome == "conflict":
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "Idempotency-Key already used with a different body",
                    "job_id": str(job["id"]),
                },
            )
        if outcome == "replay":
            return JSONResponse(status_code=200, content=JobOut(**job).model_dump(mode="json"))
        return job

    @app.get("/jobs/{job_id}", response_model=JobOut)
    def get_job(request: Request, job_id: UUID) -> dict:
        with conn(request) as c:
            job = repo.get_job(c, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @app.get("/jobs", response_model=JobPage)
    def list_jobs(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        cursor: str | None = None,
        status: Status | None = None,
    ) -> dict:
        after = decode_cursor(cursor) if cursor is not None else None
        with conn(request) as c:
            rows = repo.list_jobs(c, limit + 1, after=after, status=status)  # +1: next page?
        items = rows[:limit]
        return {
            "items": items,
            "next_cursor": encode_cursor(items[-1]) if len(rows) > limit else None,
        }

    @app.post("/jobs/{job_id}/cancel", response_model=JobOut)
    def cancel_job(request: Request, job_id: UUID) -> JSONResponse | dict:
        with conn(request) as c:
            outcome, job = repo.cancel_job(c, job_id)
        if outcome == "not_found":
            raise HTTPException(status_code=404, detail="job not found")
        if outcome == "terminal":
            return JSONResponse(
                status_code=409,
                content={"detail": f"job already {job['status']}", "status": job["status"]},
            )
        return job

    return app


def jsonable(errors: list[dict]) -> list[dict]:
    """Validation errors can carry raw input (bytes, exceptions); keep only JSON-safe fields."""
    return [{"loc": list(e.get("loc", ())), "msg": str(e.get("msg", ""))} for e in errors]


app = create_app()
