import os
import sqlite3
from contextlib import asynccontextmanager
from time import perf_counter
from typing import Annotated, Literal, Optional

from fastapi import FastAPI, Header, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .store import PayloadConflict, Store, is_contention

store = Store()
SQL_TIMING = os.getenv("QUEUE_SQL_TIMING") == "1"
MAX_ID = 2**63 - 1
JobID = Annotated[int, Path(ge=1, le=MAX_ID)]


class JobInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_ms: int = Field(default=50, ge=1, le=60000, strict=True)
    outcome: Literal["succeeded", "failed"] = "succeeded"


class Job(BaseModel):
    id: int
    state: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    attempt: int
    run_ms: int
    outcome: Literal["succeeded", "failed"]


class JobPage(BaseModel):
    items: list[Job]
    next_after: Optional[int] = None


@asynccontextmanager
async def lifespan(app):
    store.initialize()
    yield


app = FastAPI(title="Training queue", lifespan=lifespan)


def server_timing(timings):
    if not timings:
        return {}
    value = ", ".join(f"{name};dur={seconds * 1000:.3f}" for name, seconds in timings.items())
    return {"Server-Timing": value}


@app.exception_handler(sqlite3.OperationalError)
async def database_error(request: Request, exc: sqlite3.OperationalError):
    if not is_contention(exc):
        raise exc
    return JSONResponse(
        status_code=503,
        content={"detail": "database busy"},
        headers={"Retry-After": "1", **server_timing(getattr(request.state, "sql_timings", None))},
    )


def as_job(row):
    if row is None:
        raise HTTPException(404, "job not found")
    return Job.model_validate(dict(row))


@app.get("/healthz")
def health():
    store.page(0, 1)
    return {"status": "ok"}


@app.post("/jobs", status_code=201, responses={200: {"model": Job}, 409: {}})
def create_job(
    payload: JobInput,
    response: Response,
    request: Request,
    key: Annotated[str, Header(alias="Idempotency-Key", pattern=r"^[!-~]{1,128}$")],
) -> Job:
    timings = {} if SQL_TIMING else None
    if timings is not None:
        started = perf_counter()
        request.state.sql_timings = timings
    try:
        row, created = store.create(key, payload.run_ms, payload.outcome, timings=timings)
        job = as_job(row)
    except PayloadConflict:
        raise HTTPException(
            409, "key already used for a different payload", headers=server_timing(timings),
        )
    finally:
        if timings is not None:
            timings["handler"] = perf_counter() - started

    response.status_code = 201 if created else 200
    response.headers["Location"] = f"/jobs/{row['id']}"
    response.headers.update(server_timing(timings))
    return job


@app.get("/jobs")
def list_jobs(
    after: Annotated[int, Query(ge=0, le=MAX_ID)] = 0,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> JobPage:
    rows = store.page(after, limit + 1)
    items = [as_job(row) for row in rows[:limit]]
    return JobPage(items=items, next_after=items[-1].id if len(rows) > limit else None)


@app.get("/jobs/{job_id}")
def get_job(job_id: JobID) -> Job:
    return as_job(store.get(job_id))


@app.post("/jobs/{job_id}/cancel", responses={409: {"model": Job}})
def cancel_job(job_id: JobID, response: Response) -> Job:
    job = as_job(store.cancel(job_id))
    if job.state != "cancelled":
        response.status_code = 409
    return job
