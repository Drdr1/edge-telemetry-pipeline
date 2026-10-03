"""Read API over the telemetry store.

Security: API keys via X-API-Key compared in constant time, keys from env/secret
manager only; strict input validation (regex-bounded ids, bounded time windows,
capped limits); parameterised SQL only; no stack traces in responses.
"""

from __future__ import annotations

import hmac
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Security
from fastapi.security import APIKeyHeader
from psycopg.rows import dict_row
from pydantic import BaseModel

from common.settings import settings

app = FastAPI(title="Telemetry API", version="0.1.0")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

ID_PATTERN = r"^[A-Za-z0-9_\-.]{1,64}$"
METRIC_PATTERN = r"^[a-z][a-z0-9_]{0,63}$"
MAX_WINDOW = timedelta(days=7)


def require_api_key(key: str | None = Security(api_key_header)) -> str:
    valid = settings.api_keys
    if not key or not valid or not any(hmac.compare_digest(key, k) for k in valid):
        raise HTTPException(status_code=401, detail="invalid or missing API key")
    return key


def get_conn() -> Iterator[psycopg.Connection]:
    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        yield conn


class Point(BaseModel):
    bucket: datetime
    avg_value: float
    min_value: float
    max_value: float
    samples: int


class Latest(BaseModel):
    device_id: str
    metric: str
    ts: datetime
    value: float


def resolve_window(start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
    end = end or datetime.now(UTC)
    start = start or end - timedelta(hours=1)
    if start.tzinfo is None or end.tzinfo is None:
        raise HTTPException(422, "start/end must include a timezone")
    if start >= end:
        raise HTTPException(422, "start must be before end")
    if end - start > MAX_WINDOW:
        raise HTTPException(422, f"window exceeds {MAX_WINDOW.days} days; use the hourly rollup")
    return start, end


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get(
    "/v1/devices/{device_id}/metrics/{metric}/series",
    response_model=list[Point],
    dependencies=[Depends(require_api_key)],
)
def series(
    device_id: Annotated[str, Path(pattern=ID_PATTERN)],
    metric: Annotated[str, Path(pattern=METRIC_PATTERN)],
    conn: Annotated[psycopg.Connection, Depends(get_conn)],
    start: datetime | None = None,
    end: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=10_000)] = 1000,
) -> list[dict]:
    start, end = resolve_window(start, end)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bucket, avg_value, min_value, max_value, samples
            FROM readings_1m
            WHERE device_id = %s AND metric = %s AND bucket >= %s AND bucket < %s
            ORDER BY bucket LIMIT %s
            """,
            (device_id, metric, start, end, limit),
        )
        return cur.fetchall()


@app.get("/v1/latest", response_model=list[Latest], dependencies=[Depends(require_api_key)])
def latest(
    conn: Annotated[psycopg.Connection, Depends(get_conn)],
    device_id: Annotated[str | None, Query(pattern=ID_PATTERN)] = None,
) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (device_id, metric) device_id, metric, ts, value
            FROM readings
            WHERE ts > now() - interval '5 minutes' AND (%(d)s::text IS NULL OR device_id = %(d)s)
            ORDER BY device_id, metric, ts DESC
            """,
            {"d": device_id},
        )
        return cur.fetchall()
