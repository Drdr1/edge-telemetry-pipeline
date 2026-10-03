from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

import api.main as api_main
from common.settings import Settings

KEY = "test-key-123"


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows):
        self.cur = FakeCursor(rows)

    def cursor(self):
        return self.cur


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api_main, "settings", Settings(api_keys=frozenset({KEY})))
    conn = FakeConn(
        [
            {
                "bucket": datetime(2026, 10, 1, 12, 0, tzinfo=UTC),
                "avg_value": 1.0,
                "min_value": 0.5,
                "max_value": 1.5,
                "samples": 600,
            }
        ]
    )
    api_main.app.dependency_overrides[api_main.get_conn] = lambda: conn
    c = TestClient(api_main.app)
    c.conn = conn
    yield c
    api_main.app.dependency_overrides.clear()


URL = "/v1/devices/dev-001/metrics/power_kw/series"
H = {"X-API-Key": KEY}


def test_healthz_is_public(client):
    assert client.get("/healthz").status_code == 200


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
def test_rejects_missing_or_bad_key(client, headers):
    assert client.get(URL, headers=headers).status_code == 401


def test_no_keys_configured_fails_closed(monkeypatch, client):
    monkeypatch.setattr(api_main, "settings", Settings(api_keys=frozenset()))
    assert client.get(URL, headers=H).status_code == 401


def test_series_ok_and_parameterised(client):
    r = client.get(URL, headers=H)
    assert r.status_code == 200
    assert r.json()[0]["samples"] == 600
    sql, params = client.conn.cur.executed[0]
    assert "%s" in sql and params[0] == "dev-001" and params[1] == "power_kw"


@pytest.mark.parametrize(
    "url",
    [
        "/v1/devices/dev;drop/metrics/power_kw/series",
        "/v1/devices/dev-001/metrics/Power-KW/series",
        URL + "?limit=0",
        URL + "?limit=100000",
    ],
)
def test_input_validation(client, url):
    assert client.get(url, headers=H).status_code == 422


@pytest.mark.parametrize(
    "qs,msg",
    [
        ("start=2026-10-01T00:00:00Z&end=2026-09-01T00:00:00Z", "before"),
        ("start=2026-01-01T00:00:00Z&end=2026-10-01T00:00:00Z", "exceeds"),
        ("start=2026-10-01T00:00:00&end=2026-10-01T01:00:00", "timezone"),
    ],
)
def test_window_rules(client, qs, msg):
    r = client.get(f"{URL}?{qs}", headers=H)
    assert r.status_code == 422 and msg in r.json()["detail"]
