from __future__ import annotations

import math
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DATE = date(2026, 9, 28)


def make_payload(day: date = TEST_DATE, hours: int = 24, base_temp: float = 15.0) -> dict:
    """Build a payload shaped exactly like Open-Meteo's hourly response.
    Includes one hour of the next day, like the real API can when
    timezones shift, so we can test that it gets filtered out."""
    start = datetime.combine(day, datetime.min.time())
    times = [(start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(hours + 1)]
    temps = [round(base_temp + 5 * math.sin((h - 9) / 24 * 2 * math.pi), 1) for h in range(hours + 1)]
    return {
        "latitude": 51.5, "longitude": -0.12, "timezone": "UTC",
        "hourly_units": {"time": "iso8601", "temperature_2m": "°C"},
        "hourly": {
            "time": times,
            "temperature_2m": temps,
            "apparent_temperature": [t - 1.5 for t in temps],
            "relative_humidity_2m": [70 + (h % 10) for h in range(hours + 1)],
            "precipitation": [0.2 if 14 <= h <= 16 else 0.0 for h in range(hours + 1)],
            "wind_speed_10m": [10.0 + h * 0.5 for h in range(hours + 1)],
            "weather_code": [3 for _ in range(hours + 1)],
        },
    }


@pytest.fixture
def payload():
    return make_payload()


def _dsn() -> str | None:
    return os.getenv("TEST_DATABASE_DSN")


@pytest.fixture
def dsn():
    """Integration tests need a real Postgres; skipped when not configured."""
    value = _dsn()
    if not value:
        pytest.skip("TEST_DATABASE_DSN not set")
    import psycopg2

    conn = psycopg2.connect(value)
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS raw, analytics, mart, monitoring CASCADE")
    conn.commit()
    conn.close()
    return value
