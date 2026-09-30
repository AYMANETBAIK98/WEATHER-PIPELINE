"""LOAD: write raw payloads and clean rows into PostgreSQL (idempotent)."""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

from psycopg2.extras import execute_values

from etl.config import CITIES
from etl.transform import HourlyRow

log = logging.getLogger(__name__)
SCHEMA_FILE = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def init_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text())
    conn.commit()


def upsert_cities(conn) -> int:
    rows = [(c.code, c.name, c.country, c.latitude, c.longitude, c.timezone) for c in CITIES]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analytics.dim_city
                (city_code, city_name, country, latitude, longitude, timezone)
            VALUES %s
            ON CONFLICT (city_code) DO UPDATE SET
                city_name = EXCLUDED.city_name,
                country   = EXCLUDED.country,
                latitude  = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                timezone  = EXCLUDED.timezone,
                updated_at = now()
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def save_raw(conn, city_code: str, run_date: date, url: str, payload: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw.open_meteo_responses (city_code, run_date, request_url, payload)
               VALUES (%s, %s, %s, %s)""",
            (city_code, run_date, url, json.dumps(payload)),
        )
    conn.commit()


def load_latest_raw(conn, run_date: date) -> list[tuple[str, dict]]:
    """Latest raw payload per city for a day (lets transform re-run without re-calling the API)."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT ON (city_code) city_code, payload
               FROM raw.open_meteo_responses
               WHERE run_date = %s
               ORDER BY city_code, extracted_at DESC, response_id DESC""",
            (run_date,),
        )
        return cur.fetchall()


def upsert_hourly(conn, rows: list[HourlyRow]) -> int:
    if not rows:
        return 0
    values = [
        (
            r.city_code, r.observed_at_utc, r.observed_date_utc, r.temperature_c,
            r.apparent_temp_c, r.relative_humidity, r.precipitation_mm,
            r.wind_speed_kmh, r.weather_code,
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analytics.fact_weather_hourly
                (city_id, observed_at_utc, observed_date_utc, temperature_c, apparent_temp_c,
                 relative_humidity, precipitation_mm, wind_speed_kmh, weather_code)
            SELECT c.city_id, v.ts::timestamptz, v.d::date, v.t::numeric, v.at::numeric,
                   v.h::smallint, v.p::numeric, v.w::numeric, v.wc::smallint
            FROM (VALUES %s) AS v(code, ts, d, t, at, h, p, w, wc)
            JOIN analytics.dim_city c ON c.city_code = v.code
            ON CONFLICT (city_id, observed_at_utc) DO UPDATE SET
                temperature_c     = EXCLUDED.temperature_c,
                apparent_temp_c   = EXCLUDED.apparent_temp_c,
                relative_humidity = EXCLUDED.relative_humidity,
                precipitation_mm  = EXCLUDED.precipitation_mm,
                wind_speed_kmh    = EXCLUDED.wind_speed_kmh,
                weather_code      = EXCLUDED.weather_code,
                loaded_at         = now()
            """,
            values,
            page_size=len(values),  # one statement, so rowcount covers every row
        )
        affected = cur.rowcount
    conn.commit()
    return affected


def log_run(conn, run_date: date, step: str, status: str, rows: int | None = None, message: str = "") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO monitoring.pipeline_runs (run_date, step, status, rows_affected, message)
               VALUES (%s, %s, %s, %s, %s)""",
            (run_date, step, status, rows, message[:1000]),
        )
    conn.commit()
