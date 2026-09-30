"""Pipeline steps as plain functions.

Airflow tasks and the local CLI both call these, so the business logic
never depends on the orchestrator (easy to test, easy to swap Airflow out).
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date

import psycopg2

from etl import extract, load, quality, transform
from etl.config import CITIES, get_db_dsn

log = logging.getLogger(__name__)


@contextmanager
def connect(dsn: str | None = None):
    conn = psycopg2.connect(dsn or get_db_dsn())
    try:
        yield conn
    finally:
        conn.close()


def step_setup(dsn: str | None = None) -> int:
    with connect(dsn) as conn:
        load.init_schema(conn)
        return load.upsert_cities(conn)


def step_extract(run_date: date, dsn: str | None = None, fetch=None) -> int:
    """Pull every city and land the raw JSON. Fails if any city fails
    (after its own retries), so Airflow can retry the whole step."""
    fetch = fetch or extract.fetch_city_day  # injectable for tests
    failures = []
    with connect(dsn) as conn:
        for city in CITIES:
            try:
                url, payload = fetch(city, run_date)
                load.save_raw(conn, city.code, run_date, url, payload)
            except extract.ExtractError as exc:
                failures.append(f"{city.code}: {exc}")
        n_ok = len(CITIES) - len(failures)
        status = "success" if not failures else "failed"
        load.log_run(conn, run_date, "extract", status, n_ok, "; ".join(failures))
    if failures:
        raise extract.ExtractError(f"{len(failures)} city extract(s) failed: {failures}")
    return n_ok


def step_transform_load(run_date: date, dsn: str | None = None) -> int:
    with connect(dsn) as conn:
        raw_rows = load.load_latest_raw(conn, run_date)
        if not raw_rows:
            raise RuntimeError(f"No raw data found for {run_date}; run extract first")
        clean = []
        for city_code, payload in raw_rows:
            clean.extend(transform.transform_payload(city_code, payload, run_date))
        affected = load.upsert_hourly(conn, clean)
        load.log_run(conn, run_date, "transform_load", "success", affected)
        log.info("Upserted %d hourly rows for %s", affected, run_date)
        return affected


def step_quality(run_date: date, dsn: str | None = None) -> list[dict]:
    with connect(dsn) as conn:
        try:
            results = quality.run_checks(conn, run_date)
            load.log_run(conn, run_date, "quality", "success", len(results))
            return results
        except quality.DataQualityError as exc:
            load.log_run(conn, run_date, "quality", "failed", None, str(exc))
            raise


def run_all(run_date: date, dsn: str | None = None) -> None:
    step_setup(dsn)
    step_extract(run_date, dsn)
    step_transform_load(run_date, dsn)
    step_quality(run_date, dsn)
