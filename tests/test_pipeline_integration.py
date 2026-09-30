"""End-to-end tests against a real PostgreSQL (API is faked).

Run locally:  TEST_DATABASE_DSN="host=localhost port=5432 dbname=weather user=etl password=etl" pytest
"""
import psycopg2
import pytest
from conftest import TEST_DATE, make_payload

from etl import pipeline
from etl.config import CITIES
from etl.extract import ExtractError
from etl.quality import DataQualityError


def fake_fetch(city, run_date):
    base = 10 + CITIES.index(city) * 3  # different temperature per city
    return f"https://fake/{city.code}", make_payload(run_date, base_temp=base)


def query(dsn, sql, params=None):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def run(dsn, fetch=fake_fetch):
    pipeline.step_setup(dsn)
    pipeline.step_extract(TEST_DATE, dsn, fetch=fetch)
    pipeline.step_transform_load(TEST_DATE, dsn)
    return pipeline.step_quality(TEST_DATE, dsn)


def test_full_run_loads_every_city_and_passes_checks(dsn):
    results = run(dsn)
    assert all(r["passed"] for r in results)
    (n,) = query(dsn, "SELECT COUNT(*) FROM analytics.fact_weather_hourly")[0]
    assert n == 24 * len(CITIES)
    mart = query(dsn, "SELECT city_name, hours_recorded FROM mart.daily_city_weather")
    assert len(mart) == len(CITIES) and all(h == 24 for _, h in mart)


def test_transform_load_reports_all_rows(dsn):
    pipeline.step_setup(dsn)
    pipeline.step_extract(TEST_DATE, dsn, fetch=fake_fetch)
    assert pipeline.step_transform_load(TEST_DATE, dsn) == 24 * len(CITIES)


def test_rerun_is_idempotent(dsn):
    run(dsn)
    run(dsn)  # same day again (Airflow retry / manual re-run)
    (n,) = query(dsn, "SELECT COUNT(*) FROM analytics.fact_weather_hourly")[0]
    assert n == 24 * len(CITIES)


def test_rerun_picks_up_corrected_source_values(dsn):
    run(dsn)

    def corrected(city, run_date):
        url, p = fake_fetch(city, run_date)
        p["hourly"]["temperature_2m"] = [20.0] * len(p["hourly"]["temperature_2m"])
        return url, p

    run(dsn, fetch=corrected)
    temps = query(dsn, "SELECT DISTINCT temperature_c FROM analytics.fact_weather_hourly")
    assert temps == [(20,)]


def test_missing_hours_fail_quality_gate(dsn):
    def partial(city, run_date):
        url, _ = fake_fetch(city, run_date)
        return url, make_payload(run_date, hours=20)  # 4 hours missing

    with pytest.raises(DataQualityError, match="row_count_per_city"):
        run(dsn, fetch=partial)
    failed = query(dsn, "SELECT check_name FROM monitoring.dq_results WHERE NOT passed")
    assert ("row_count_per_city",) in failed


def test_extract_failure_is_logged_and_raised(dsn):
    def flaky(city, run_date):
        if city.code == "MAD":
            raise ExtractError("HTTP 500")
        return fake_fetch(city, run_date)

    pipeline.step_setup(dsn)
    with pytest.raises(ExtractError, match="MAD"):
        pipeline.step_extract(TEST_DATE, dsn, fetch=flaky)
    status = query(dsn, "SELECT status, rows_affected FROM monitoring.pipeline_runs")
    assert status == [("failed", len(CITIES) - 1)]
