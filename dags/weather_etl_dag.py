"""Airflow DAG: daily weather batch ETL.

setup -> extract -> transform_load -> data_quality

Each run processes the *previous full day*, so the run at 2026-10-02 06:00
loads 2026-10-01. Because every step is idempotent, you can re-run or
backfill any date safely (Airflow 3 CLI):

    airflow backfill create --dag-id weather_etl \
        --from-date 2026-09-01 --to-date 2026-09-07
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, get_current_context, task

# make the etl package importable (it is mounted next to the dags folder)
sys.path.append(str(Path(__file__).resolve().parent.parent))

from etl import pipeline  # noqa: E402

log = logging.getLogger(__name__)


def _run_date() -> date:
    """The day this run is responsible for: the full day before the run time.

    Uses data_interval_end (falls back to logical_date) so the result is the
    same whether Airflow uses a cron *trigger* timetable (Airflow 3 default)
    or the classic cron *data-interval* timetable.
    """
    ctx = get_current_context()
    anchor = ctx.get("data_interval_end") or ctx.get("logical_date")
    if anchor is None:  # manual trigger without a logical date
        return date.today() - timedelta(days=1)
    return anchor.date() - timedelta(days=1)


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
}


@dag(
    dag_id="weather_etl",
    description="Daily hourly-weather ETL: Open-Meteo API -> PostgreSQL",
    schedule="0 6 * * *",  # 06:00 UTC, after the previous day is complete
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["portfolio", "etl", "weather"],
)
def weather_etl():
    @task
    def setup() -> int:
        return pipeline.step_setup()

    @task
    def extract() -> int:
        return pipeline.step_extract(_run_date())

    @task
    def transform_load() -> int:
        return pipeline.step_transform_load(_run_date())

    @task(retries=0)  # a failed quality check needs a human, not a retry
    def data_quality() -> list[dict]:
        return pipeline.step_quality(_run_date())

    setup() >> extract() >> transform_load() >> data_quality()


weather_etl()
