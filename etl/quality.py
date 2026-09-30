"""DATA QUALITY: SQL checks run after every load.

Each check is a query returning the number of *failing* rows.
- severity 'error' -> pipeline fails (Airflow marks the task red, alert fires)
- severity 'warn'  -> result is logged but the run continues
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from etl.config import CITIES

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    severity: str = "error"
    description: str = ""


CHECKS: list[Check] = [
    Check(
        "row_count_per_city",
        f"""SELECT COUNT(*) FROM (
                SELECT c.city_id, COUNT(f.observed_at_utc) AS n
                FROM analytics.dim_city c
                LEFT JOIN analytics.fact_weather_hourly f
                  ON f.city_id = c.city_id AND f.observed_date_utc = %(run_date)s
                WHERE c.city_code IN ({",".join(f"'{c.code}'" for c in CITIES)})
                GROUP BY c.city_id
                HAVING COUNT(f.observed_at_utc) <> 24) x""",
        description="Every configured city must have exactly 24 hourly rows",
    ),
    Check(
        "no_null_temperature",
        """SELECT COUNT(*) FROM analytics.fact_weather_hourly
           WHERE observed_date_utc = %(run_date)s AND temperature_c IS NULL""",
        description="Temperature is the core metric and cannot be null",
    ),
    Check(
        "no_duplicate_keys",
        """SELECT COUNT(*) FROM (
               SELECT city_id, observed_at_utc FROM analytics.fact_weather_hourly
               WHERE observed_date_utc = %(run_date)s
               GROUP BY 1, 2 HAVING COUNT(*) > 1) d""",
        description="One row per city per hour",
    ),
    Check(
        "humidity_in_range",
        """SELECT COUNT(*) FROM analytics.fact_weather_hourly
           WHERE observed_date_utc = %(run_date)s
             AND relative_humidity NOT BETWEEN 0 AND 100""",
        description="Relative humidity is a percentage",
    ),
    Check(
        "min_not_above_max",
        """SELECT COUNT(*) FROM mart.daily_city_weather
           WHERE day = %(run_date)s AND temp_min_c > temp_max_c""",
        description="Sanity check on the mart aggregation",
    ),
    Check(
        "nulls_in_secondary_metrics",
        """SELECT COUNT(*) FROM analytics.fact_weather_hourly
           WHERE observed_date_utc = %(run_date)s
             AND (relative_humidity IS NULL OR precipitation_mm IS NULL
                  OR wind_speed_kmh IS NULL)""",
        severity="warn",
        description="Secondary metrics may occasionally be missing",
    ),
]


class DataQualityError(RuntimeError):
    pass


def run_checks(conn, run_date: date, checks: list[Check] = CHECKS) -> list[dict]:
    results = []
    with conn.cursor() as cur:
        for check in checks:
            cur.execute(check.sql, {"run_date": run_date})
            failing = cur.fetchone()[0]
            passed = failing == 0
            results.append(
                {"check": check.name, "severity": check.severity,
                 "passed": passed, "failing_rows": failing}
            )
            cur.execute(
                """INSERT INTO monitoring.dq_results
                       (run_date, check_name, severity, passed, failing_rows, details)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (run_date, check.name, check.severity, passed, failing, check.description),
            )
            level = logging.INFO if passed else logging.WARNING
            log.log(level, "DQ %-28s %s (%d failing rows)", check.name,
                    "PASS" if passed else "FAIL", failing)
    conn.commit()

    blocking = [r for r in results if not r["passed"] and r["severity"] == "error"]
    if blocking:
        names = ", ".join(r["check"] for r in blocking)
        raise DataQualityError(f"{len(blocking)} blocking check(s) failed for {run_date}: {names}")
    return results
