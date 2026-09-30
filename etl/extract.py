"""EXTRACT: call the Open-Meteo API for one city and one day."""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta

import requests

from etl.config import (
    ARCHIVE_URL,
    FORECAST_URL,
    HOURLY_VARIABLES,
    HTTP_MAX_RETRIES,
    HTTP_TIMEOUT_SECONDS,
    RECENT_DAYS_LIMIT,
    City,
)

log = logging.getLogger(__name__)


class ExtractError(RuntimeError):
    """Raised when the API cannot be reached or returns bad data."""


def choose_endpoint(run_date: date, today: date | None = None) -> str:
    today = today or date.today()
    if (today - run_date) > timedelta(days=RECENT_DAYS_LIMIT):
        return ARCHIVE_URL
    return FORECAST_URL


def build_params(city: City, run_date: date) -> dict:
    return {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "start_date": run_date.isoformat(),
        "end_date": run_date.isoformat(),
        "timezone": "UTC",  # store everything in UTC, convert in reporting
        "wind_speed_unit": "kmh",
    }


def fetch_city_day(
    city: City,
    run_date: date,
    session: requests.Session | None = None,
    backoff_seconds: float = 2.0,
) -> tuple[str, dict]:
    """Return (request_url, json_payload). Retries with exponential backoff
    on network errors, HTTP 429 and 5xx. 4xx errors fail immediately."""
    session = session or requests.Session()
    url = choose_endpoint(run_date)
    params = build_params(city, run_date)

    last_error = ""
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=HTTP_TIMEOUT_SECONDS)
        except requests.RequestException as exc:  # timeout, DNS, connection reset...
            last_error, retryable = str(exc), True
        else:
            if resp.status_code == 200:
                payload = resp.json()
                if "hourly" not in payload:
                    raise ExtractError(f"Unexpected payload for {city.code}: no 'hourly' key")
                log.info("Extracted %s %s (attempt %d)", city.code, run_date, attempt)
                return resp.url, payload
            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
            # 429 (rate limit) and 5xx are temporary; other 4xx will not fix themselves
            retryable = resp.status_code == 429 or resp.status_code >= 500

        if not retryable or attempt == HTTP_MAX_RETRIES:
            break
        wait = backoff_seconds * 2 ** (attempt - 1)
        log.warning("%s %s failed (%s), retrying in %.0fs", city.code, run_date, last_error, wait)
        time.sleep(wait)

    raise ExtractError(f"Giving up on {city.code} {run_date}: {last_error}")
