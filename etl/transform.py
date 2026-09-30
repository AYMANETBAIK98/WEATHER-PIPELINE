"""TRANSFORM: turn the column-oriented API payload into clean hourly rows."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

log = logging.getLogger(__name__)

# API field -> warehouse column
FIELD_MAP = {
    "temperature_2m": "temperature_c",
    "apparent_temperature": "apparent_temp_c",
    "relative_humidity_2m": "relative_humidity",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_speed_kmh",
    "weather_code": "weather_code",
}

# Physically plausible ranges; values outside are nulled (and caught by DQ checks)
VALID_RANGES = {
    "temperature_c": (-70, 60),
    "apparent_temp_c": (-90, 75),
    "relative_humidity": (0, 100),
    "precipitation_mm": (0, 500),
    "wind_speed_kmh": (0, 400),
    "weather_code": (0, 99),
}


@dataclass
class HourlyRow:
    city_code: str
    observed_at_utc: datetime
    observed_date_utc: date
    temperature_c: float | None
    apparent_temp_c: float | None
    relative_humidity: int | None
    precipitation_mm: float | None
    wind_speed_kmh: float | None
    weather_code: int | None


def _clean(column: str, value):
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    low, high = VALID_RANGES[column]
    if not (low <= value <= high):
        log.warning("Out-of-range %s=%s nulled", column, value)
        return None
    if column in ("relative_humidity", "weather_code"):
        return int(round(value))
    return round(value, 2)


def transform_payload(city_code: str, payload: dict, run_date: date) -> list[HourlyRow]:
    hourly = payload["hourly"]
    times = hourly.get("time", [])
    rows: dict[datetime, HourlyRow] = {}

    for i, ts in enumerate(times):
        observed = datetime.fromisoformat(ts).replace(tzinfo=UTC)
        if observed.date() != run_date:
            continue  # keep only the day we asked for
        values = {
            col: _clean(col, (hourly.get(api_field) or [None] * len(times))[i])
            for api_field, col in FIELD_MAP.items()
        }
        # dict keyed on timestamp => duplicates in the source are de-duplicated
        rows[observed] = HourlyRow(
            city_code=city_code,
            observed_at_utc=observed,
            observed_date_utc=observed.date(),
            **values,
        )

    return sorted(rows.values(), key=lambda r: r.observed_at_utc)
